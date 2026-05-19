package sd2526.trab.impl.rest.servers;

import java.net.URI;
import java.util.List;
import java.util.logging.Logger;

import jakarta.inject.Singleton;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response.Status;
import jakarta.ws.rs.core.Response;

import sd2526.trab.api.Message;
import sd2526.trab.api.java.Messages;
import sd2526.trab.api.rest.RestMessages;
import sd2526.trab.impl.api.java.AdminMessages;
import sd2526.trab.impl.api.rest.RestAdminMessages;
import sd2526.trab.impl.java.clients.Clients;
import sd2526.trab.impl.java.servers.JavaMessages;
import sd2526.trab.impl.zookeeper.ReplicationManager;

@Singleton
public class RestMessagesResource extends RestResource implements RestMessages, RestAdminMessages {

	private static final Logger Log = Logger.getLogger(RestMessagesResource.class.getName());
	static boolean isGateway = false;

	Messages impl;
	private ReplicationManager repManager;
	private String myURI;

	// Cliente HTTP para o Primário enviar as replicações para os Secundários
	private final Client httpClient = ClientBuilder.newClient();

	synchronized Messages impl() {
		if (impl == null)
			impl = isGateway ? Clients.MessagesClient.get() : JavaMessages.getInstance();
		return impl;
	}

	public RestMessagesResource() {
	}

	public RestMessagesResource(ReplicationManager repManager, String myURI) {
		this.repManager = repManager;
		this.myURI = myURI;
		this.impl = JavaMessages.getInstance();
	}

	RestMessagesResource(boolean gw) {
		isGateway = gw;
	}

	public RestMessagesResource(Messages impl) {
		this.impl = impl;
	}

	@Override
	public String postMessage(String pwd, Message msg) {
		if (repManager != null && !repManager.isPrimary()) {
			return forwardRequestToPrimary(Entity.entity(msg, MediaType.APPLICATION_JSON), "/messages/?pwd=" + pwd,
					"POST",
					String.class);
		}

		String mid = super.resultOrThrow(impl().postMessage(pwd, msg));
		msg.setId(mid);

		if (repManager != null) {
			long newVersion = repManager.incrementAndGetVersion();
			propagateToSecondaries(Entity.entity(msg, MediaType.APPLICATION_JSON),
					"/replica/post?pwd=" + pwd,
					newVersion, "POST");
		}

		return mid;
	}

	@Override
	public void removeFromUserInbox(String name, String mid, String pwd) {
		if (repManager != null && !repManager.isPrimary()) {
			forwardRequestToPrimary(null, "/messages/mbox/" + name + "/" + mid + "?pwd=" + pwd, "DELETE", Void.class);
			return;
		}

		super.resultOrThrow(impl().removeInboxMessage(name, mid, pwd));

		if (repManager != null) {
			long newVersion = repManager.incrementAndGetVersion();
			propagateToSecondaries(null,
					"/replica/inbox/" + name + "/" + mid + "?pwd=" + pwd,
					newVersion, "DELETE");
		}
	}

	@Override
	public void deleteMessage(String name, String mid, String pwd) {
		if (repManager != null && !repManager.isPrimary()) {
			forwardRequestToPrimary(null, "/messages/" + name + "/" + mid + "?pwd=" + pwd, "DELETE", Void.class);
			return;
		}

		super.resultOrThrow(impl().deleteMessage(name, mid, pwd));

		if (repManager != null) {
			long newVersion = repManager.incrementAndGetVersion();
			propagateToSecondaries(null,
					"/replica/msg/" + name + "/" + mid + "?pwd=" + pwd,
					newVersion, "DELETE");
		}
	}

	// Novo método para fazer o encaminhamento para o Primário
	private <T> T forwardRequestToPrimary(Entity<?> entity, String uriPath, String method, Class<T> responseType) {
		String primaryURI = repManager.getPrimaryURI();
		if (primaryURI == null) {
			throw new WebApplicationException("Primário não disponível", Status.SERVICE_UNAVAILABLE);
		}

		try {
			var request = httpClient.target(primaryURI)
					.path(uriPath)
					.request();

			Response response;
			if ("POST".equalsIgnoreCase(method)) {
				response = request.post(entity);
			} else if ("DELETE".equalsIgnoreCase(method)) {
				response = request.delete();
			} else {
				throw new WebApplicationException("Método não suportado para forwarding", Status.BAD_REQUEST);
			}

			if (response.getStatusInfo().getFamily() != Response.Status.Family.SUCCESSFUL) {
				throw new WebApplicationException(response.getStatus());
			}

			if (responseType == Void.class)
				return null;

			return response.readEntity(responseType);

		} catch (WebApplicationException wae) {
			throw wae;
		} catch (Exception e) {
			throw new WebApplicationException("Falha ao contactar o primário", Status.INTERNAL_SERVER_ERROR);
		}
	}

	@Override
	public Message getMessage(String name, String mid, String pwd) {
		return super.resultOrThrow(impl().getInboxMessage(name, mid, pwd));
	}

	@Override
	public List<String> getMessages(String name, String pwd, String query) {
		if (query != null && !query.isEmpty())
			return super.resultOrThrow(impl().searchInbox(name, pwd, query));
		else
			return super.resultOrThrow(impl().getAllInboxMessages(name, pwd));
	}

	@POST
	@Path("/replica/post")
	@Consumes(MediaType.APPLICATION_JSON)
	public void replicaPostMessage(@QueryParam("pwd") String pwd, Message msg,
			@HeaderParam("X-REPLICA-VERSION") long version) {
		// Secundário força a gravação da mensagem enviada pelo primário
		super.resultOrThrow(impl().postMessage(pwd, msg));

		// Secundário atualiza a sua versão para ficar igual à do primário
		repManager.updateVersion(version);
		Log.info("Replicação com sucesso: POST mensagem " + msg.getId() + " | Nova Versão: " + version);
	}

	@DELETE
	@Path("/replica/inbox/{user}/{mid}")
	public void replicaRemoveFromInbox(@PathParam("user") String name, @PathParam("mid") String mid,
			@QueryParam("pwd") String pwd, @HeaderParam("X-REPLICA-VERSION") long version) {
		super.resultOrThrow(impl().removeInboxMessage(name, mid, pwd));
		repManager.updateVersion(version);
		Log.info("Replicação com sucesso: REMOVE INBOX " + mid + " | Nova Versão: " + version);
	}

	@DELETE
	@Path("/replica/msg/{user}/{mid}")
	public void replicaDeleteMessage(@PathParam("user") String name, @PathParam("mid") String mid,
			@QueryParam("pwd") String pwd, @HeaderParam("X-REPLICA-VERSION") long version) {
		super.resultOrThrow(impl().deleteMessage(name, mid, pwd));
		repManager.updateVersion(version);
		Log.info("Replicação com sucesso: DELETE MSG " + mid + " | Nova Versão: " + version);
	}

	private void checkIfPrimary() {
		if (repManager != null && !repManager.isPrimary()) {
			throw new WebApplicationException("Operação rejeitada: Este nó é um Secundário.", Status.FORBIDDEN);
		}
	}

	/**
	 * Envia o pedido de replicação para todos os outros servidores do mesmo
	 * domínio.
	 */
	private void propagateToSecondaries(Entity<?> entity, String uriPath, long newVersion, String method) {
		List<String> replicaURIs = getReplicaURIsDoMeuDominio();

		for (String uri : replicaURIs) {
			try {
				var request = httpClient.target(uri)
						.path(uriPath)
						.request()
						.header("X-REPLICA-VERSION", newVersion);

				if ("POST".equalsIgnoreCase(method)) {
					request.post(entity);
				} else if ("DELETE".equalsIgnoreCase(method)) {
					request.delete();
				}
			} catch (Exception e) {
				Log.warning("Falha ao replicar para o nó secundário [" + uri + "]: " + e.getMessage());
			}
		}
	}

	private List<String> getReplicaURIsDoMeuDominio() {

		URI[] discoveredUris = sd2526.trab.impl.discovery.Discovery.getInstance().knownUrisOf(Messages.SERVICE_NAME, 1);

		List<String> replicaUris = new java.util.ArrayList<>();

		if (discoveredUris != null) {
			for (URI uri : discoveredUris) {
				String uriString = uri.toString();
				if (!uriString.equals(myURI)) {
					replicaUris.add(uriString);
				}
			}
		}

		return replicaUris;
	}

	@Override
	public void remotePostMessage(Message m) {
		super.resultOrThrow(((AdminMessages) impl()).remotePostMessage(m));
	}

	@Override
	public void remoteDeleteMessage(String mid) {
		super.resultOrThrow(((AdminMessages) impl()).remoteDeleteMessage(mid));
	}

	@Override
	public void remoteDeleteUserInbox(String name) {
		super.resultOrThrow(((AdminMessages) impl()).remoteDeleteUserInbox(name));
	}
}
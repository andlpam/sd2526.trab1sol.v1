package sd2526.trab.impl.zookeeper;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.MediaType;
import sd2526.trab.api.rest.RestMessages;
import sd2526.trab.api.rest.RestReplicatedMessages;
import sd2526.trab.impl.java.utils.ReplicationLogEntry;

public class ReplicationDispatcher extends Thread {

  private static final Logger Log = Logger.getLogger(ReplicationDispatcher.class.getName());

  private final String targetUri;
  private final String secret;
  private final BlockingQueue<ReplicationLogEntry> queue;
  private final Client client;

  public ReplicationDispatcher(String targetUri, String secret) {
    this.targetUri = targetUri;
    this.secret = secret;
    this.queue = new LinkedBlockingQueue<>();
    this.client = ClientBuilder.newClient(); // Usa o InsecureHostnameVerifier se tiveres HTTPS self-signed
    this.start(); // Arranca a thread automaticamente
  }

  // O Primário chama isto para colocar a operação na fila (não bloqueia)
  public void dispatch(ReplicationLogEntry entry) {
    queue.add(entry);
  }

  @Override
  public void run() {
    while (true) {
      try {
        // Fica bloqueado à espera que haja operações na fila
        ReplicationLogEntry wrapper = queue.take();

        long v = wrapper.version();
        var target = client.target(targetUri).path(RestReplicatedMessages.PATH)
            .queryParam(RestReplicatedMessages.SECRET, secret);

        // O teu switch exatamente igual
        switch (wrapper.operationType()) {
          case POST_MESSAGE:
          case REMOTE_POST_MESSAGE:
            target.path("/message").request().header(RestMessages.HEADER_VERSION, v)
                .post(Entity.entity(wrapper.message(), MediaType.APPLICATION_JSON));
            break;
          case REMOVE_INBOX:
            target.path(RestMessages.MBOX).path(wrapper.sourceDomain()).path(wrapper.generatedId())
                .request().header(RestMessages.HEADER_VERSION, v).delete();
            break;
          case DELETE_MESSAGE:
          case REMOTE_DELETE_MESSAGE:
            String user = wrapper.sourceDomain() != null ? wrapper.sourceDomain() : "sys";
            target.path(user).path(wrapper.generatedId())
                .request().header(RestMessages.HEADER_VERSION, v).delete();
            break;
          case REMOTE_DELETE_USER_INBOX:
            target.path("mbox").path(wrapper.sourceDomain())
                .request().header(RestMessages.HEADER_VERSION, v).delete();
            break;
          default:
            Log.warning("Operação ignorada pelo Dispatcher: " + wrapper.operationType());
        }

        Log.info("Replicado com sucesso para: " + targetUri + " (Versão: " + v + ")");

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      } catch (Exception e) {
        // Se o secundário estiver em baixo, ignora.
        // Quando o secundário voltar, ele invoca o getState() e vai buscar o que
        // perdeu.
        Log.warning("Falha a replicar para " + targetUri + ": Secundário offline?");
      }
    }
  }
}

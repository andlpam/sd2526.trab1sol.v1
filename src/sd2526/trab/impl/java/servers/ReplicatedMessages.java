package sd2526.trab.impl.java.servers;

import sd2526.trab.impl.api.java.AdminMessages;
import sd2526.trab.impl.zookeeper.ReplicationManager;
import sd2526.trab.api.java.Messages;
import sd2526.trab.api.java.Result;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.net.URI;

import java.util.logging.Logger;

import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import sd2526.trab.impl.java.clients.Clients;
import sd2526.trab.impl.java.utils.*;
import sd2526.trab.impl.rest.filters.SidHeaderHandler;
import sd2526.trab.api.Message;

public class ReplicatedMessages extends JavaBaseService implements Messages, AdminMessages {

  private static final Logger Log = Logger.getLogger(ReplicatedMessages.class.getName());

  private final ReplicationManager replicationManager;
  private final Messages baseServer;

  private Map<Long, ReplicationLogEntry> versionLog = new ConcurrentHashMap<>();
  private Map<String, Long> domainsSid = new ConcurrentHashMap<>();

  public ReplicatedMessages(Messages baseServer, ReplicationManager repManager) {
    super();
    this.replicationManager = repManager;
    this.baseServer = baseServer;
  }

  private boolean isDuplicateRemoteOperation(String sourceDomain, Long incomingSid) {
    return incomingSid <= domainsSid.getOrDefault(sourceDomain, 0L);
  }

  private boolean isValidSecret(String secret) {
    return secret != null && secret.equals(replicationManager.getSecret());
  }

  private Long getSidFromRequestHeaders() {
    Long sid = SidHeaderHandler.incomingSid.get();
    return sid != null ? sid : 0L;
  }

  private synchronized void updateLocalVersion(long version) {
    replicationManager.setLocalVersion(version);
    this.notifyAll();
  }

  // NOVO MÉTODO AUXILIAR: Redireciona com o Caminho (Path) Completo!
  private void checkPrimaryAndRedirect() {
    if (!replicationManager.isPrimary()) {
      String primaryBase = replicationManager.getPrimaryURI();
      if (primaryBase == null) {
        // Zookeeper ainda a eleger
        throw new WebApplicationException(Response.status(Response.Status.SERVICE_UNAVAILABLE).build());
      }

      // Vamos buscar a URI completa que o cliente tentou aceder originalmente
      URI currentUri = SidHeaderHandler.requestUri.get();
      URI targetUri;

      if (currentUri != null) {
        // O primaryBase é ex: "https://messages0.ourorg0:7987/rest"
        URI base = URI.create(primaryBase);

        // Substituímos APENAS o host e o porto. O caminho (/messages/...) mantém-se
        // intacto!
        targetUri = jakarta.ws.rs.core.UriBuilder.fromUri(currentUri)
            .scheme(base.getScheme())
            .host(base.getHost())
            .port(base.getPort())
            .build();
      } else {
        targetUri = URI.create(primaryBase);
      }

      // O cliente agora recebe 307 com a Location exata do método original
      throw new WebApplicationException(Response.temporaryRedirect(targetUri).build());
    }
  }

  @Override
  public Result<String> postMessage(String pwd, Message msg) {
    checkPrimaryAndRedirect();

    Result<String> res = baseServer.postMessage(pwd, msg);

    if (res.isOK()) {
      Set<String> localDestinations = msg.getDestination().stream()
          .filter(this::isLocalAddress)
          .collect(Collectors.toSet());

      Result<Set<String>> unknownRes = Clients.AdminUsersClient.get().checkUsers(localDestinations);
      Set<String> validRecipients = new HashSet<>(localDestinations);
      if (unknownRes.isOK())
        validRecipients.removeAll(unknownRes.value());

      long v = replicationManager.incrementAndGetVersion();
      ReplicationLogEntry wrapper = new ReplicationLogEntry(
          v, OperationType.POST_MESSAGE, msg, res.value(), validRecipients, null, null);

      versionLog.put(v, wrapper);
      replicationManager.logAndPropagateToSecundaries(wrapper);
    }
    return res;
  }

  @Override
  public Result<Void> remotePostMessage(Message msg) {
    Long incomingSid = getSidFromRequestHeaders();

    checkPrimaryAndRedirect();

    String sourceDomain = getDomain(msg.getSender());
    if (isDuplicateRemoteOperation(sourceDomain, incomingSid)) {
      Log.info("Mensagem fantasma descartada!");
      return Result.ok();
    }

    Result<Void> res = ((AdminMessages) baseServer).remotePostMessage(msg);

    if (res.isOK()) {
      long v = replicationManager.incrementAndGetVersion();
      ReplicationLogEntry wrapper = new ReplicationLogEntry(
          v, OperationType.REMOTE_POST_MESSAGE, msg, null, null, sourceDomain, incomingSid);

      versionLog.put(v, wrapper);
      replicationManager.logAndPropagateToSecundaries(wrapper);
    }
    return res;
  }

  @Override
  public Result<Void> removeInboxMessage(String name, String mid, String pwd) {
    checkPrimaryAndRedirect();

    Result<Void> res = baseServer.removeInboxMessage(name, mid, pwd);

    if (res.isOK()) {
      long v = replicationManager.incrementAndGetVersion();
      ReplicationLogEntry wrapper = new ReplicationLogEntry(
          v, OperationType.REMOVE_INBOX, null, mid, null, name, null);

      versionLog.put(v, wrapper);
      replicationManager.logAndPropagateToSecundaries(wrapper);
    }
    return res;
  }

  @Override
  public Result<Void> deleteMessage(String name, String mid, String pwd) {
    checkPrimaryAndRedirect();

    Result<Void> res = baseServer.deleteMessage(name, mid, pwd);

    if (res.isOK()) {
      long v = replicationManager.incrementAndGetVersion();
      ReplicationLogEntry wrapper = new ReplicationLogEntry(
          v, OperationType.DELETE_MESSAGE, null, mid, null, name, null);

      versionLog.put(v, wrapper);
      replicationManager.logAndPropagateToSecundaries(wrapper);
    }
    return res;
  }

  @Override
  public Result<Void> remoteDeleteMessage(String mid) {
    checkPrimaryAndRedirect();

    Result<Void> res = ((AdminMessages) baseServer).remoteDeleteMessage(mid);

    if (res.isOK()) {
      long v = replicationManager.incrementAndGetVersion();
      ReplicationLogEntry wrapper = new ReplicationLogEntry(
          v, OperationType.REMOTE_DELETE_MESSAGE, null, mid, null, null, null);

      versionLog.put(v, wrapper);
      replicationManager.logAndPropagateToSecundaries(wrapper);
    }
    return res;
  }

  @Override
  public Result<Void> remoteDeleteUserInbox(String name) {
    checkPrimaryAndRedirect();

    Result<Void> res = ((AdminMessages) baseServer).remoteDeleteUserInbox(name);

    if (res.isOK()) {
      long v = replicationManager.incrementAndGetVersion();
      ReplicationLogEntry wrapper = new ReplicationLogEntry(
          v, OperationType.REMOTE_DELETE_USER_INBOX, null, null, null, name, null);

      versionLog.put(v, wrapper);
      replicationManager.logAndPropagateToSecundaries(wrapper);
    }
    return res;
  }

  @Override
  public Result<Void> adminRemoveInboxMessage(String name, String mid) {
    checkPrimaryAndRedirect();

    Result<Void> res = ((AdminMessages) baseServer).adminRemoveInboxMessage(name, mid);

    if (res.isOK()) {
      long v = replicationManager.incrementAndGetVersion();
      ReplicationLogEntry wrapper = new ReplicationLogEntry(
          v, OperationType.REMOVE_INBOX, null, mid, null, name, null);

      versionLog.put(v, wrapper);
      replicationManager.logAndPropagateToSecundaries(wrapper);
    }
    return res;
  }

  @Override
  public Result<Message> getInboxMessage(String name, String mid, String pwd) {
    return baseServer.getInboxMessage(name, mid, pwd);
  }

  @Override
  public Result<List<String>> getAllInboxMessages(String name, String pwd) {
    return baseServer.getAllInboxMessages(name, pwd);
  }

  @Override
  public Result<List<String>> searchInbox(String name, String pwd, String query) {
    return baseServer.searchInbox(name, pwd, query);
  }

  public Result<Void> replicatePostMessage(long version, String secret, Message msg) {
    if (!isValidSecret(secret))
      return Result.error(Result.ErrorCode.FORBIDDEN);

    Result<Void> res = ((AdminMessages) baseServer).remotePostMessage(msg);

    if (res.isOK()) {
      updateLocalVersion(version);
    }
    return res;
  }

  public Result<Void> replicateRemoveFromUserInbox(long version, String secret, String name, String mid) {
    if (!isValidSecret(secret))
      return Result.error(Result.ErrorCode.FORBIDDEN);

    Result<Void> res = ((AdminMessages) baseServer).adminRemoveInboxMessage(name, mid);

    if (res.isOK()) {
      updateLocalVersion(version);
    }
    return res;
  }

  public Result<Void> replicateDeleteMessage(long version, String secret, String name, String mid) {
    if (!isValidSecret(secret))
      return Result.error(Result.ErrorCode.FORBIDDEN);

    Result<Void> res = ((AdminMessages) baseServer).remoteDeleteMessage(mid);

    if (res.isOK()) {
      updateLocalVersion(version);
    }
    return res;
  }

  public Result<List<ReplicationLogEntry>> getState(long fromVersion, String secret) {
    if (!isValidSecret(secret))
      return Result.error(Result.ErrorCode.FORBIDDEN);

    List<ReplicationLogEntry> missedEntries = versionLog.entrySet().stream()
        .filter(entry -> entry.getKey() > fromVersion)
        .sorted(Map.Entry.comparingByKey())
        .map(Map.Entry::getValue)
        .collect(Collectors.toList());

    return Result.ok(missedEntries);
  }

  public void recoverStateFromPrimary() {
    String primaryUri = replicationManager.getPrimaryURI();
    if (primaryUri == null)
      return;

    try {
      Log.info("A iniciar recuperação de estado a partir do Primário: " + primaryUri);

      jakarta.ws.rs.client.Client client = jakarta.ws.rs.client.ClientBuilder.newClient();
      List<ReplicationLogEntry> missedLog = client.target(primaryUri)
          .path(sd2526.trab.api.rest.RestReplicatedMessages.PATH)
          .path("state")
          .queryParam("fromVersion", replicationManager.getLocalVersion())
          .queryParam("secret", replicationManager.getSecret())
          .request(jakarta.ws.rs.core.MediaType.APPLICATION_JSON)
          .get(new jakarta.ws.rs.core.GenericType<List<ReplicationLogEntry>>() {
          });

      for (ReplicationLogEntry entry : missedLog) {
        replicationManager.saveToLocalDisk(entry);

        switch (entry.operationType()) {
          case POST_MESSAGE:
          case REMOTE_POST_MESSAGE:
            ((AdminMessages) baseServer).remotePostMessage(entry.message());
            break;
          case REMOVE_INBOX:
            ((AdminMessages) baseServer).adminRemoveInboxMessage(entry.sourceDomain(), entry.generatedId());
            break;
          case DELETE_MESSAGE:
          case REMOTE_DELETE_MESSAGE:
            ((AdminMessages) baseServer).remoteDeleteMessage(entry.generatedId());
            break;
          case REMOTE_DELETE_USER_INBOX:
            ((AdminMessages) baseServer).remoteDeleteUserInbox(entry.sourceDomain());
            break;
          default:
            Log.warning("Operação ignorada no recovery: " + entry.operationType());
            break;
        }

        updateLocalVersion(entry.version());
        versionLog.put(entry.version(), entry);
      }
      Log.info("Recuperação concluída. Versão atual: " + replicationManager.getLocalVersion());
    } catch (Exception e) {
      Log.warning("Falha ao recuperar estado do primário: " + e.getMessage());
    }
  }
}
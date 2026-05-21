package sd2526.trab.impl.java.servers;

import sd2526.trab.impl.api.java.AdminMessages;
import sd2526.trab.impl.rest.servers.RestReplicatedMessagesServer;
import sd2526.trab.impl.zookeeper.ReplicationManager;
import sd2526.trab.api.java.Messages;
import sd2526.trab.api.java.Result;
import java.util.concurrent.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.hsqldb.persist.Log;

import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import sd2526.trab.impl.java.clients.Clients;
import sd2526.trab.impl.java.utils.*;
import sd2526.trab.api.Message;
import java.util.Set;
import java.net.URI;
import java.util.HashSet;
import java.util.Map;

//Wrapper class
public class ReplicatedMessages extends JavaBaseService implements Messages, AdminMessages {

  private final ReplicationManager replicationManager;
  private final Messages baseServer;

  private Map<Long, ReplicationLogEntry> versionLog = new ConcurrentHashMap<>();
  private Map<String, Long> domainsSid = new ConcurrentHashMap<>();

  public ReplicatedMessages(Messages baseServer, ReplicationManager repManager) {
    // When we does super he earns all the methods
    super();
    this.replicationManager = repManager;
    this.baseServer = baseServer;
  }

  private boolean isDuplicateRemoteOperation(String sourceDomain, Long incomingSid) {
    return incomingSid <= domainsSid.getOrDefault(sourceDomain, 0L);
  }

  @Override
  public Result<String> postMessage(String pwd, Message msg) {
    // 1. Se não for o Primário, redireciona!
    if (!replicationManager.isPrimary()) {
      throw new WebApplicationException(
          Response.temporaryRedirect(URI.create(replicationManager.getPrimaryURI())).build());
    }

    // 2. Deixa o baseServer fazer a magia toda (validar pass, gerar ID, e enviar
    // para domínios remotos!)
    Result<String> res = baseServer.postMessage(pwd, msg);

    if (res.isOK()) {
      // 3. Descobrir os destinatários válidos (para os teus secundários não terem de
      // ir ao UsersServer)
      Set<String> localDestinations = msg.getDestination().stream()
          .filter(this::isLocalAddress)
          .collect(Collectors.toSet());

      Result<Set<String>> unknownRes = Clients.AdminUsersClient.get().checkUsers(localDestinations);
      Set<String> validRecipients = new HashSet<>(localDestinations);
      if (unknownRes.isOK())
        validRecipients.removeAll(unknownRes.value());

      // 4. Cria a versão e o pacote local! (Aqui NÃO mandas SID, metes null)
      long v = replicationManager.incrementAndGetVersion();
      ReplicationLogEntry wrapper = new ReplicationLogEntry(
          v, OperationType.POST_MESSAGE, msg, res.value(), validRecipients, null, null);

      // 5. Guarda no disco e envia para os TEUS secundários
      versionLog.put(v, wrapper);
      replicationManager.logAndPropagateToSecundaries(wrapper);
    }
    return res;
  }

  @Override
  public Result<Void> remotePostMessage(Message msg) {
    Long incomingSid = getSidFromRequestHeaders();

    if (!replicationManager.isPrimary()) {
      throw new WebApplicationException(
          Response.temporaryRedirect(URI.create(replicationManager.getPrimaryURI())).build());
    }

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

}

package sd2526.trab.impl.rest.servers;

import jakarta.inject.Singleton;
import sd2526.trab.api.Message;
import sd2526.trab.api.rest.RestReplicatedMessages;
import sd2526.trab.impl.java.servers.ReplicatedMessages;
import sd2526.trab.impl.java.utils.ReplicationLogEntry;
import java.util.List;

@Singleton
public class RestReplicatedMessagesResource extends RestResource implements RestReplicatedMessages {

  private final ReplicatedMessages repManager;

  public RestReplicatedMessagesResource(ReplicatedMessages repEngine) {
    this.repManager = repEngine;
  }

  @Override
  public void replicatePostMessage(long version, String secret, Message msg) {
    // O repManager deve devolver um Result<Void> e validar o secret lá dentro
    super.resultOrThrow(repManager.replicatePostMessage(version, secret, msg));
  }

  @Override
  public void replicateRemoveFromUserInbox(long version, String secret, String name, String mid) {
    super.resultOrThrow(repManager.replicateRemoveFromUserInbox(version, secret, name, mid));
  }

  @Override
  public void replicateDeleteMessage(long version, String secret, String name, String mid) {
    super.resultOrThrow(repManager.replicateDeleteMessage(version, secret, name, mid));
  }

  @Override
  public List<ReplicationLogEntry> getState(long fromVersion, String secret) {
    return super.resultOrThrow(repManager.getState(fromVersion, secret));
  }
}
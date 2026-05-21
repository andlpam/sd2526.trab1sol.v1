package sd2526.trab.impl.rest.servers;

import java.util.logging.Logger;
import org.glassfish.jersey.server.ResourceConfig;
import sd2526.trab.api.java.Messages;
import sd2526.trab.impl.rest.filters.VersionHeaderHandler;
import sd2526.trab.impl.zookeeper.LeaderElection;
import sd2526.trab.impl.zookeeper.ReplicationManager;
import sd2526.trab.impl.utils.IP;
import sd2526.trab.impl.java.servers.ReplicatedMessages;
import sd2526.trab.impl.java.servers.ProxyMessages;

public class RestReplicatedMessagesServer extends AbstractRestServer {
  public static final int PORT = 7987;
  private static Logger Log = Logger.getLogger(RestReplicatedMessagesServer.class.getName());

  // A variável tem de ser estática para sobreviver à inicialização do Java!
  private static ReplicationManager repManager;

  private static Messages replicatedEngine;

  public RestReplicatedMessagesServer() {
    super(Log, Messages.SERVICE_NAME, PORT);
  }

  @Override
  void registerResources(ResourceConfig config) {
    config.register(replicatedEngine);
  }

  public static void main(String[] args) {
    String zkServers = (args.length > 0) ? args[0] : "localhost:2181";

    String myURI = String.format("https://%s:%s/rest", IP.hostname(), PORT);

    Log.info("A iniciar eleição de líder com Zookeeper em: " + zkServers + " para " + myURI);

    LeaderElection election = new LeaderElection(zkServers, Messages.SERVICE_NAME, myURI);
    repManager = new ReplicationManager(election);
    election.start();

    Messages tmp = ProxyMessages.getInstance();

    replicatedEngine = new ReplicatedMessages(tmp, repManager);

    new RestReplicatedMessagesServer().start();
  }
}
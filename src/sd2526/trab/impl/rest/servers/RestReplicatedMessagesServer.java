package sd2526.trab.impl.rest.servers;

import java.util.logging.Logger;
import org.glassfish.jersey.server.ResourceConfig;
import sd2526.trab.api.java.Messages;
import sd2526.trab.impl.rest.filters.SidHeaderHandler;
import sd2526.trab.impl.rest.filters.VersionHeaderHandler;
import sd2526.trab.impl.zookeeper.LeaderElection;
import sd2526.trab.impl.zookeeper.ReplicationManager;
import sd2526.trab.impl.utils.IP;
import sd2526.trab.impl.java.servers.ReplicatedMessages;
import sd2526.trab.impl.java.servers.JavaMessages;

public class RestReplicatedMessagesServer extends AbstractRestServer {
  public static final int PORT = 7987;
  private static Logger Log = Logger.getLogger(RestReplicatedMessagesServer.class.getName());

  private static ReplicationManager repManager;
  private static ReplicatedMessages replicatedEngine;

  public RestReplicatedMessagesServer() {
    super(Log, Messages.SERVICE_NAME, PORT);
  }

  @Override
  void registerResources(ResourceConfig config) {
    // Usar registerInstances para o Jersey detetar bem os métodos (como fizemos nos
    // outros)
    config.registerInstances(new RestMessagesResource(replicatedEngine));
    config.registerInstances(new RestReplicatedMessagesResource(replicatedEngine));

    config.register(new VersionHeaderHandler(repManager));
    config.register(new SidHeaderHandler());
  }

  public static void main(String[] args) {
    try {
      System.out.println(">>>> MAIN DO REPLICATED A ARRANCAR! <<<<");

      String zkServers = (args.length > 0) ? args[0] : "localhost:2181";
      String secret = (args.length > 1) ? args[1] : "default_secret";

      String myURI = String.format("https://%s:%s/rest", IP.hostname(), PORT);

      Log.info("A iniciar eleição de líder com Zookeeper em: " + zkServers + " para " + myURI);

      LeaderElection election = new LeaderElection(zkServers, Messages.SERVICE_NAME, myURI);
      repManager = new ReplicationManager(election, secret);
      election.start();

      // CORREÇÃO FATAL: O baseServer tem de ser o JavaMessages (Base de Dados local)
      // e não o Proxy do Zoho!
      Messages baseServer = JavaMessages.getInstance();

      replicatedEngine = new ReplicatedMessages(baseServer, repManager);

      // Esperar 1.5s para dar tempo à rede e ao Zookeeper de elegerem o líder
      Thread.sleep(1500);

      if (!repManager.isPrimary()) {
        Log.info("Sou secundário! A iniciar recovery...");
        replicatedEngine.recoverStateFromPrimary();
      } else {
        Log.info("Fui eleito Primário pelo Zookeeper!");
      }

      new RestReplicatedMessagesServer().start();

    } catch (Exception e) {
      System.err.println(">>>> ERRO CATASTRÓFICO NO ARRANQUE DO SERVIDOR REPLICADO <<<<");
      e.printStackTrace();
    }
  }
}
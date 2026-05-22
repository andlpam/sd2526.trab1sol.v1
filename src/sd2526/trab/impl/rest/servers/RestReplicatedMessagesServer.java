package sd2526.trab.impl.rest.servers;

import java.util.logging.Logger;
import org.glassfish.jersey.server.ResourceConfig;
import sd2526.trab.api.java.Messages;
import sd2526.trab.impl.java.servers.JavaMessages;
import sd2526.trab.impl.java.servers.KafkaReplicatedMessages;
import sd2526.trab.impl.rest.filters.VersionHeaderHandler;

public class RestReplicatedMessagesServer extends AbstractRestServer {
  private static Logger Log = Logger.getLogger(RestReplicatedMessagesServer.class.getName());
  private static Messages replicatedEngine;

  public RestReplicatedMessagesServer(int port) {
    super(Log, Messages.SERVICE_NAME, port);
  }

  @Override
  void registerResources(ResourceConfig config) {
    // Registar as fachadas com a nova engine Kafka
    config.registerInstances(new RestMessagesResource(replicatedEngine));
    config.registerInstances(new VersionHeaderHandler((KafkaReplicatedMessages) replicatedEngine));
  }

  public static void main(String[] args) {
    try {
      System.out.println(">>>> MAIN DO REPLICATED (KAFKA) A ARRANCAR! <<<<");

      // Argumentos que o Tester nos passa
      String kafkaServers = (args.length > 0) ? args[0] : "kafka:9092";
      // O porto, caso o Tester queira injetar (senão usa o default)
      int port = (args.length > 2) ? Integer.parseInt(args[2]) : 7987;

      Messages baseServer = JavaMessages.getInstance(); // A nossa base de dados local
      replicatedEngine = new KafkaReplicatedMessages(baseServer, kafkaServers);

      new RestReplicatedMessagesServer(port).start();

    } catch (Exception e) {
      System.err.println(">>>> ERRO CATASTRÓFICO <<<<");
      e.printStackTrace();
    }
  }
}
package sd2526.trab.impl.rest.servers;

import java.util.logging.Logger;
import org.glassfish.jersey.server.ResourceConfig;
import sd2526.trab.api.java.Messages;
import sd2526.trab.impl.rest.filters.VersionHeaderHandler;

public class RestReplicatedMessagesServer extends AbstractRestServer {
  public static final int PORT = 7987;
  //
  private static Logger Log = Logger.getLogger(RestMessagesServer.class.getName());
  private ReplicationManager repManager;

  RestReplicatedMessagesServer(String zkServers) {
    super(Log, Messages.SERVICE_NAME, PORT);

  }

  @Override
  void registerResources(ResourceConfig config) {
    config.register(new RestMessagesResource());
    config.register(new VersionHeaderHandler(repManager));
  }

  public static void main(String[] args) {
    new RestMessagesServer().start();
  }
}

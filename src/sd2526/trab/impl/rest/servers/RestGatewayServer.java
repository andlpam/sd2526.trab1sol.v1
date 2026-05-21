package sd2526.trab.impl.rest.servers;

import java.util.logging.Logger;

import org.glassfish.jersey.server.ResourceConfig;

import sd2526.trab.impl.java.clients.Clients;

public class RestGatewayServer extends AbstractRestServer {

	public static final int PORT = 6666;

	private static Logger Log = Logger.getLogger(RestGatewayServer.class.getName());

	RestGatewayServer() {
		super(Log, null, PORT);
	}

	@Override
	void registerResources(ResourceConfig config) {
		config.registerInstances(new RestUsersResource(Clients.UsersClient.get()),
				new RestMessagesResource(Clients.MessagesClient.get()));
	}

	public static void main(String[] args) {
		new RestGatewayServer().start();
	}
}
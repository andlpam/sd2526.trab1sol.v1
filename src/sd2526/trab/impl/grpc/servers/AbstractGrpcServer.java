package sd2526.trab.impl.grpc.servers;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.util.List;
import java.util.logging.Logger;

import javax.net.ssl.KeyManagerFactory;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import sd2526.trab.impl.discovery.Discovery;
import sd2526.trab.impl.java.servers.AbstractServer;
import sd2526.trab.impl.utils.IP;

public abstract class AbstractGrpcServer extends AbstractServer {
	private static final String SERVER_BASE_URI = "grpc://%s:%s%s";
	private static final String GRPC_CTX = "/grpc";

	protected Server server;

	protected AbstractGrpcServer(Logger log, String service, int port) {
		super(log, service, String.format(SERVER_BASE_URI, IP.hostname(), port, GRPC_CTX));

		String keyStoreFilename = System.getProperty("javax.net.ssl.keyStore");
		String keyStorePassword = System.getProperty("javax.net.ssl.keyStorePassword");

		try (FileInputStream input = new FileInputStream(keyStoreFilename)) {
			KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
			keyStore.load(input, keyStorePassword.toCharArray());

			KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(
					KeyManagerFactory.getDefaultAlgorithm());
			keyManagerFactory.init(keyStore, keyStorePassword.toCharArray());

			SslContext context = GrpcSslContexts.configure(
					SslContextBuilder.forServer(keyManagerFactory)).build();

			var builder = NettyServerBuilder.forPort(port).sslContext(context);
			for (var s : controllers(super.serverURI)) {
				builder.addService(s);
			}

			this.server = builder.build();
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Failed to start gRPC server", e);
		}
	}

	protected abstract List<GrpcController> controllers(String uri);

	protected void start() throws IOException {
		Discovery.getInstance().announce(serviceName(), super.serverURI);

		Log.info(String.format("%s gRPC Server ready @ %s\n", service, serverURI));

		server.start();
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			System.err.println("*** shutting down gRPC server since JVM is shutting down");
			server.shutdownNow();
			System.err.println("*** server shut down");
		}));
	}
}
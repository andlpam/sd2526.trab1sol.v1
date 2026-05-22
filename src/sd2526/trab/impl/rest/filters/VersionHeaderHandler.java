package sd2526.trab.impl.rest.filters;

import java.io.IOException;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.ext.Provider;
import sd2526.trab.impl.java.servers.KafkaReplicatedMessages;

@Provider
public class VersionHeaderHandler implements ContainerRequestFilter, ContainerResponseFilter {

  private final KafkaReplicatedMessages engine;

  public VersionHeaderHandler(KafkaReplicatedMessages engine) {
    this.engine = engine;
  }

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    String clientVersion = requestContext.getHeaderString("X-MESSAGES-version");
    if (clientVersion != null) {
      engine.awaitVersion(Long.parseLong(clientVersion));
    }
  }

  @Override
  public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext)
      throws IOException {
    // Injeta a versão local na resposta para o Tester guardar e mandar no próximo
    // pedido
    responseContext.getHeaders().add("X-MESSAGES-version", String.valueOf(engine.getLocalVersion()));
  }
}
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

  public static final ThreadLocal<Long> version = new ThreadLocal<>();

  private final KafkaReplicatedMessages engine;

  public VersionHeaderHandler(KafkaReplicatedMessages engine) {
    this.engine = engine;
  }

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    version.set(null);
    String clientVersion = requestContext.getHeaderString("X-MESSAGES-version");
    if (clientVersion != null && !clientVersion.trim().isEmpty()) {
      try {
        // O Tester pode juntar cabeçalhos com vírgula. Apanhamos só o primeiro para não
        // dar erro.
        String cleanVersion = clientVersion.split(",")[0].trim();
        engine.awaitVersion(Long.parseLong(cleanVersion));
      } catch (NumberFormatException e) {
        // Se o Tester mandar lixo que não é número, ignoramos em vez de dar Erro 500
      }
    }
  }

  @Override
  public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext)
      throws IOException {
    Long v = version.get();
    if (v == null) {
      v = engine.getLocalVersion();
    }
    responseContext.getHeaders().add("X-MESSAGES-version", String.valueOf(v));
  }
}
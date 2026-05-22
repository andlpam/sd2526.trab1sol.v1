package sd2526.trab.impl.rest.filters;

import java.io.IOException;
import java.net.URI; // Importar a URI
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.ext.Provider;

@Provider
public class SidHeaderHandler implements ContainerRequestFilter {

  public static final ThreadLocal<Long> incomingSid = new ThreadLocal<>();
  // NOVA LINHA: Guarda a URI exata do pedido (com o path todo)
  public static final ThreadLocal<URI> requestUri = new ThreadLocal<>();

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    // Guarda a URI completa antes de entrar no ReplicatedMessages
    requestUri.set(requestContext.getUriInfo().getRequestUri());

    String sidStr = requestContext.getHeaderString("X-MESSAGES-sid");
    if (sidStr != null) {
      incomingSid.set(Long.parseLong(sidStr));
    } else {
      incomingSid.remove();
    }
  }
}
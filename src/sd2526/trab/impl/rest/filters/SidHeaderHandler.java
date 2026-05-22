package sd2526.trab.impl.rest.filters;

import java.io.IOException;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.ext.Provider;
import sd2526.trab.impl.zookeeper.ReplicationManager;

@Provider
public class SidHeaderHandler implements ContainerRequestFilter {

  public static final ThreadLocal<Long> incomingSid = new ThreadLocal<>();

  @Override
  public void filter(ContainerRequestContext reqCtx) throws IOException {
    // 1. Lemos o valor que vem no pedido da rua
    String value = reqCtx.getHeaderString(HEADER_SID);

    if (value != null && !value.isEmpty()) {
      try {
        // 2. Guardamos o número na gaveta
        incomingSid.set(Long.valueOf(value));
      } catch (NumberFormatException e) {
        // Se vier lixo, limpamos a gaveta
        incomingSid.remove();
      }
    } else {
      // Se o pedido não trouxer SID, garantimos que a gaveta está vazia
      incomingSid.remove();
    }
  }
}

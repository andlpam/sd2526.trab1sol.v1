package sd2526.trab.impl.rest.filters;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.ext.Provider;
import sd2526.trab.api.rest.RestMessages;
import sd2526.trab.api.rest.RestReplicatedMessages;
import sd2526.trab.impl.zookeeper.ReplicationManager;

import java.io.IOException;

@Provider
public class VersionHeaderHandler implements ContainerResponseFilter, ContainerRequestFilter {

  private final ReplicationManager repManager;

  public VersionHeaderHandler(ReplicationManager repManager) {
    this.repManager = repManager;
  }

  @Override
  public void filter(ContainerRequestContext reqCtx) throws IOException {
    String path = reqCtx.getUriInfo().getPath();
    if (path.contains(RestReplicatedMessages.PATH)) { // ignora tudo o que vai para "/replicate"
      return;
    }
    String value = reqCtx.getHeaderString(RestMessages.HEADER_VERSION);
    if (value != null && !value.isEmpty()) {
      try {
        long requiredVersion = Long.parseLong(value);
        repManager.awaitVersion(requiredVersion);
      } catch (NumberFormatException e) {
      }
    }
  }

  @Override
  public void filter(ContainerRequestContext reqCtx, ContainerResponseContext resCtx) throws IOException {
    long currentVersion = repManager.getCurrentVersion();
    resCtx.getHeaders().add(RestMessages.HEADER_VERSION, Long.toString(currentVersion));
  }
}
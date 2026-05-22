package sd2526.trab.api.rest;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import sd2526.trab.api.Message;
import java.util.List;
import sd2526.trab.impl.java.utils.ReplicationLogEntry;

@Path(RestReplicatedMessages.PATH)
public interface RestReplicatedMessages {

  final String PATH = "/replicate";
  final String SECRET = "secret";
  final String NAME = "name";
  final String MID = "mid";
  final String FROM_VERSION = "fromVersion";

  @POST
  @Path("/message")
  @Consumes(MediaType.APPLICATION_JSON)
  void replicatePostMessage(
      @HeaderParam(RestMessages.HEADER_VERSION) long version,
      @QueryParam(SECRET) String secret,
      Message msg);

  @DELETE
  @Path(RestMessages.MBOX + "/{" + NAME + "}/{" + MID + "}")
  void replicateRemoveFromUserInbox(
      @HeaderParam(RestMessages.HEADER_VERSION) long version,
      @QueryParam(SECRET) String secret,
      @PathParam(NAME) String name,
      @PathParam(MID) String mid);

  @DELETE
  @Path("/{" + NAME + "}/{" + MID + "}")
  void replicateDeleteMessage(
      @HeaderParam(RestMessages.HEADER_VERSION) long version,
      @QueryParam(SECRET) String secret,
      @PathParam(NAME) String name,
      @PathParam(MID) String mid);

  @GET
  @Path("/state")
  @Produces(MediaType.APPLICATION_JSON)
  List<ReplicationLogEntry> getState(
      @QueryParam(FROM_VERSION) long fromVersion,
      @QueryParam(SECRET) String secret);
}

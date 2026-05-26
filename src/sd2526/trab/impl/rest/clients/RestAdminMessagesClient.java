package sd2526.trab.impl.rest.clients;

import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.MediaType;
import sd2526.trab.api.Message;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.rest.RestMessages;
import sd2526.trab.impl.api.java.AdminMessages;
import sd2526.trab.impl.api.rest.RestAdminMessages;

public class RestAdminMessagesClient extends RestClient implements AdminMessages {

	public RestAdminMessagesClient(String serverURI) {
		super(serverURI, RestMessages.PATH);
	}

	@Override
	public Result<Void> remotePostMessage(Message m, long sid) {
		return super.reTry(() -> doRemotePostMessage(m, sid));
	}

	@Override
	public Result<Void> remoteDeleteMessage(String mid, long sid) {
		return super.reTry(() -> doRemoteDeleteMessage(mid, sid));
	}

	@Override
	public Result<Void> remoteDeleteUserInbox(String name, long sid) {
		return super.reTry(() -> doRemoteDeleteUserInbox(name, sid));
	}

	private Result<Void> doRemotePostMessage(Message msg, long sid) {
		return super.toJavaResult(target
				.path(RestAdminMessages.ADMIN)
				.request()
				.header(RestAdminMessages.HEADER_SID, sid)
				.post(Entity.entity(msg, MediaType.APPLICATION_JSON)));
	}

	private Result<Void> doRemoteDeleteMessage(String mid, long sid) {
		return super.toJavaResult(target
				.path(RestAdminMessages.ADMIN)
				.path(mid)
				.request()
				.header(RestAdminMessages.HEADER_SID, sid)
				.delete());
	}

	private Result<Void> doRemoteDeleteUserInbox(String name, long sid) {
		return super.toJavaResult(target
				.path(RestAdminMessages.ADMIN)
				.path(RestAdminMessages.INBOX)
				.path(name)
				.request()
				.header(RestAdminMessages.HEADER_SID, sid)
				.delete());
	}

	@Override
	public Result<Void> adminRemoveInboxMessage(String name, String mid) {
		return Result.error(Result.ErrorCode.NOT_IMPLEMENTED);
	}
}

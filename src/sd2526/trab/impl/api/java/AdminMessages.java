package sd2526.trab.impl.api.java;

import sd2526.trab.api.Message;
import sd2526.trab.api.java.Result;

public interface AdminMessages {

	Result<Void> remotePostMessage(Message m, long sid);

	Result<Void> remoteDeleteMessage(String mid, long sid);

	Result<Void> remoteDeleteUserInbox(String name, long sid);

	Result<Void> adminRemoveInboxMessage(String name, String mid);
}

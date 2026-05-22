package sd2526.trab.impl.java.servers;

import static sd2526.trab.api.java.Result.ok;

import java.time.Duration;
import java.util.Collection;
import java.util.List;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import sd2526.trab.api.Message;
import sd2526.trab.api.java.Result;
import sd2526.trab.impl.db.DB;

public class JavaMessages extends AbstractMessages {

	private static final long DIRTY_INBOX_CACHE_EXPIRATION = 10000;

	protected final Cache<String, String> gcDeletedMessageCache = CacheBuilder.newBuilder()
			.expireAfterWrite(Duration.ofMillis(DIRTY_INBOX_CACHE_EXPIRATION))
			.removalListener((removed) -> {
				var sqlExpr = """
						SELECT * FROM Message m
						  WHERE NOT EXISTS
						    (SELECT 1 FROM InboxEntry e WHERE e.mid = m.id)
						""";

				DB.transaction((hibernate) -> hibernate.select(sqlExpr, Message.class)
						.thenWith((orphans) -> hibernate.deleteMany(orphans)));
			})
			.build();

	private static JavaMessages instance;

	private JavaMessages() {
		super();
	}

	public static synchronized JavaMessages getInstance() {
		if (instance == null) {
			instance = new JavaMessages();
		}
		return instance;
	}

	@Override
	protected Result<Message> fetchInboxMessage(String name, String mid) {
		return DB.getOne(new InboxEntry(mid, name), InboxEntry.class)
				.then(() -> DB.getOne(mid, Message.class));
	}

	@Override
	protected Result<List<String>> fetchAllInboxMessages(String name) {
		var sqlExpr = "SELECT m.mid FROM InboxEntry m WHERE m.recipient = '%s'".formatted(name);
		return DB.select(sqlExpr, String.class);
	}

	@Override
	protected Result<List<String>> executeSearchInbox(String name, String query) {
		var sqlExpr = """
				SELECT m.id FROM Message m
				INNER JOIN InboxEntry e
				ON e.mid = m.id
				AND e.recipient = '%s'
				WHERE (upper(m.subject) LIKE '%%%s%%' OR upper(m.contents) LIKE '%%%s%%')
				""".formatted(name, query.toUpperCase(), query.toUpperCase());

		return DB.select(sqlExpr, String.class);
	}

	@Override
	protected Result<Void> performRemoveInboxMessage(String name, String mid) {
		return DB.deleteOne(new InboxEntry(mid, name)).mapToVoid()
				.then(() -> {
					gcDeletedMessageCache.put(mid, mid);
					return ok();
				});
	}

	@Override
	protected Result<Void> deleteFromLocalInbox(String mid) {
		var sql = "SELECT * FROM InboxEntry e WHERE e.mid = '%s'".formatted(mid);

		return DB.transaction(hibernate -> {
			hibernate.getOne(mid, Message.class)
					.thenWith(msg -> hibernate.deleteOne(msg));

			return hibernate.select(sql, InboxEntry.class)
					.thenWith((entries) -> hibernate.deleteMany(entries));
		});
	}

	@Override
	protected void deliverToKnownLocalRecipients(Collection<String> addresses, Message msg) {
		DB.transaction((hibernate) -> {
			hibernate.persistOne(msg);
			for (var address : addresses) {
				hibernate.persistOne(new InboxEntry(msg.getId(), getName(address)));
			}
			return ok();
		});
	}

	@Override
	public Result<Void> remoteDeleteUserInbox(String name) {
		Log.info(() -> "remoteDeleteUserInbox : name = %s\n".formatted(name));

		var sqlExpr = "SELECT * FROM InboxEntry e WHERE e.recipient = '%s'".formatted(name);

		return DB.transaction(hibernate -> {
			return hibernate.select(sqlExpr, InboxEntry.class)
					.thenWith((entries) -> {
						hibernate.deleteMany(entries);
						for (var e : entries) {
							gcDeletedMessageCache.put(e.mid, e.mid);
						}
						return ok();
					});
		});
	}

	@Override
	public Result<Void> adminRemoveInboxMessage(String name, String mid) {
		Log.info(() -> "adminRemoveInboxMessage (Secundário) : name = %s, mid = %s\n".formatted(name, mid));

		return performRemoveInboxMessage(name, mid);
	}

}
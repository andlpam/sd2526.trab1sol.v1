package sd2526.trab.impl.java.servers;

import sd2526.trab.impl.api.java.AdminMessages;
import sd2526.trab.api.java.Messages;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.Message;
import sd2526.trab.api.User;
import sd2526.trab.impl.java.utils.OperationType;
import sd2526.trab.impl.utils.IP;
import sd2526.trab.impl.db.DB;
import sd2526.trab.impl.rest.filters.VersionHeaderHandler;
import sd2526.trab.impl.java.clients.Clients;

import java.util.List;
import java.util.ArrayList;
import java.util.UUID;
import java.util.HashSet;
import java.util.concurrent.*;
import java.util.logging.Logger;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;

public class KafkaReplicatedMessages extends JavaBaseService implements Messages, AdminMessages {

	private static final Logger Log = Logger.getLogger(KafkaReplicatedMessages.class.getName());

	private final String TOPIC_NAME;
	private final String DOMAIN;
	private final Messages baseServer;
	private final ObjectMapper jsonMapper = new ObjectMapper();

	private KafkaProducer<String, String> producer;
	private KafkaConsumer<String, String> consumer;

	private final AtomicLong localVersion = new AtomicLong(-1);
	private final String replicaId = UUID.randomUUID().toString();

	private final Map<String, Long> lastSeenSids = new ConcurrentHashMap<>();

	private long endOffsetOnStartup = -1;

	public KafkaReplicatedMessages(Messages baseServer, String kafkaServers) {
		this.baseServer = baseServer;
		this.DOMAIN = IP.domain();
		this.TOPIC_NAME = "messages-replicated-" + DOMAIN;
		initKafka(kafkaServers);
	}

	public long getLocalVersion() {
		return localVersion.get();
	}

	public void awaitVersion(long targetVersion) {
		long startTime = System.currentTimeMillis();
		synchronized (localVersion) {
			while (localVersion.get() < targetVersion) {
				if (System.currentTimeMillis() - startTime > 15000)
					break;
				try {
					localVersion.wait(1000);
				} catch (InterruptedException e) {
					break;
				}
			}
		}
	}

	private void initKafka(String kafkaServers) {
		java.util.Properties prodProps = new java.util.Properties();
		prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
		prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		prodProps.put(ProducerConfig.ACKS_CONFIG, "all");
		prodProps.put(ProducerConfig.RETRIES_CONFIG, 0);
		producer = new KafkaProducer<>(prodProps);

		java.util.Properties consProps = new java.util.Properties();
		consProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
		// Use a unique group ID every time to force replay from 'earliest' because the DB is cleared on startup
		consProps.put(ConsumerConfig.GROUP_ID_CONFIG, "replica-" + IP.hostname() + "-" + UUID.randomUUID().toString());
		consProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		consProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		consProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		consProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		consumer = new KafkaConsumer<>(consProps);
		consumer.subscribe(Collections.singletonList(TOPIC_NAME));

		// Find end offset to distinguish replay from live events
		try {
			var tp = new org.apache.kafka.common.TopicPartition(TOPIC_NAME, 0);
			var endOffsets = consumer.endOffsets(Collections.singletonList(tp));
			if (endOffsets != null && endOffsets.containsKey(tp))
				endOffsetOnStartup = endOffsets.get(tp);
		} catch (Exception e) {
			Log.warning("Could not determine end offset: " + e.getMessage());
		}

		new Thread(this::consumeLoop).start();
	}

	public static class KafkaEvent {
		public OperationType type;
		public String origin;
		public String sourceDomain;
		public long sid;
		public Message msg;
		public String user;
		public String mid;
		public Set<String> localRecipients;
		public Set<String> remoteDomains;

		public KafkaEvent() {
		}

		public KafkaEvent(OperationType type, String origin, Message msg, String user, String mid,
				Set<String> localRecipients) {
			this.type = type;
			this.origin = origin;
			this.msg = msg;
			this.user = user;
			this.mid = mid;
			this.localRecipients = localRecipients;
		}

		public KafkaEvent(OperationType type, String origin, String sourceDomain, long sid, Message msg, String user,
				String mid) {
			this.type = type;
			this.origin = origin;
			this.sourceDomain = sourceDomain;
			this.sid = sid;
			this.msg = msg;
			this.user = user;
			this.mid = mid;
		}
	}

	private void consumeLoop() {
		while (true) {
			try {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
				for (ConsumerRecord<String, String> record : records) {
					try {
						KafkaEvent event = jsonMapper.readValue(record.value(), KafkaEvent.class);
						long offset = record.offset();

						processEvent(event, offset);

						synchronized (localVersion) {
							localVersion.set(offset);
							localVersion.notifyAll();
						}
					} catch (Exception e) {
						Log.warning("Error processing record: " + e.getMessage());
					}
				}
			} catch (Exception e) {
				Log.warning("Error in consume loop: " + e.getMessage());
			}
		}
	}

	private void processEvent(KafkaEvent event, long offset) {
		// Inter-domain sid tracking
		if (event.sourceDomain != null && !event.sourceDomain.equals(DOMAIN)) {
			long lastSid = lastSeenSids.getOrDefault(event.sourceDomain, -1L);
			if (event.sid <= lastSid) {
				Log.info("Discarding late/duplicate event from " + event.sourceDomain + " with sid " + event.sid);
				return;
			}
			lastSeenSids.put(event.sourceDomain, event.sid);
		}

		AbstractMessages absBase = (AbstractMessages) baseServer;
		JavaMessages jm = (JavaMessages) baseServer;

		boolean isReplay = offset < endOffsetOnStartup;

		switch (event.type) {
			case POST_MESSAGE:
				if (event.msg != null) {
					String mid = "%s+%04d".formatted(DOMAIN, offset);
					var existing = absBase.messagesCache.getIfPresent(event.msg.originId());
					if (existing != null && !existing.getId().equals(mid)) {
						Log.info("Discarding duplicate POST_MESSAGE event for originId: " + event.msg.originId());
						return;
					}

					event.msg.setId("%s+%04d".formatted(DOMAIN, offset));
					absBase.messagesCache.put(event.msg.originId(), new Message(event.msg));
					absBase.messagesCache.put(event.msg.getId(), event.msg);

					if (event.localRecipients != null && !event.localRecipients.isEmpty()) {
						jm.deliverToKnownLocalRecipients(event.localRecipients, event.msg);
					}

					// Propagate to remote domains only if not replaying
					if (!isReplay) {
						propagateRemotePost(event.msg, offset);
					}
				}
				break;

			case DELETE_MESSAGE:
				if (event.mid != null) {
					Message msg = absBase.messagesCache.getIfPresent(event.mid);
					if (msg == null) {
						Result<Message> res = DB.getOne(event.mid, Message.class);
						if (res.isOK())
							msg = res.value();
					}

					jm.deleteFromLocalInbox(event.mid);
					absBase.messagesCache.invalidate(event.mid);

					if (!isReplay) {
						if (event.remoteDomains != null && !event.remoteDomains.isEmpty()) {
							for (String targetDomain : event.remoteDomains) {
								absBase.jobs.submit(targetDomain, () -> {
									reTry(() -> Clients.AdminMessagesClient.get(targetDomain).remoteDeleteMessage(event.mid, offset),
											90000);
								});
							}
						} else if (msg != null) {
							propagateRemoteDelete(msg, offset);
						}
					}
				}
				break;

			case REMOVE_INBOX:
				jm.adminRemoveInboxMessage(event.user, event.mid);
				break;

			case REMOTE_POST_MESSAGE:
				if (event.msg != null) {
					((AdminMessages) baseServer).remotePostMessage(event.msg, 0L); // sid already checked above
				}
				break;

			case REMOTE_DELETE_MESSAGE:
				if (event.mid != null) {
					((AdminMessages) baseServer).remoteDeleteMessage(event.mid, 0L);
				}
				break;

			case REMOTE_DELETE_USER_INBOX:
				if (event.user != null) {
					jm.remoteDeleteUserInbox(event.user, 0L);
				}
				break;

			default:
				break;
		}
	}

	private void propagateRemotePost(Message msg, long offset) {
		var remoteAddresses = msg.getDestination().stream()
				.filter(d -> !d.endsWith("@" + DOMAIN))
				.collect(Collectors.toSet());

		if (!remoteAddresses.isEmpty()) {
			var remoteTargets = remoteAddresses.stream().collect(
					Collectors.groupingBy(d -> d.split("@")[1], Collectors.toSet()));

			for (var e : remoteTargets.entrySet()) {
				String targetDomain = e.getKey();
				((AbstractMessages) baseServer).jobs.submit(targetDomain, () -> {
					reTry(() -> Clients.AdminMessagesClient.get(targetDomain).remotePostMessage(msg, offset), 90000);
				});
			}
		}
	}

	private void propagateRemoteDelete(Message msg, long offset) {
		var remoteDomains = msg.getDestination().stream()
				.filter(d -> !d.endsWith("@" + DOMAIN))
				.map(d -> d.split("@")[1])
				.collect(Collectors.toSet());

		for (String targetDomain : remoteDomains) {
			((AbstractMessages) baseServer).jobs.submit(targetDomain, () -> {
				reTry(() -> Clients.AdminMessagesClient.get(targetDomain).remoteDeleteMessage(msg.getId(), offset), 90000);
			});
		}
	}

	private long publish(KafkaEvent event) {
		try {
			String jsonPayload = jsonMapper.writeValueAsString(event);
			RecordMetadata metadata = producer.send(new ProducerRecord<>(TOPIC_NAME, jsonPayload)).get();
			return metadata.offset();
		} catch (Exception e) {
			throw new WebApplicationException(Response.Status.SERVICE_UNAVAILABLE);
		}
	}

	@Override
	public Result<String> postMessage(String pwd, Message msg) {
		Result<User> userRes = ((AbstractMessages) baseServer).getUser(msg.getSender(), pwd);
		if (!userRes.isOK())
			return Result.error(userRes.error());

		User user = userRes.value();
		msg.setSender("%s <%s@%s>".formatted(user.getDisplayName(), user.getName(), user.getDomain()));

		// Idempotence check
		var cached = ((AbstractMessages) baseServer).messagesCache.getIfPresent(msg.originId());
		if (cached != null)
			return Result.ok(cached.getId());

		// Check local recipients existence (non-deterministic part)
		var localAddresses = msg.getDestination().stream()
				.filter(d -> d.endsWith("@" + DOMAIN))
				.collect(Collectors.toList());

		Result<Set<String>> checkRes = ((AbstractMessages) baseServer).checkUsers(localAddresses);
		if (!checkRes.isOK())
			return Result.error(checkRes.error());

		Set<String> unknown = checkRes.value();
		Set<String> known = new HashSet<>(localAddresses);
		known.removeAll(unknown);

		KafkaEvent event = new KafkaEvent(OperationType.POST_MESSAGE, replicaId, msg, null, null, known);
		long offset = publish(event);

		String mid = "%s+%04d".formatted(DOMAIN, offset);
		msg.setId(mid);
		((AbstractMessages) baseServer).messagesCache.put(msg.originId(), new Message(msg));
		((AbstractMessages) baseServer).messagesCache.put(mid, msg);

		VersionHeaderHandler.version.set(offset);
		return Result.ok(mid);
	}

	@Override
	public Result<Void> deleteMessage(String name, String mid, String pwd) {
		AbstractMessages absBase = (AbstractMessages) baseServer;
		Result<User> userRes = absBase.getUser(name, pwd);
		if (!userRes.isOK())
			return Result.error(userRes.error());

		// Get remote domains before deleting to include in Kafka event
		Set<String> remoteDomains = new HashSet<>();
		Message msg = absBase.messagesCache.getIfPresent(mid);
		if (msg == null) {
			Result<Message> msgRes = DB.getOne(mid, Message.class);
			if (msgRes.isOK())
				msg = msgRes.value();
		}

		if (msg != null) {
			remoteDomains = msg.getDestination().stream()
					.filter(d -> !d.endsWith("@" + DOMAIN))
					.map(d -> d.split("@")[1])
					.collect(Collectors.toSet());
		}

		KafkaEvent event = new KafkaEvent(OperationType.DELETE_MESSAGE, replicaId, null, name, mid, null);
		event.remoteDomains = remoteDomains;
		long offset = publish(event);
		VersionHeaderHandler.version.set(offset);
		return Result.ok();
	}

	@Override
	public Result<Void> removeInboxMessage(String name, String mid, String pwd) {
		Result<User> userRes = ((AbstractMessages) baseServer).getUser(name, pwd);
		if (!userRes.isOK())
			return Result.error(userRes.error());

		KafkaEvent event = new KafkaEvent(OperationType.REMOVE_INBOX, replicaId, null, name, mid, null);
		long offset = publish(event);
		VersionHeaderHandler.version.set(offset);
		return Result.ok();
	}

	@Override
	public Result<Void> remotePostMessage(Message msg, long sid) {
		KafkaEvent event = new KafkaEvent(OperationType.REMOTE_POST_MESSAGE, replicaId,
				super.getDomain(msg.senderAddress()), sid, msg, null, null);
		long offset = publish(event);
		VersionHeaderHandler.version.set(offset);
		return Result.ok();
	}

	@Override
	public Result<Void> remoteDeleteMessage(String mid, long sid) {
		// Extract domain from mid
		String sourceDomain = mid.split("\\+")[0];
		KafkaEvent event = new KafkaEvent(OperationType.REMOTE_DELETE_MESSAGE, replicaId, sourceDomain, sid, null, null,
				mid);
		long offset = publish(event);
		VersionHeaderHandler.version.set(offset);
		return Result.ok();
	}

	@Override
	public Result<Void> remoteDeleteUserInbox(String name, long sid) {
		KafkaEvent event = new KafkaEvent(OperationType.REMOTE_DELETE_USER_INBOX, replicaId, DOMAIN, sid, null, name, null);
		long offset = publish(event);
		VersionHeaderHandler.version.set(offset);
		return Result.ok();
	}

	@Override
	public Result<Void> adminRemoveInboxMessage(String name, String mid) {
		KafkaEvent event = new KafkaEvent(OperationType.REMOVE_INBOX, replicaId, null, name, mid, null);
		long offset = publish(event);
		VersionHeaderHandler.version.set(offset);
		return Result.ok();
	}

	@Override
	public Result<Message> getInboxMessage(String name, String mid, String pwd) {
		return baseServer.getInboxMessage(name, mid, pwd);
	}

	@Override
	public Result<List<String>> getAllInboxMessages(String name, String pwd) {
		return baseServer.getAllInboxMessages(name, pwd);
	}

	@Override
	public Result<List<String>> searchInbox(String name, String pwd, String query) {
		return baseServer.searchInbox(name, pwd, query);
	}
}
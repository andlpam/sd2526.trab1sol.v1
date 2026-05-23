package sd2526.trab.impl.java.servers;

import sd2526.trab.impl.api.java.AdminMessages;
import sd2526.trab.api.java.Messages;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.Message;
import sd2526.trab.impl.java.utils.OperationType;
import sd2526.trab.impl.utils.IP;

import java.util.List;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.logging.Logger;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaReplicatedMessages extends JavaBaseService implements Messages, AdminMessages {

  private static final Logger Log = Logger.getLogger(KafkaReplicatedMessages.class.getName());

  private final String TOPIC_NAME;
  private final Messages baseServer;

  private final ObjectMapper jsonMapper = new ObjectMapper();

  private KafkaProducer<String, String> producer;
  private KafkaConsumer<String, String> consumer;

  private final Map<String, CompletableFuture<Result<?>>> pendingRequests = new ConcurrentHashMap<>();

  private final AtomicLong localVersion = new AtomicLong(-1);

  private final Object stateLock = new Object();

  private final String replicaId = UUID.randomUUID().toString();

  public KafkaReplicatedMessages(Messages baseServer, String kafkaServers) {

    this.baseServer = baseServer;

    String hostname = IP.hostname();

    String domain = hostname.contains(".")
        ? hostname.substring(hostname.indexOf(".") + 1)
        : "default";

    this.TOPIC_NAME = "messages-replicated-" + domain;

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
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
  }

  private void initKafka(String kafkaServers) {

    java.util.Properties prodProps = new java.util.Properties();

    prodProps.put(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        kafkaServers);

    prodProps.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());

    prodProps.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());

    prodProps.put(
        ProducerConfig.ACKS_CONFIG,
        "all");

    producer = new KafkaProducer<>(prodProps);

    java.util.Properties consProps = new java.util.Properties();

    consProps.put(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
        kafkaServers);

    consProps.put(
        ConsumerConfig.GROUP_ID_CONFIG,
        "replica-" + UUID.randomUUID());

    consProps.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());

    consProps.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());

    consProps.put(
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
        "earliest");

    consumer = new KafkaConsumer<>(consProps);

    consumer.subscribe(Collections.singletonList(TOPIC_NAME));

    new Thread(this::consumeLoop).start();
  }

  public static class KafkaEvent {

    public String reqId;

    public String origin;

    public OperationType type;

    public String pwd;

    public Message msg;

    public String name;

    public String mid;

    public KafkaEvent() {
    }

    public KafkaEvent(
        String reqId,
        String origin,
        OperationType type,
        String pwd,
        Message msg,
        String name,
        String mid) {

      this.reqId = reqId;
      this.origin = origin;
      this.type = type;
      this.pwd = pwd;
      this.msg = msg;
      this.name = name;
      this.mid = mid;
    }
  }

  private void consumeLoop() {

    while (true) {

      try {

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

        for (ConsumerRecord<String, String> record : records) {

          KafkaEvent event = null;

          Result<?> result = Result.error(Result.ErrorCode.INTERNAL_ERROR);

          try {

            event = jsonMapper.readValue(
                record.value(),
                KafkaEvent.class);

            synchronized (stateLock) {
              result = applyLocally(event);
            }

          } catch (Exception e) {

            Log.warning("Consume error: " + e.getMessage());

          } finally {

            synchronized (localVersion) {

              localVersion.set(record.offset());

              localVersion.notifyAll();
            }

            if (event != null
                && event.reqId != null
                && replicaId.equals(event.origin)) {

              CompletableFuture<Result<?>> future = pendingRequests.remove(event.reqId);

              if (future != null && !future.isDone()) {
                future.complete(result);
              }
            }
          }
        }

      } catch (Exception e) {

        Log.warning("Kafka loop failure: " + e.getMessage());
      }
    }
  }

  private Result<?> applyLocally(KafkaEvent ev) {

    AdminMessages adminBase = (AdminMessages) baseServer;

    try {

      switch (ev.type) {

        case POST_MESSAGE:
          return baseServer.postMessage(ev.pwd, ev.msg);

        case REMOTE_POST_MESSAGE:
          return adminBase.remotePostMessage(ev.msg);

        case REMOVE_INBOX:
          return baseServer.removeInboxMessage(
              ev.name,
              ev.mid,
              ev.pwd);

        case DELETE_MESSAGE:
          return baseServer.deleteMessage(
              ev.name,
              ev.mid,
              ev.pwd);

        case REMOTE_DELETE_MESSAGE:
          return adminBase.remoteDeleteMessage(ev.mid);

        case REMOTE_DELETE_USER_INBOX:
          return adminBase.remoteDeleteUserInbox(ev.name);

        default:
          return Result.error(Result.ErrorCode.NOT_IMPLEMENTED);
      }

    } catch (Exception e) {

      Log.warning("Apply locally failed: " + e.getMessage());

      return Result.error(Result.ErrorCode.INTERNAL_ERROR);
    }
  }

  @SuppressWarnings("unchecked")
  private <T> Result<T> publishAndWait(KafkaEvent event) {

    try {

      CompletableFuture<Result<?>> future = new CompletableFuture<>();

      pendingRequests.put(event.reqId, future);

      String jsonPayload = jsonMapper.writeValueAsString(event);

      producer.send(
          new ProducerRecord<>(
              TOPIC_NAME,
              event.reqId,
              jsonPayload));

      producer.flush();

      Result<?> result = future.get(30, TimeUnit.SECONDS);

      return (Result<T>) result;

    } catch (TimeoutException te) {

      pendingRequests.remove(event.reqId);

      Log.warning("Kafka operation timed out");

      return Result.error(Result.ErrorCode.INTERNAL_ERROR);

    } catch (Exception e) {

      pendingRequests.remove(event.reqId);

      Log.warning("Kafka publish failed: " + e.getMessage());

      return Result.error(Result.ErrorCode.INTERNAL_ERROR);
    }
  }

  // =========================================================
  // READS
  // =========================================================

  @Override
  public Result<Message> getInboxMessage(
      String name,
      String mid,
      String pwd) {

    synchronized (stateLock) {

      try {

        return baseServer.getInboxMessage(
            name,
            mid,
            pwd);

      } catch (Exception e) {

        return Result.error(Result.ErrorCode.INTERNAL_ERROR);
      }
    }
  }

  @Override
  public Result<List<String>> getAllInboxMessages(
      String name,
      String pwd) {

    synchronized (stateLock) {

      try {

        Result<List<String>> res = baseServer.getAllInboxMessages(name, pwd);

        if (res.isOK() && res.value() != null) {
          return Result.ok(new ArrayList<>(res.value()));
        }

        return res;

      } catch (Exception e) {

        return Result.error(Result.ErrorCode.INTERNAL_ERROR);
      }
    }
  }

  @Override
  public Result<List<String>> searchInbox(
      String name,
      String pwd,
      String query) {

    synchronized (stateLock) {

      try {

        Result<List<String>> res = baseServer.searchInbox(name, pwd, query);

        if (res.isOK() && res.value() != null) {
          return Result.ok(new ArrayList<>(res.value()));
        }

        return res;

      } catch (Exception e) {

        return Result.error(Result.ErrorCode.INTERNAL_ERROR);
      }
    }
  }

  // =========================================================
  // WRITES
  // =========================================================

  @Override
  public Result<String> postMessage(String pwd, Message msg) {

    KafkaEvent ev = new KafkaEvent(
        UUID.randomUUID().toString(),
        replicaId,
        OperationType.POST_MESSAGE,
        pwd,
        msg,
        null,
        null);

    return publishAndWait(ev);
  }

  @Override
  public Result<Void> remotePostMessage(Message msg) {

    KafkaEvent ev = new KafkaEvent(
        UUID.randomUUID().toString(),
        replicaId,
        OperationType.REMOTE_POST_MESSAGE,
        null,
        msg,
        null,
        null);

    return publishAndWait(ev);
  }

  @Override
  public Result<Void> removeInboxMessage(
      String name,
      String mid,
      String pwd) {

    KafkaEvent ev = new KafkaEvent(
        UUID.randomUUID().toString(),
        replicaId,
        OperationType.REMOVE_INBOX,
        pwd,
        null,
        name,
        mid);

    return publishAndWait(ev);
  }

  @Override
  public Result<Void> deleteMessage(
      String name,
      String mid,
      String pwd) {

    KafkaEvent ev = new KafkaEvent(
        UUID.randomUUID().toString(),
        replicaId,
        OperationType.DELETE_MESSAGE,
        pwd,
        null,
        name,
        mid);

    return publishAndWait(ev);
  }

  @Override
  public Result<Void> remoteDeleteMessage(String mid) {

    KafkaEvent ev = new KafkaEvent(
        UUID.randomUUID().toString(),
        replicaId,
        OperationType.REMOTE_DELETE_MESSAGE,
        null,
        null,
        null,
        mid);

    return publishAndWait(ev);
  }

  @Override
  public Result<Void> remoteDeleteUserInbox(String name) {

    KafkaEvent ev = new KafkaEvent(
        UUID.randomUUID().toString(),
        replicaId,
        OperationType.REMOTE_DELETE_USER_INBOX,
        null,
        null,
        name,
        null);

    return publishAndWait(ev);
  }

  @Override
  public Result<Void> adminRemoveInboxMessage(
      String name,
      String mid) {

    KafkaEvent ev = new KafkaEvent(
        UUID.randomUUID().toString(),
        replicaId,
        OperationType.REMOVE_INBOX,
        null,
        null,
        name,
        mid);

    return publishAndWait(ev);
  }
}
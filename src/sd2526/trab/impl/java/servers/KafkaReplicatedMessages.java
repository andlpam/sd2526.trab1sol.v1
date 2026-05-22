package sd2526.trab.impl.java.servers;

import sd2526.trab.impl.api.java.AdminMessages;
import sd2526.trab.api.java.Messages;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.Message;
import sd2526.trab.impl.java.utils.OperationType;
import sd2526.trab.impl.utils.IP;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.logging.Logger;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaReplicatedMessages extends JavaBaseService implements Messages, AdminMessages {

  private static final Logger Log = Logger.getLogger(KafkaReplicatedMessages.class.getName());

  // O tópico agora usa o domínio para garantir a regra: "Cannot be used for
  // inter-domain"
  private final String TOPIC_NAME;

  private final Messages baseServer;
  private final ObjectMapper jsonMapper = new ObjectMapper();

  private KafkaProducer<String, String> producer;
  private KafkaConsumer<String, String> consumer;

  private final Map<String, CompletableFuture<Result<?>>> pendingRequests = new ConcurrentHashMap<>();

  // --- LÓGICA DE MONOTONIC READS ---
  private final AtomicLong localVersion = new AtomicLong(-1);

  public KafkaReplicatedMessages(Messages baseServer, String kafkaServers) {
    this.baseServer = baseServer;

    // Ex: "messages0.ourorg0" -> extrai "ourorg0"
    String hostname = IP.hostname();
    String domain = hostname.contains(".") ? hostname.substring(hostname.indexOf(".") + 1) : "default";
    this.TOPIC_NAME = "messages-replicated-" + domain;

    initKafka(kafkaServers);
  }

  public long getLocalVersion() {
    return localVersion.get();
  }

  public void awaitVersion(long targetVersion) {
    synchronized (localVersion) {
      while (localVersion.get() < targetVersion) {
        try {
          localVersion.wait();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  private void initKafka(String kafkaServers) {
    java.util.Properties prodProps = new java.util.Properties();
    prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
    prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    this.producer = new KafkaProducer<>(prodProps);

    java.util.Properties consProps = new java.util.Properties();
    consProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
    consProps.put(ConsumerConfig.GROUP_ID_CONFIG, "replica-" + UUID.randomUUID().toString());
    consProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    this.consumer = new KafkaConsumer<>(consProps);
    this.consumer.subscribe(Collections.singletonList(TOPIC_NAME));

    new Thread(this::consumeLoop).start();
  }

  public static class KafkaEvent {
    public String reqId;
    public OperationType type;
    public String pwd;
    public Message msg;
    public String name;
    public String mid;

    public KafkaEvent() {
    }

    public KafkaEvent(String reqId, OperationType type, String pwd, Message msg, String name, String mid) {
      this.reqId = reqId;
      this.type = type;
      this.pwd = pwd;
      this.msg = msg;
      this.name = name;
      this.mid = mid;
    }
  }

  private void consumeLoop() {
    Log.info("À escuta no Kafka! Tópico: " + TOPIC_NAME);
    while (true) {
      try {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<String, String> record : records) {
          KafkaEvent event = jsonMapper.readValue(record.value(), KafkaEvent.class);

          Result<?> result = applyLocally(event);

          // ATUALIZAR VERSÃO (Offset do Kafka serve perfeitamente como versão absoluta!)
          synchronized (localVersion) {
            localVersion.set(record.offset());
            localVersion.notifyAll(); // Acorda pedidos pendentes
          }

          CompletableFuture<Result<?>> future = pendingRequests.remove(event.reqId);
          if (future != null) {
            future.complete(result);
          }
        }
      } catch (Exception e) {
        Log.severe("Erro no consumidor Kafka: " + e.getMessage());
      }
    }
  }

  private Result<?> applyLocally(KafkaEvent ev) {
    AdminMessages adminBase = (AdminMessages) baseServer;
    switch (ev.type) {
      case POST_MESSAGE:
        return baseServer.postMessage(ev.pwd, ev.msg);
      case REMOTE_POST_MESSAGE:
        return adminBase.remotePostMessage(ev.msg);
      case REMOVE_INBOX:
        return baseServer.removeInboxMessage(ev.name, ev.mid, ev.pwd);
      case DELETE_MESSAGE:
        return baseServer.deleteMessage(ev.name, ev.mid, ev.pwd);
      case REMOTE_DELETE_MESSAGE:
        return adminBase.remoteDeleteMessage(ev.mid);
      case REMOTE_DELETE_USER_INBOX:
        return adminBase.remoteDeleteUserInbox(ev.name);
      default:
        return Result.error(Result.ErrorCode.NOT_IMPLEMENTED);
    }
  }

  @SuppressWarnings("unchecked")
  private <T> Result<T> publishAndWait(KafkaEvent event) {
    try {
      CompletableFuture<Result<?>> future = new CompletableFuture<>();
      pendingRequests.put(event.reqId, future);

      String jsonPayload = jsonMapper.writeValueAsString(event);
      producer.send(new ProducerRecord<>(TOPIC_NAME, event.reqId, jsonPayload));

      Result<?> result = future.get(10, TimeUnit.SECONDS);
      return (Result<T>) result;
    } catch (Exception e) {
      pendingRequests.remove(event.reqId);
      return Result.error(Result.ErrorCode.INTERNAL_ERROR);
    }
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

  @Override
  public Result<String> postMessage(String pwd, Message msg) {
    KafkaEvent ev = new KafkaEvent(UUID.randomUUID().toString(), OperationType.POST_MESSAGE, pwd, msg, null, null);
    return publishAndWait(ev);
  }

  @Override
  public Result<Void> remotePostMessage(Message msg) {
    KafkaEvent ev = new KafkaEvent(UUID.randomUUID().toString(), OperationType.REMOTE_POST_MESSAGE, null, msg, null,
        null);
    return publishAndWait(ev);
  }

  @Override
  public Result<Void> removeInboxMessage(String name, String mid, String pwd) {
    KafkaEvent ev = new KafkaEvent(UUID.randomUUID().toString(), OperationType.REMOVE_INBOX, pwd, null, name, mid);
    return publishAndWait(ev);
  }

  @Override
  public Result<Void> deleteMessage(String name, String mid, String pwd) {
    KafkaEvent ev = new KafkaEvent(UUID.randomUUID().toString(), OperationType.DELETE_MESSAGE, pwd, null, name, mid);
    return publishAndWait(ev);
  }

  @Override
  public Result<Void> remoteDeleteMessage(String mid) {
    KafkaEvent ev = new KafkaEvent(UUID.randomUUID().toString(), OperationType.REMOTE_DELETE_MESSAGE, null, null, null,
        mid);
    return publishAndWait(ev);
  }

  @Override
  public Result<Void> remoteDeleteUserInbox(String name) {
    KafkaEvent ev = new KafkaEvent(UUID.randomUUID().toString(), OperationType.REMOTE_DELETE_USER_INBOX, null, null,
        name, null);
    return publishAndWait(ev);
  }

  @Override
  public Result<Void> adminRemoveInboxMessage(String name, String mid) {
    KafkaEvent ev = new KafkaEvent(UUID.randomUUID().toString(), OperationType.REMOVE_INBOX, null, null, name, mid);
    return publishAndWait(ev);
  }
}
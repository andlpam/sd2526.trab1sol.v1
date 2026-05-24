package sd2526.trab.impl.java.servers;

import sd2526.trab.impl.api.java.AdminMessages;
import sd2526.trab.api.java.Messages;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.Message;
import sd2526.trab.impl.java.utils.OperationType;
import sd2526.trab.impl.utils.IP;
import sd2526.trab.impl.db.DB;

import java.util.List;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.logging.Logger;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

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
    String domain = hostname.contains(".") ? hostname.substring(hostname.indexOf(".") + 1) : "default";
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
    producer = new KafkaProducer<>(prodProps);

    java.util.Properties consProps = new java.util.Properties();
    consProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
    consProps.put(ConsumerConfig.GROUP_ID_CONFIG, "replica-" + UUID.randomUUID());
    consProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
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

    public KafkaEvent(String reqId, String origin, OperationType type, String pwd, Message msg, String name,
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
          try {
            event = jsonMapper.readValue(record.value(), KafkaEvent.class);

            if (!replicaId.equals(event.origin)) {
              synchronized (stateLock) {
                AbstractMessages absBase = (AbstractMessages) baseServer;
                JavaMessages jm = (JavaMessages) baseServer;

                try {
                  switch (event.type) {
                    case POST_MESSAGE:
                    case REMOTE_POST_MESSAGE:
                      if (event.msg != null) {
                        absBase.messagesCache.put(event.msg.originId(), new Message(event.msg));
                        absBase.messagesCache.put(event.msg.getId(), event.msg);

                        try {
                          String[] parts = event.msg.getId().split("\\+");
                          if (parts.length == 2) {
                            long c = Long.parseLong(parts[1]);
                            absBase.counter.accumulateAndGet(c, Math::max);
                          }
                        } catch (Exception ignored) {
                        }

                        String myDomain = TOPIC_NAME.replace("messages-replicated-", "");
                        List<String> localDests = new ArrayList<>();
                        if (event.msg.getDestination() != null) {
                          for (String dest : event.msg.getDestination()) {
                            String name = dest;
                            String dom = myDomain;
                            if (dest.contains("@")) {
                              String[] dparts = dest.split("@", 2);
                              name = dparts[0];
                              dom = dparts[1];
                            }
                            if (dom.equals(myDomain))
                              localDests.add(name);
                          }
                        }
                        jm.deliverToKnownLocalRecipients(localDests, event.msg);
                      }
                      break;

                    case REMOVE_INBOX:
                      jm.adminRemoveInboxMessage(event.name, event.mid);
                      break;

                    case DELETE_MESSAGE:
                    case REMOTE_DELETE_MESSAGE:
                      if (event.mid != null)
                        absBase.messagesCache.invalidate(event.mid);
                      jm.deleteFromLocalInbox(event.mid);
                      break;

                    case REMOTE_DELETE_USER_INBOX:
                      jm.remoteDeleteUserInbox(event.name);
                      break;

                    default:
                      break;
                  }
                } catch (Exception e) {
                  Log.warning("Erro Backup: " + e.getMessage());
                }
              }
            }
          } catch (Exception e) {
          } finally {
            synchronized (localVersion) {
              localVersion.set(record.offset());
              localVersion.notifyAll();
            }
            if (event != null && replicaId.equals(event.origin)) {
              CompletableFuture<Result<?>> future = pendingRequests.remove(event.reqId);
              if (future != null)
                future.complete(Result.ok());
            }
          }
        }
      } catch (Exception e) {
      }
    }
  }

  private void publishAndWait(KafkaEvent event) {
    try {
      CompletableFuture<Result<?>> future = new CompletableFuture<>();
      pendingRequests.put(event.reqId, future);
      String jsonPayload = jsonMapper.writeValueAsString(event);
      producer.send(new ProducerRecord<>(TOPIC_NAME, event.reqId, jsonPayload));
      producer.flush();

      Result<?> result = future.get(15, TimeUnit.SECONDS);
      if (!result.isOK() && result.error() == Result.ErrorCode.INTERNAL_ERROR) {
        throw new WebApplicationException(Response.Status.SERVICE_UNAVAILABLE);
      }
    } catch (Exception e) {
      pendingRequests.remove(event.reqId);
      throw new WebApplicationException(Response.Status.SERVICE_UNAVAILABLE);
    }
  }

  private void restoreMessageToCache(String mid) {
    try {
      AbstractMessages absBase = (AbstractMessages) baseServer;
      if (absBase.messagesCache.getIfPresent(mid) == null) {
        Result<Message> msgRes = DB.getOne(mid, Message.class);
        if (msgRes.isOK() && msgRes.value() != null) {
          Message m = msgRes.value();
          absBase.messagesCache.put(m.originId(), m);
          absBase.messagesCache.put(m.getId(), m);
          Log.info("MENSAGEM " + mid + " SALVA DA CACHE EXPIRADA!");
        }
      }
    } catch (Exception ignored) {
    }
  }

  @Override
  public Result<String> postMessage(String pwd, Message msg) {
    Result<String> res = baseServer.postMessage(pwd, msg);
    if (res.isOK()) {
      KafkaEvent ev = new KafkaEvent(UUID.randomUUID().toString(), replicaId, OperationType.POST_MESSAGE, pwd, msg,
          null, res.value());
      publishAndWait(ev);
    } else if (res.error() == Result.ErrorCode.INTERNAL_ERROR) {
      throw new WebApplicationException(Response.Status.SERVICE_UNAVAILABLE);
    }
    return res;
  }

  @Override
  public Result<Void> remotePostMessage(Message msg) {
    Result<Void> res = ((AdminMessages) baseServer).remotePostMessage(msg);
    if (res.isOK()) {
      KafkaEvent ev = new KafkaEvent(UUID.randomUUID().toString(), replicaId, OperationType.REMOTE_POST_MESSAGE, null,
          msg, null, null);
      publishAndWait(ev);
    } else if (res.error() == Result.ErrorCode.INTERNAL_ERROR) {
      throw new WebApplicationException(Response.Status.SERVICE_UNAVAILABLE);
    }
    return res;
  }

  @Override
  public Result<Void> removeInboxMessage(String name, String mid, String pwd) {
    Result<Void> res = baseServer.removeInboxMessage(name, mid, pwd);
    if (res.isOK()) {
      KafkaEvent ev = new KafkaEvent(UUID.randomUUID().toString(), replicaId, OperationType.REMOVE_INBOX, pwd, null,
          name, mid);
      publishAndWait(ev);
    } else if (res.error() == Result.ErrorCode.INTERNAL_ERROR) {
      throw new WebApplicationException(Response.Status.SERVICE_UNAVAILABLE);
    }
    return res;
  }

  @Override
  public Result<Void> deleteMessage(String name, String mid, String pwd) {
    restoreMessageToCache(mid);

    Result<Void> res = baseServer.deleteMessage(name, mid, pwd);
    if (res.isOK()) {
      KafkaEvent ev = new KafkaEvent(UUID.randomUUID().toString(), replicaId, OperationType.DELETE_MESSAGE, pwd, null,
          name, mid);
      publishAndWait(ev);
    } else if (res.error() == Result.ErrorCode.INTERNAL_ERROR) {
      throw new WebApplicationException(Response.Status.SERVICE_UNAVAILABLE);
    }
    return res;
  }

  @Override
  public Result<Void> remoteDeleteMessage(String mid) {
    Result<Void> res = ((AdminMessages) baseServer).remoteDeleteMessage(mid);
    if (res.isOK()) {
      KafkaEvent ev = new KafkaEvent(UUID.randomUUID().toString(), replicaId, OperationType.REMOTE_DELETE_MESSAGE, null,
          null, null, mid);
      publishAndWait(ev);
    } else if (res.error() == Result.ErrorCode.INTERNAL_ERROR) {
      throw new WebApplicationException(Response.Status.SERVICE_UNAVAILABLE);
    }
    return res;
  }

  @Override
  public Result<Void> remoteDeleteUserInbox(String name) {
    Result<Void> res = ((AdminMessages) baseServer).remoteDeleteUserInbox(name);
    if (res.isOK()) {
      KafkaEvent ev = new KafkaEvent(UUID.randomUUID().toString(), replicaId, OperationType.REMOTE_DELETE_USER_INBOX,
          null, null, name, null);
      publishAndWait(ev);
    } else if (res.error() == Result.ErrorCode.INTERNAL_ERROR) {
      throw new WebApplicationException(Response.Status.SERVICE_UNAVAILABLE);
    }
    return res;
  }

  @Override
  public Result<Void> adminRemoveInboxMessage(String name, String mid) {
    Result<Void> res = ((AdminMessages) baseServer).adminRemoveInboxMessage(name, mid);
    if (res.isOK()) {
      KafkaEvent ev = new KafkaEvent(UUID.randomUUID().toString(), replicaId, OperationType.REMOVE_INBOX, null, null,
          name, mid);
      publishAndWait(ev);
    } else if (res.error() == Result.ErrorCode.INTERNAL_ERROR) {
      throw new WebApplicationException(Response.Status.SERVICE_UNAVAILABLE);
    }
    return res;
  }

  @Override
  public Result<Message> getInboxMessage(String name, String mid, String pwd) {
    synchronized (stateLock) {
      try {
        Result<Message> res = baseServer.getInboxMessage(name, mid, pwd);
        if (!res.isOK() && res.error() == Result.ErrorCode.INTERNAL_ERROR)
          return Result.error(Result.ErrorCode.NOT_FOUND);
        return res;
      } catch (Exception e) {
        return Result.error(Result.ErrorCode.NOT_FOUND);
      }
    }
  }

  @Override
  public Result<List<String>> getAllInboxMessages(String name, String pwd) {
    synchronized (stateLock) {
      try {
        Result<List<String>> res = baseServer.getAllInboxMessages(name, pwd);
        if (res.isOK() && res.value() != null)
          return Result.ok(new ArrayList<>(res.value()));
        return res;
      } catch (Exception e) {
        return Result.error(Result.ErrorCode.INTERNAL_ERROR);
      }
    }
  }

  @Override
  public Result<List<String>> searchInbox(String name, String pwd, String query) {
    synchronized (stateLock) {
      try {
        Result<List<String>> res = baseServer.searchInbox(name, pwd, query);
        if (res.isOK() && res.value() != null)
          return Result.ok(new ArrayList<>(res.value()));
        if (res.error() != Result.ErrorCode.INTERNAL_ERROR)
          return res;
      } catch (Exception ignored) {
      }

      try {
        Result<List<String>> allRes = baseServer.getAllInboxMessages(name, pwd);
        if (!allRes.isOK())
          return allRes;
        List<String> matched = new ArrayList<>();
        for (String mid : allRes.value()) {
          try {
            Result<Message> msgRes = baseServer.getInboxMessage(name, mid, pwd);
            if (msgRes.isOK() && msgRes.value() != null) {
              Message m = msgRes.value();
              boolean match = false;
              if (m.getSubject() != null && m.getSubject().contains(query))
                match = true;
              if (m.getContents() != null && m.getContents().contains(query))
                match = true;
              if (match)
                matched.add(mid);
            }
          } catch (Exception ignored) {
          }
        }
        return Result.ok(matched);
      } catch (Exception e) {
        return Result.ok(new ArrayList<>());
      }
    }
  }
}
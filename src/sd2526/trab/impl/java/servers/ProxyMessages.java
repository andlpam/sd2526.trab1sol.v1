package sd2526.trab.impl.java.servers;

import static sd2526.trab.api.java.Result.error;
import static sd2526.trab.api.java.Result.ok;
import static sd2526.trab.api.java.Result.ErrorCode.BAD_REQUEST;
import static sd2526.trab.api.java.Result.ErrorCode.FORBIDDEN;
import static sd2526.trab.api.java.Result.ErrorCode.INTERNAL_ERROR;

import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import sd2526.trab.api.Message;
import sd2526.trab.api.User;
import sd2526.trab.api.java.Messages;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.java.Result.ErrorCode;
import sd2526.trab.impl.api.java.AdminMessages;
import sd2526.trab.impl.db.Zoho;
import sd2526.trab.impl.db.utils.JSON;
import sd2526.trab.impl.java.clients.Clients;
import sd2526.trab.impl.utils.IP;

public class ProxyMessages extends JavaBaseService implements Messages, AdminMessages {

  private static final int REMOTE_COMM_DEADLINE = 90000;
  private static final long MESSAGES_CACHE_EXPIRATION = 30000;

  final JobDispatcher jobs;
  final AtomicLong counter = new AtomicLong(0L);
  private static Logger Log = Logger.getLogger(ProxyMessages.class.getName());

  protected final Cache<String, Message> messagesCache = CacheBuilder.newBuilder()
      .expireAfterWrite(Duration.ofMillis(MESSAGES_CACHE_EXPIRATION))
      .build();

  private ProxyMessages() {
    this.jobs = new JobDispatcher();
  }

  private String serializeToEmailBody(Message msg) {
    return msg.getContents() + "\n------\n" + JSON.encode(msg);
  }

  private Message deserializeFromEmailBody(String emailBody) {
    if (emailBody == null)
      return null;
    String[] parts = emailBody.split("------");
    if (parts.length > 1) {
      try {
        return JSON.decode(parts[1].trim(), Message.class);
      } catch (Exception e) {
        Log.warning("Falha ao descodificar mensagem do email: " + e.getMessage());
      }
    }
    return null;
  }

  @Override
  public Result<String> postMessage(String pwd, Message msg) {
    Log.info(() -> "postMessage (Proxy) : pwd = %s, msg = %s\n".formatted(pwd, msg));

    return getUser(msg.getSender(), pwd)
        .thenWith((user) -> doAsyncPost(user, msg));
  }

  @Override
  public Result<Message> getInboxMessage(String name, String mid, String pwd) {
    Log.info(() -> "getInboxMessage (Proxy) : name = %s, mid = %s, pwd = %s\n".formatted(name, mid, pwd));

    if (badParams(name, mid, pwd))
      return error(BAD_REQUEST);

    return getUser(name, pwd).then(() -> {
      try {
        List<String> emailBodies = Zoho.getInstance().getAllEmails();

        for (String body : emailBodies) {
          Message msg = deserializeFromEmailBody(body);
          if (msg != null && msg.getId().equals(mid)) {
            return ok(msg);
          }
        }
        return error(Result.ErrorCode.NOT_FOUND);
      } catch (Exception e) {
        e.printStackTrace();
        return error(INTERNAL_ERROR);
      }
    });
  }

  @Override
  public Result<List<String>> getAllInboxMessages(String name, String pwd) {
    Log.info(() -> "getAllInboxMessages (Proxy) : name = %s, pwd = %s\n".formatted(name, pwd));

    return getUser(name, pwd).then(() -> {
      try {
        List<String> emailBodies = Zoho.getInstance().getAllEmails();
        List<String> mids = emailBodies.stream()
            .map(this::deserializeFromEmailBody)
            .filter(m -> m != null)
            .map(Message::getId)
            .toList();
        return ok(mids);
      } catch (Exception e) {
        e.printStackTrace();
        return error(INTERNAL_ERROR);
      }
    });
  }

  @Override
  public Result<List<String>> searchInbox(String name, String pwd, String query) {
    Log.info(() -> "searchInbox (Proxy) : name = %s, pwd = %s, query=%s\n".formatted(name, pwd, query));

    String upperQuery = query.toUpperCase();
    return getUser(name, pwd).then(() -> {
      try {
        List<String> emailBodies = Zoho.getInstance().getAllEmails();
        List<String> matchedMids = emailBodies.stream()
            .map(this::deserializeFromEmailBody)
            .filter(m -> m != null)
            .filter(m -> m.getSubject().toUpperCase().contains(upperQuery) ||
                m.getContents().toUpperCase().contains(upperQuery))
            .map(Message::getId)
            .toList();
        return ok(matchedMids);
      } catch (Exception e) {
        e.printStackTrace();
        return error(INTERNAL_ERROR);
      }
    });
  }

  @Override
  public Result<Void> removeInboxMessage(String name, String mid, String pwd) {
    Log.info(() -> "removeInboxMessage (Proxy) : name = %s, mid = %s, pwd = %s\n".formatted(name, mid, pwd));

    return getUser(name, pwd).then(() -> deleteFromLocalInbox(mid));
  }

  @Override
  public Result<Void> deleteMessage(String name, String mid, String pwd) {
    Log.info(() -> "deleteMessage (Proxy) : name = %s, mid = %s, pwd = %s\n".formatted(name, mid, pwd));

    return getUser(name, pwd)
        .then(() -> getCachedMessage(mid))
        .thenWith(msg -> name.equals(getName(msg.senderAddress())) ? ok(msg) : error(FORBIDDEN))
        .thenWith((msg) -> doAsyncDelete(msg));
  }

  protected Result<User> getUser(String user, String pwd) {
    try {
      var name = user.split("@", 2)[0];
      return Clients.UsersClient.get().getUser(name, pwd);
    } catch (Exception x) {
      x.printStackTrace();
      return Result.error(INTERNAL_ERROR);
    }
  }

  protected Result<Set<String>> checkUsers(Collection<String> addresses) {
    return Clients.AdminUsersClient.get().checkUsers(addresses);
  }

  private void deliverToKnownLocalRecipients(Collection<String> addresses, Message msg) {
    Log.info(() -> "deliverToKnownLocalRecipients (Proxy) : local known addresses = %s, msg = %s\n".formatted(addresses,
        msg));
    try {
      String subject = msg.getSubject();
      String emailBody = serializeToEmailBody(msg);

      Zoho.getInstance().sendEmail(subject, emailBody);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void reportUnknownLocalRecipients(Collection<String> addresses, Message msg) {
    Log.info(
        () -> "reportUnknownLocalRecipients (Proxy) : unknown addresses = %s, msg = %s\n".formatted(addresses, msg));
    var senderDomain = super.getDomain(msg.senderAddress());

    try {
      for (var recipientAddress : addresses) {
        var errorMsg = msg.cloneWithUserNotFound(recipientAddress);
        if (super.isLocalDomain(senderDomain)) {
          deliverToKnownLocalRecipients(Set.of(msg.senderAddress()), errorMsg);
        } else {
          doAsyncRemotePost(senderDomain, errorMsg);
        }
      }
    } catch (Exception x) {
      x.printStackTrace();
    }
  }

  private Result<Void> postToLocalInboxes(Collection<String> addresses, Message msg) {
    Log.info(() -> "postToLocalInboxes (Proxy) : localRecipients = %s, msg = %s\n".formatted(addresses, msg));

    return checkUsers(addresses)
        .thenWith(unknownAddresses -> {
          var knownAddresses = new HashSet<>(addresses);
          knownAddresses.removeAll(unknownAddresses);

          if (knownAddresses.size() > 0)
            deliverToKnownLocalRecipients(knownAddresses, msg);

          if (unknownAddresses.size() > 0)
            reportUnknownLocalRecipients(unknownAddresses, msg);

          return ok();
        });
  }

  @Override
  public Result<Void> remotePostMessage(Message msg) {
    Log.info(() -> "remotePostMessage (Proxy) : msg = %s\n".formatted(msg));
    var localAddresses = getLocalRecipientAddresses(msg);
    return postToLocalInboxes(localAddresses, msg);
  }

  private Result<Void> deleteFromLocalInbox(String mid) {
    Log.info(() -> "deleteFromLocalInbox (Proxy) : mid = %s\n".formatted(mid));
    try {
      Zoho.getInstance().deleteEmailBySystemMid(mid);
      return ok();
    } catch (Exception e) {
      e.printStackTrace();
      return error(INTERNAL_ERROR);
    }
  }

  @Override
  public Result<Void> remoteDeleteMessage(String mid) {
    Log.info(() -> "remoteDeleteMessage (Proxy) : mid = %s\n".formatted(mid));
    return deleteFromLocalInbox(mid);
  }

  @Override
  public Result<Void> remoteDeleteUserInbox(String name) {
    Log.info(() -> "remoteDeleteUserInbox (Proxy) : name = %s\n".formatted(name));
    try {
      Zoho.getInstance().deleteAllEmails();
      return ok();
    } catch (Exception e) {
      e.printStackTrace();
      return error(INTERNAL_ERROR);
    }
  }

  public Result<String> doAsyncPost(User sender, Message msg) {
    return getCachedMessage(msg.originId()).mapValue(Message::getId).orElse(() -> {
      msg.setId("%s+%04d".formatted(THIS_DOMAIN, counter.incrementAndGet()));
      messagesCache.put(msg.originId(), new Message(msg));

      msg.setSender("%s <%s@%s>".formatted(sender.getDisplayName(), sender.getName(), sender.getDomain()));
      messagesCache.put(msg.getId(), msg);

      var localAdresses = getLocalRecipientAddresses(msg);
      var remoteAddresses = getRemoteRecipientAddresses(msg);

      if (localAdresses.size() > 0)
        postToLocalInboxes(localAdresses, msg);

      if (remoteAddresses.size() > 0) {
        var remoteTargets = remoteAddresses.stream().collect(
            Collectors.groupingBy(super::getDomain, Collectors.mapping(address -> address, Collectors.toSet())));

        for (var e : remoteTargets.entrySet()) {
          var domain = e.getKey();
          var domainRecipientAddressess = e.getValue();

          jobs.submit(domain, () -> {
            var res = super.reTry(() -> Clients.AdminMessagesClient.get(domain).remotePostMessage(msg),
                REMOTE_COMM_DEADLINE);
            if (res.error() == ErrorCode.TIMEOUT) {
              for (var address : domainRecipientAddressess)
                postToLocalInboxes(Set.of(msg.senderAddress()), msg.cloneWithTimeout(address));
            }
          });
        }
      }
      return Result.ok(msg.getId());
    });
  }

  public Result<Void> doAsyncDelete(Message msg) {
    var domains = msg.getDestination().stream().map(r -> r.split("@")[1]).collect(Collectors.toSet());
    for (var domain : domains)
      if (domain.equals(IP.domain()))
        deleteFromLocalInbox(msg.getId());
      else
        jobs.submit(domain,
            () -> super.reTry(() -> Clients.AdminMessagesClient.get(domain).remoteDeleteMessage(msg.getId()),
                REMOTE_COMM_DEADLINE));
    return Result.ok();
  }

  public void doAsyncRemotePost(String remoteDomain, Message msg) {
    jobs.submit(remoteDomain,
        () -> super.reTry(() -> Clients.AdminMessagesClient.get(remoteDomain).remotePostMessage(msg),
            REMOTE_COMM_DEADLINE));
  }

  protected Result<Message> getCachedMessage(String mid) {
    var msg = messagesCache.getIfPresent(mid);
    return msg != null ? ok(msg) : error(FORBIDDEN);
  }

  private List<String> getLocalRecipientAddresses(Message msg) {
    return msg.getDestination().stream().filter(super::isLocalAddress).toList();
  }

  private Set<String> getRemoteRecipientAddresses(Message msg) {
    return msg.getDestination().stream().filter(Predicate.not(super::isLocalAddress)).collect(Collectors.toSet());
  }

  public final class JobDispatcher {
    private final ConcurrentHashMap<String, ExecutorService> executors = new ConcurrentHashMap<>();

    public void submit(String domain, Runnable job) {
      ExecutorService executor = executors.computeIfAbsent(domain,
          d -> Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r);
            t.setUncaughtExceptionHandler((thr, ex) -> ex.printStackTrace());
            return t;
          }));
      executor.submit(job);
    }
  }

  static ProxyMessages instance;

  public static synchronized ProxyMessages getInstance() {
    if (instance == null)
      instance = new ProxyMessages();
    return instance;
  }
}
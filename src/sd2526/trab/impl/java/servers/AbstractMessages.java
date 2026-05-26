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
import sd2526.trab.impl.java.clients.Clients;
import sd2526.trab.impl.utils.IP;

public abstract class AbstractMessages extends JavaBaseService implements Messages, AdminMessages {

  protected static final int REMOTE_COMM_DEADLINE = 90000;
  protected static final long MESSAGES_CACHE_EXPIRATION = 30000;

  protected final JobDispatcher jobs;
  protected final AtomicLong counter = new AtomicLong(0L);
  protected final Logger Log = Logger.getLogger(this.getClass().getName());

  protected final Cache<String, Message> messagesCache = CacheBuilder.newBuilder()
      .expireAfterWrite(Duration.ofMillis(MESSAGES_CACHE_EXPIRATION))
      .build();

  protected AbstractMessages() {
    this.jobs = new JobDispatcher();
  }

  protected abstract Result<Message> fetchInboxMessage(String name, String mid);

  protected abstract Result<List<String>> fetchAllInboxMessages(String name);

  protected abstract Result<List<String>> executeSearchInbox(String name, String query);

  protected abstract Result<Void> deleteFromLocalInbox(String mid);

  protected abstract Result<Void> performRemoveInboxMessage(String name, String mid);

  protected abstract void deliverToKnownLocalRecipients(Collection<String> addresses, Message msg);

  @Override
  public Result<String> postMessage(String pwd, Message msg) {
    Log.info(() -> "postMessage : pwd = %s, msg = %s\n".formatted(pwd, msg));
    return getUser(msg.getSender(), pwd).thenWith((user) -> doAsyncPost(senderName(msg.getSender()), user, msg));
  }

  @Override
  public Result<Message> getInboxMessage(String name, String mid, String pwd) {
    Log.info(() -> "getInboxMessage : name = %s, mid = %s, pwd = %s\n".formatted(name, mid, pwd));
    if (badParams(name, mid, pwd))
      return error(BAD_REQUEST);
    return getUser(name, pwd).then(() -> fetchInboxMessage(name, mid));
  }

  @Override
  public Result<List<String>> getAllInboxMessages(String name, String pwd) {
    Log.info(() -> "getAllInboxMessages : name = %s, pwd = %s\n".formatted(name, pwd));
    return getUser(name, pwd).then(() -> fetchAllInboxMessages(name));
  }

  @Override
  public Result<List<String>> searchInbox(String name, String pwd, String query) {
    Log.info(() -> "searchInbox : name = %s, pwd = %s, query=%s\n".formatted(name, pwd, query));
    return getUser(name, pwd).then(() -> executeSearchInbox(name, query));
  }

  @Override
  public Result<Void> removeInboxMessage(String name, String mid, String pwd) {
    Log.info(() -> "removeInboxMessage : name = %s, mid = %s, pwd = %s\n".formatted(name, mid, pwd));
    return getUser(name, pwd).then(() -> performRemoveInboxMessage(name, mid));
  }

  @Override
  public Result<Void> deleteMessage(String name, String mid, String pwd) {
    Log.info(() -> "deleteMessage : name = %s, mid = %s, pwd = %s\n".formatted(name, mid, pwd));
    return getUser(name, pwd)
        .then(() -> getCachedMessage(mid))
        .thenWith(msg -> name.equals(getName(msg.senderAddress())) ? ok(msg) : error(FORBIDDEN))
        .thenWith(this::doAsyncDelete);
  }

  @Override
  public Result<Void> remotePostMessage(Message msg, long sid) {
    Log.info(() -> "remotePostMessage : msg = %s, sid = %d\n".formatted(msg, sid));
    var localAddresses = getLocalRecipientAddresses(msg);
    return postToLocalInboxes(localAddresses, msg);
  }

  @Override
  public Result<Void> remoteDeleteMessage(String mid, long sid) {
    Log.info(() -> "remoteDeleteMessage : mid = %s, sid = %d\n".formatted(mid, sid));
    return deleteFromLocalInbox(mid);
  }

  private String senderName(String sender) {
    if (sender.contains("<")) {
      return sender.substring(sender.indexOf("<") + 1, sender.indexOf("@"));
    }
    return sender.split("@")[0];
  }

  public Result<String> doAsyncPost(String senderName, User sender, Message msg) {
    return getCachedMessage(msg.originId()).mapValue(Message::getId).orElse(() -> {

      msg.setId("%s+%04d".formatted(THIS_DOMAIN, counter.incrementAndGet()));

      messagesCache.put(msg.originId(), new Message(msg));
      msg.setSender("%s <%s@%s>".formatted(sender.getDisplayName(), sender.getName(), sender.getDomain()));
      messagesCache.put(msg.getId(), msg);

      var localAdresses = getLocalRecipientAddresses(msg);
      var remoteAddresses = getRemoteRecipientAddresses(msg);

      System.out.println("Local Recipients:" + localAdresses);
      System.out.println("Remote Recipients:" + remoteAddresses);

      if (localAdresses.size() > 0)
        postToLocalInboxes(localAdresses, msg);

      if (remoteAddresses.size() > 0) {
        var remoteTargets = remoteAddresses.stream().collect(
            Collectors.groupingBy(super::getDomain, Collectors.mapping(address -> address, Collectors.toSet())));

        for (var e : remoteTargets.entrySet()) {
          var domain = e.getKey();
          var domainRecipientAddressess = e.getValue();

          jobs.submit(domain, () -> {
            var res = super.reTry(() -> Clients.AdminMessagesClient.get(domain).remotePostMessage(msg, 0L),
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
    for (var domain : domains) {
      if (domain.equals(IP.domain())) {
        deleteFromLocalInbox(msg.getId());
      } else {
        jobs.submit(domain, () -> {
          super.reTry(() -> Clients.AdminMessagesClient.get(domain).remoteDeleteMessage(msg.getId(), 0L),
              REMOTE_COMM_DEADLINE);
        });
      }
    }
    return Result.ok();
  }

  public void doAsyncRemotePost(String remoteDomain, Message msg) {
    Log.info(() -> "\nenqueueRemotePost : remoteDomain=%s, msg = %s\n".formatted(remoteDomain, msg));
    jobs.submit(remoteDomain, () -> {
      super.reTry(() -> Clients.AdminMessagesClient.get(remoteDomain).remotePostMessage(msg, 0L), REMOTE_COMM_DEADLINE);
    });
  }

  private Result<Void> postToLocalInboxes(Collection<String> addresses, Message msg) {
    Log.info(() -> "postToLocalInboxes : localRecipients = %s, msg = %s\n".formatted(addresses, msg));
    return checkUsers(addresses).thenWith(unknownAddresses -> {

      var knownAddresses = new HashSet<>(addresses);
      knownAddresses.removeAll(unknownAddresses);

      if (knownAddresses.size() > 0)
        deliverToKnownLocalRecipients(knownAddresses, msg);

      if (unknownAddresses.size() > 0)
        reportUnknownLocalRecipients(unknownAddresses, msg);

      return ok();
    });
  }

  private void reportUnknownLocalRecipients(Collection<String> addresses, Message msg) {
    Log.info(() -> "reportUnknownLocalRecipients : unknown addresses = %s, msg = %s\n".formatted(addresses, msg));
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

  protected Result<User> getUser(String user, String pwd) {
    try {
      var name = user.contains("@") ? user.split("@", 2)[0] : user;
      return reTry(() -> Clients.UsersClient.get().getUser(name, pwd), REMOTE_COMM_DEADLINE);
    } catch (Exception x) {
      x.printStackTrace();
      return Result.error(INTERNAL_ERROR);
    }
  }

  protected Result<Set<String>> checkUsers(Collection<String> addresses) {
    return reTry(() -> Clients.AdminUsersClient.get().checkUsers(addresses), REMOTE_COMM_DEADLINE);
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
      ExecutorService executor = executors.computeIfAbsent(
          domain,
          d -> Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r);
            t.setUncaughtExceptionHandler((thr, ex) -> {
              ex.printStackTrace();
            });
            return t;
          }));
      executor.submit(job);
    }
  }
}
package sd2526.trab.impl.java.servers;

import static sd2526.trab.api.java.Result.error;
import static sd2526.trab.api.java.Result.ok;
import static sd2526.trab.api.java.Result.ErrorCode.INTERNAL_ERROR;
import static sd2526.trab.api.java.Result.ErrorCode.NOT_FOUND;

import java.util.Collection;
import java.util.List;

import sd2526.trab.api.Message;
import sd2526.trab.api.java.Result;
import sd2526.trab.impl.db.Zoho;
import sd2526.trab.impl.db.utils.JSON;

public class ProxyMessages extends AbstractMessages {

  private static ProxyMessages instance;

  private ProxyMessages() {
    super();
  }

  public static synchronized ProxyMessages getInstance() {
    if (instance == null) {
      instance = new ProxyMessages();
    }
    return instance;
  }

  private String serializeToEmailBody(Message msg) {
    return msg.getContents() + "\n------\n" + JSON.encode(msg);
  }

  private Message deserializeFromEmailBody(String emailBody) {
    if (emailBody == null) {
      return null;
    }

    try {
      int start = emailBody.indexOf('{');
      int end = emailBody.lastIndexOf('}');

      if (start != -1 && end != -1 && start < end) {
        String cleanJson = emailBody.substring(start, end + 1);
        return JSON.decode(cleanJson, Message.class);
      }
    } catch (Exception e) {
      Log.warning("Falha ao descodificar mensagem do email: " + e.getMessage());
    }
    return null;
  }

  @Override
  protected Result<Message> fetchInboxMessage(String name, String mid) {
    try {
      List<String> emailBodies = Zoho.getInstance().getAllEmails();

      for (String body : emailBodies) {
        Message msg = deserializeFromEmailBody(body);
        if (msg != null && msg.getId().equals(mid)) {
          return ok(msg);
        }
      }
      return error(NOT_FOUND);
    } catch (Exception e) {
      e.printStackTrace();
      return error(INTERNAL_ERROR);
    }
  }

  @Override
  protected Result<List<String>> fetchAllInboxMessages(String name) {
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
  }

  @Override
  protected Result<List<String>> executeSearchInbox(String name, String query) {
    String upperQuery = query.toUpperCase();
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
  }

  @Override
  protected Result<Void> performRemoveInboxMessage(String name, String mid) {
    return deleteFromLocalInbox(mid);
  }

  @Override
  protected Result<Void> deleteFromLocalInbox(String mid) {
    try {
      Zoho.getInstance().deleteEmailBySystemMid(mid);
      return ok();
    } catch (Exception e) {
      e.printStackTrace();
      return error(INTERNAL_ERROR);
    }
  }

  @Override
  protected void deliverToKnownLocalRecipients(Collection<String> addresses, Message msg) {
    try {
      String subject = msg.getSubject();
      String emailBody = serializeToEmailBody(msg);

      Zoho.getInstance().sendEmail(subject, emailBody);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public Result<Void> remoteDeleteUserInbox(String name, long sid) {
    Log.info(() -> "remoteDeleteUserInbox (Proxy) : name = %s, sid = %d\n".formatted(name, sid));
    try {
      Zoho.getInstance().deleteAllEmails();
      return ok();
    } catch (Exception e) {
      e.printStackTrace();
      return error(INTERNAL_ERROR);
    }
  }

  @Override
  public Result<Void> adminRemoveInboxMessage(String name, String mid) {
    return Result.error(Result.ErrorCode.NOT_IMPLEMENTED);
  }
}
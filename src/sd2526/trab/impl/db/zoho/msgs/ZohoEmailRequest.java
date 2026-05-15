package sd2526.trab.impl.db.zoho.msgs;

public record ZohoEmailRequest(String fromAddress, String toAddress, String subject, String content,
    String mailFormat) {
}

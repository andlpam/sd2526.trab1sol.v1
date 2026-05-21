package sd2526.trab.impl.java.utils;

import java.io.Serializable;
import java.util.Collection;
import sd2526.trab.api.Message;

public record ReplicationLogEntry(
    long version,
    OperationType operationType,
    Message message,
    String generatedId,
    Collection<String> validLocalRecipients,
    String sourceDomain,
    Long sourceSid)
    implements Serializable {
}

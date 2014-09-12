package message;

/**
 * Created by clfung on 9/11/14.
 */
public class Message {

    private OperationType operationType;
    private int schemaVersion;
    private String payload;

    public Message(OperationType type, String payload) {
        operationType = type;
        this.payload = payload;
    }

    public OperationType getOperationType() {
        return operationType;
    }

    public int getSchemaVersion() {
        return schemaVersion;
    }

    public String getPayload() {
        return payload;
    }

    public String toString() {
        return operationType.toString() + " " + payload;
    }

}

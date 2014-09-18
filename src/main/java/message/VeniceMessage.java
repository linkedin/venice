package message;

/**
 * Created by clfung on 9/11/14.
 */
public class VeniceMessage {

  public static final int DEFAULT_MAGIC_BYTE = 13;
  public static final int DEFAULT_SCHEMA_VERSION = 17;

  private byte magicByte;
  private byte schemaVersion;

  private OperationType operationType;
  private String payload;
  private Object timestamp;

  public VeniceMessage(OperationType type, String payload) {

    magicByte = DEFAULT_MAGIC_BYTE;
    schemaVersion = DEFAULT_SCHEMA_VERSION;

    operationType = type;
    this.payload = payload;

  }

  public byte getMagicByte() {
    return magicByte;
  }

  public OperationType getOperationType() {
    return operationType;
  }

  public byte getSchemaVersion() {
    return schemaVersion;
  }

  public String getPayload() {
    return payload;
  }

  public String toString() {
    return operationType.toString() + " " + payload;
  }

}

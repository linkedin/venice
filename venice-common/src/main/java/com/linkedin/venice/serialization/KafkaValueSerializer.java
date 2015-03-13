package com.linkedin.venice.serialization;

import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.message.KafkaValue;
import com.linkedin.venice.message.OperationType;
import kafka.utils.VerifiableProperties;
import org.apache.log4j.Logger;

import java.io.*;


/**
 * Serializer to encode/decode KafkaValue for Venice customized kafka message
 * Used by Kafka to convert to/from byte arrays.
 *
 * KafkaValue Schema (in order)
 * - Magic Byte
 * - Operation Type
 * - Schema Version
 * - Timestamp
 * - ZoneId
 * - Payload
 *
 */
public class KafkaValueSerializer implements Serializer<KafkaValue> {

  static final Logger logger = Logger.getLogger(KafkaValueSerializer.class.getName()); // log4j logger

  public KafkaValueSerializer(VerifiableProperties verifiableProperties) {
        /* This constructor is not used, but is required for compilation */
  }

  @Override
  /**
   * Converts from a byte array to a VeniceMessage
   * @param bytes - byte[] to be converted
   * @return Converted Venice Message
   * */
  public KafkaValue fromBytes(byte[] bytes) {

    byte magicByte;
    OperationType operationType = null;
    short schemaVersionId;
    long timestamp;
    byte zoneId;
    byte[] payload;

    ByteArrayInputStream bytesIn = null;
    ObjectInputStream objectInputStream = null;

    try {

      bytesIn = new ByteArrayInputStream(bytes);
      objectInputStream = new ObjectInputStream(bytesIn);

      /* read magicByte and validate Venice message */
      magicByte = objectInputStream.readByte();
      if (magicByte != KafkaValue.DEFAULT_MAGIC_BYTE) {
        // This means a non Venice kafka message was produced.
        throw new VeniceMessageException("Illegal magic byte given: " + magicByte);
      }

      /* read operationType */
      byte opTypeByte = objectInputStream.readByte();
      switch (opTypeByte) {
        case 1:
          operationType = OperationType.PUT;
          break;
        case 2:
          operationType = OperationType.DELETE;
          break;
        case 3:
          operationType = OperationType.PARTIAL_PUT;
          break;
        default:
          operationType = null;
          logger.error("Invalid operation type found: " + operationType);
          break;
      }

      /* read schemaVersionId - TODO: currently unused */
      schemaVersionId = objectInputStream.readShort();

      /* read timestamp  - TODO: use current time or this timestamp for new venice message? */
      timestamp = objectInputStream.readLong();

      /* read zone Id - TODO: not used to create new venice message */
      zoneId = objectInputStream.readByte();

      /* read payload, one byte at a time */
      int byteCount = objectInputStream.available();
      payload = new byte[byteCount];
      for (int i = 0; i < byteCount; i++) {
        payload[i] = objectInputStream.readByte();
      }
    } catch (VeniceMessageException e) {
      logger.error("Error occurred during deserialization of venice message", e);
      return new KafkaValue(OperationType.ERROR);
    } catch (IOException e) {
      logger.error("IOException while converting to VeniceMessage: ", e);
      return new KafkaValue(OperationType.ERROR);
    } finally {

      // safely close the input/output streams
      try {
        objectInputStream.close();
      } catch (IOException e) {
        logger.error("IOException while closing the input stream", e);
      }
      try {
        bytesIn.close();
      } catch (IOException e) {
        logger.error("IOException while closing the input stream", e);
      }
    }

    return new KafkaValue(operationType, payload, schemaVersionId);
  }

  @Override
  /**
   * Converts from a KafkaValue to a byte[]
   * @param kafkaValue - KafkaValue to be converted
   * @return Converted byte[]
   * */
  public byte[] toBytes(KafkaValue kafkaValue) {

    ByteArrayOutputStream bytesOut = null;
    ObjectOutputStream objectOutputStream = null;
    byte[] bytes = new byte[0];

    try {

      bytesOut = new ByteArrayOutputStream();
      objectOutputStream = new ObjectOutputStream(bytesOut);

      /* write Magic byte */
      objectOutputStream.writeByte(kafkaValue.getMagicByte());

      /* write operation type */
      // serialize the operation type enum
      switch (kafkaValue.getOperationType()) {
        case PUT:
          objectOutputStream.writeByte(1);
          break;
        case DELETE:
          objectOutputStream.writeByte(2);
          break;
        case PARTIAL_PUT:
          objectOutputStream.writeByte(3);
          break;
        default:
          logger.error("Operation Type not recognized: " + kafkaValue.getOperationType());
          objectOutputStream.writeByte(0);
          break;
      }

      /* write schema version Id */
      objectOutputStream.writeShort(kafkaValue.getSchemaVersionId());

      /* write timestamp */
      objectOutputStream.writeLong(kafkaValue.getTimestamp());

      /* write zone Id */
      objectOutputStream.writeByte(kafkaValue.getZoneId());

      /* write payload */
      objectOutputStream.write(kafkaValue.getValue());
      objectOutputStream.flush();

      bytes = bytesOut.toByteArray();
    } catch (IOException e) {
      logger.error("Could not serialize KafkaValue: " + kafkaValue.getValue());
    } finally {

      // safely close the input/output streams
      try {
        objectOutputStream.close();
      } catch (IOException e) {}

      try {
        bytesOut.close();
      } catch (IOException e) {}
    }

    return bytes;
  }
}

package com.linkedin.venice.serialization;

import com.linkedin.venice.exceptions.VeniceMessageException;
import kafka.serializer.Decoder;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.IOException;
import com.linkedin.venice.message.VeniceMessage;
import com.linkedin.venice.message.OperationType;


/**
 * Venice's custom serialization class. Used by Kafka to convert to/from byte arrays.
 *
 * Message Schema (in order)
 * - Magic Byte
 * - Operation Type
 * - Schema Version
 * - Timestamp
 * - ZoneId
 * - Payload
 *
 */
public class VeniceMessageSerializer implements Encoder<VeniceMessage>, Decoder<VeniceMessage> {

  static final Logger logger = Logger.getLogger(VeniceMessageSerializer.class.getName()); // log4j logger

  public VeniceMessageSerializer(VerifiableProperties verifiableProperties) {
        /* This constructor is not used, but is required for compilation */
  }

  @Override
  /**
   * Converts from a byte array to a VeniceMessage
   * @param byteArray - byte array to be converted
   * @return Converted Venice Message
   * */
  public VeniceMessage fromBytes(byte[] byteArray) {

    byte magicByte;
    OperationType operationType = null;
    short schemaVersionId;
    long timestamp;
    byte zoneId;
    byte[] payload;

    ByteArrayInputStream bytesIn = null;
    ObjectInputStream objectInputStream = null;

    try {

      bytesIn = new ByteArrayInputStream(byteArray);
      objectInputStream = new ObjectInputStream(bytesIn);

          /* read magicByte TODO: currently unused */
      magicByte = objectInputStream.readByte();

      if (magicByte != VeniceMessage.DEFAULT_MAGIC_BYTE) {
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
        default:
          operationType = null;
          logger.error("Invalid operation type found: " + operationType);
      }

      /* read schemaVersionId - TODO: currently unused */
      schemaVersionId = objectInputStream.readShort();

      /* read timestamp */
      timestamp = objectInputStream.readLong();

      /* read zone Id */
      zoneId = objectInputStream.readByte();

      /* read payload, one byte at a time */
      int byteCount = objectInputStream.available();
      payload = new byte[byteCount];
      for (int i = 0; i < byteCount; i++) {
        payload[i] = objectInputStream.readByte();
      }
    } catch (VeniceMessageException e) {
      logger.error("Error occurred during deserialization of venice message", e);
      return new VeniceMessage(OperationType.ERROR);
    } catch (IOException e) {
      logger.error("IOException while converting to VeniceMessage: ", e);
      return new VeniceMessage(OperationType.ERROR);
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

    return new VeniceMessage(operationType, payload, schemaVersionId);
  }

  @Override
  /**
   * Converts from a VeniceMessage to a byte array
   * @param byteArray - byte array to be converted
   * @return Converted Venice Message
   * */
  public byte[] toBytes(VeniceMessage vm) {

    ByteArrayOutputStream bytesOut = null;
    ObjectOutputStream objectOutputStream = null;
    byte[] message = new byte[0];

    try {

      bytesOut = new ByteArrayOutputStream();
      objectOutputStream = new ObjectOutputStream(bytesOut);

      /* write Magic byte */
      objectOutputStream.writeByte(vm.getMagicByte());

      /* write operation type */
      // serialize the operation type enum
      switch (vm.getOperationType()) {
        case PUT:
          objectOutputStream.writeByte(1);
          break;
        case DELETE:
          objectOutputStream.writeByte(2);
          break;
        default:
          logger.error("Operation Type not recognized: " + vm.getOperationType());
          objectOutputStream.writeByte(0);
          break;
      }

      /* write schema version Id */
      objectOutputStream.writeShort(vm.getSchemaVersionId());

      /* write timestamp */
      objectOutputStream.writeLong(vm.getTimestamp());

      /* write zone Id */
      objectOutputStream.writeByte(vm.getZoneId());

      /* write payload */
      objectOutputStream.write(vm.getPayload());
      objectOutputStream.flush();

      message = bytesOut.toByteArray();
    } catch (IOException e) {
      logger.error("Could not serialize message: " + vm.getPayload());
    } finally {

      // safely close the input/output streams
      try {
        objectOutputStream.close();
      } catch (IOException e) {
      }
      try {
        bytesOut.close();
      } catch (IOException e) {
      }
    }

    return message;
  }
}

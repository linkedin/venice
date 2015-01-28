package com.linkedin.venice.serialization;

import kafka.serializer.Decoder;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

import org.apache.log4j.Logger;

import com.linkedin.venice.message.OperationType;
import com.linkedin.venice.message.VeniceMessage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.IOException;


/**
 * Venice's custom serialization class. Used by Kafka to convert to/from byte arrays.
 *
 * Message Schema (in order)
 * - Magic Byte
 * - Operation Type
 * - Schema Version
 * - Timestamp
 * - Payload
 *
 */
public class VeniceMessageSerializer implements Encoder<VeniceMessage>, Decoder<VeniceMessage> {

  static final Logger logger = Logger.getLogger(VeniceMessageSerializer.class.getName()); // log4j logger

  private static final int HEADER_LENGTH = 3; // length of the VeniceMessage header in bytes

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
    byte schemaVersion;
    OperationType operationType = null;
    StringBuffer payload = new StringBuffer();

    ByteArrayInputStream bytesIn = null;
    ObjectInputStream objectInputStream = null;

    try {

      bytesIn = new ByteArrayInputStream(byteArray);
      objectInputStream = new ObjectInputStream(bytesIn);

      /* read magicByte TODO: currently unused */
      magicByte = objectInputStream.readByte();

      /* read operation type */
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
          logger.error("Operation Type not recognized: " + opTypeByte);
          // TODO throw an appropriate exception ?
      }

      /* read schemaVersion - TODO: currently unused */
      schemaVersion = objectInputStream.readByte();

      /* read payload, one character at a time */
      int byteCount = objectInputStream.available();

      for (int i = 0; i < byteCount; i++) {
        payload.append(Character.toString((char) objectInputStream.readByte()));
      }
    } catch (IOException e) {
      logger.error("IOException while converting to VeniceMessage: " + e.getMessage());
      e.printStackTrace();
    } finally {
      // safely close the input/output streams
      try {
        if (objectInputStream != null) {
          objectInputStream.close();
        }
        if (bytesIn != null) {
          bytesIn.close();
        }
      } catch (IOException e) {
        logger.error("IOException while closing input/output streams: " + e.getMessage());
      }
    }

    return new VeniceMessage(operationType, payload.toString());
  }

  @Override
  /**
   * Converts from a VeniceMessage to a byte array
   * @param byteArray - byte array to be converted
   * @return Converted Venice Message
   * */
  public byte[] toBytes(VeniceMessage veniceMessage) {

    ByteArrayOutputStream bytesOut = null;
    ObjectOutputStream objectOutputStream = null;
    byte[] message = new byte[0];

    try {

      bytesOut = new ByteArrayOutputStream();
      objectOutputStream = new ObjectOutputStream(bytesOut);

      objectOutputStream.writeByte(veniceMessage.getMagicByte());

      // serialize the operation type enum
      switch (veniceMessage.getOperationType()) {
        case PUT:
          objectOutputStream.write(1);
          break;
        case DELETE:
          objectOutputStream.write(2);
          break;
        default:
          logger.error("Operation Type not recognized: " + veniceMessage.getOperationType());
          objectOutputStream.write(0);
          break;
      }

      objectOutputStream.writeByte(veniceMessage.getSchemaVersion());

      // write the payload to the byte array
      objectOutputStream.writeBytes(veniceMessage.getPayload());
      objectOutputStream.flush();

      message = bytesOut.toByteArray();
    } catch (IOException e) {
      logger.error("IOException in serializing message \" " + veniceMessage.getPayload() + "\" : ", e);
    } finally {

      // safely close the input/output streams
      try {
        if (objectOutputStream != null) {
          objectOutputStream.close();
        }
        if (bytesOut != null) {
          bytesOut.close();
        }
      } catch (IOException e) {
        logger.error("IOException while closing input/output streams: ", e);
      }
    }

    return message;
  }
}

package com.linkedin.venice.serialization;

import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.message.KafkaKey;
import kafka.utils.VerifiableProperties;
import org.apache.log4j.Logger;

import java.io.*;


/**
 * Serializer to encode/decode KafkaKey for Venice customized kafka message
 * Used by Kafka to convert to/from byte arrays.
 *
 * KafkaKey Schema (in order)
 * - Magic Byte
 * - Payload (Key Object)
 *
 */
public class KafkaKeySerializer implements Serializer<KafkaKey> {

  static final Logger logger = Logger.getLogger(KafkaKeySerializer.class.getName()); // log4j logger

  public KafkaKeySerializer(VerifiableProperties verifiableProperties) {
        /* This constructor is not used, but is required for compilation */
  }

  @Override
  /**
   * Converts from a byte[] to a KafkaKey
   * @param bytes - byte[] to be converted
   * @return Converted Venice Message
   * */
  public KafkaKey fromBytes(byte[] bytes) {

    byte magicByte;
    byte[] payload = null;

    ByteArrayInputStream bytesIn = null;
    ObjectInputStream objectInputStream = null;

    try {

      bytesIn = new ByteArrayInputStream(bytes);
      objectInputStream = new ObjectInputStream(bytesIn);

      /* read magicByte and validate Venice message */
      magicByte = objectInputStream.readByte();
      if (magicByte != KafkaKey.DEFAULT_MAGIC_BYTE) {
        // This means a non Venice kafka message was produced.
        throw new VeniceMessageException("Illegal magic byte given: " + magicByte);
      }

      /* read payload, one byte at a time */
      int byteCount = objectInputStream.available();
      payload = new byte[byteCount];
      for (int i = 0; i < byteCount; i++) {
        payload[i] = objectInputStream.readByte();
      }
    } catch (VeniceMessageException e) {
      logger.error("Error occurred during deserialization of KafkaKey", e);
    } catch (IOException e) {
      logger.error("IOException while converting byte[] to KafkaKey: ", e);
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

      return new KafkaKey(payload);
    }
  }

  @Override
  /**
   * Converts from a KafkaKey to a byte[]
   * @param kafkaKey - KafkaKey to be converted
   * @return Converted byte[]
   * */
  public byte[] toBytes(KafkaKey kafkaKey) {

    ByteArrayOutputStream bytesOut = null;
    ObjectOutputStream objectOutputStream = null;
    byte[] bytes = new byte[0];

    try {

      bytesOut = new ByteArrayOutputStream();
      objectOutputStream = new ObjectOutputStream(bytesOut);

      /* write Magic byte */
      objectOutputStream.writeByte(kafkaKey.getMagicByte());

      /* write payload */
      objectOutputStream.write(kafkaKey.getKey());
      objectOutputStream.flush();

      bytes = bytesOut.toByteArray();
    } catch (IOException e) {
      logger.error("Could not serialize KafkaKey: " + kafkaKey.getKey());
    } finally {

      // safely close the input/output streams
      try {
        objectOutputStream.close();
      } catch (IOException e) {}

      try {
        bytesOut.close();
      } catch (IOException e) {}

      return bytes;
    }
  }
}

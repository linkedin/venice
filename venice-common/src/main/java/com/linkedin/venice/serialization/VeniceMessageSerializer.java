    package com.linkedin.venice.serialization;

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
        short schemaVersion;
        OperationType operationType = null;
        StringBuffer payload = new StringBuffer();

        ByteArrayInputStream bytesIn = null;
        ObjectInputStream objectInputStream = null;

        try {

          bytesIn = new ByteArrayInputStream(byteArray);
          objectInputStream = new ObjectInputStream(bytesIn);

          /* read magicByte TODO: currently unused */
          magicByte = objectInputStream.readByte();
            byte opTypeByte = objectInputStream.readByte();
          operationType = OperationType.getOperationType(opTypeByte);

          /* read schemaVersion - TODO: currently unused */
          schemaVersion = objectInputStream.readShort();

          /* read payload, one character at a time */
          int byteCount = objectInputStream.available();

          for (int i = 0; i < byteCount; i++) {
            payload.append(Character.toString((char) objectInputStream.readByte()));
          }
        } catch (IOException e) {
            logger.error("IOException while converting to VeniceMessage: ", e);
            return null;
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

        return new VeniceMessage(operationType, payload.toString());
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

          objectOutputStream.writeByte(vm.getMagicByte());

          byte opType = (byte) vm.getOperationType().ordinal();
          objectOutputStream.writeByte(opType);

          objectOutputStream.writeShort(vm.getSchemaVersion());

          // write the payload to the byte array
          objectOutputStream.writeBytes(vm.getPayload());
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

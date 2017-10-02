package com.linkedin.venice.samza;

import com.linkedin.venice.controllerapi.ControllerApiConstants;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.serialization.DefaultSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroGenericSerializer;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.samza.SamzaException;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;

import static com.linkedin.venice.ConfigKeys.*;


public class VeniceSystemProducer implements SystemProducer {

  private static final Schema STRING_SCHEMA = Schema.create(Schema.Type.STRING);
  private static final Schema INT_SCHEMA = Schema.create(Schema.Type.INT);
  private static final Schema LONG_SCHEMA = Schema.create(Schema.Type.LONG);
  private static final Schema FLOAT_SCHEMA = Schema.create(Schema.Type.FLOAT);
  private static final Schema DOUBLE_SCHEMA = Schema.create(Schema.Type.DOUBLE);
  private static final Schema BYTES_SCHEMA = Schema.create(Schema.Type.BYTES);
  private static final Schema BOOL_SCHEMA = Schema.create(Schema.Type.BOOLEAN);
  private static final DatumWriter<Utf8> STRING_DATUM_WRITER = new GenericDatumWriter<>(STRING_SCHEMA);
  private static final DatumWriter<Integer> INT_DATUM_WRITER = new GenericDatumWriter<>(INT_SCHEMA);
  private static final DatumWriter<Long> LONG_DATUM_WRITER = new GenericDatumWriter<>(LONG_SCHEMA);
  private static final DatumWriter<Float> FLOAT_DATUM_WRITER = new GenericDatumWriter<>(FLOAT_SCHEMA);
  private static final DatumWriter<Double> DOUBLE_DATUM_WRITER = new GenericDatumWriter<>(DOUBLE_SCHEMA);
  private static final DatumWriter<ByteBuffer> BYTES_DATUM_WRITER = new GenericDatumWriter<>(BYTES_SCHEMA);
  private static final DatumWriter<Boolean> BOOL_DATUM_WRITER = new GenericDatumWriter<>(BOOL_SCHEMA);

  //TODO:  A lot of these maps use store as the key, we could build a VeniceContext object that has all the value info
  /**
   * key is Venice store name (same as Samza stream name)
   * value is VeniceWriter to use for writing to that store
   */
  private ConcurrentMap<String, VeniceWriter<byte[], byte[]>> writerMap = new ConcurrentHashMap<>();

  /**
   * key is store name and value schema
   * value is Venice value schema ID
   */
  private ConcurrentMap<StoreAndSchema, Integer> schemaIds = new ConcurrentHashMap<>();

  /**
   * key is store name
   * value is schema
   */
  private ConcurrentMap<String, Schema> keySchemas = new ConcurrentHashMap<>();

  /**
   * key is schema
   * value is Avro serializer
   */
  private ConcurrentMap<String, VeniceAvroGenericSerializer> serializers = new ConcurrentHashMap<>();

  /**
   * key is Venice store name (same as Samza stream name)
   * value is Venice Kafka topic to write to
   */
  private ConcurrentMap<String, String> storeToTopic = new ConcurrentHashMap<>();

  /**
   * key is Venice store name
   * value is Kafka bootstrap server
   */
  private ConcurrentMap<String, String> storeToKafkaServers = new ConcurrentHashMap<>();

  private String veniceClusterName;
  private String samzaJobId;
  private ControllerApiConstants.PushType pushType;
  private ControllerClient veniceControllerClient;
  private final Time time;

  public VeniceSystemProducer(String veniceUrl, String veniceCluster, ControllerApiConstants.PushType pushType, String samzaJobId) {
    this(veniceUrl, veniceCluster, pushType, samzaJobId, SystemTime.INSTANCE);
  }

  public VeniceSystemProducer(String veniceUrl, String veniceCluster, ControllerApiConstants.PushType pushType, String samzaJobId, Time time) {
    this.veniceClusterName = veniceCluster;
    this.pushType = pushType;
    this.samzaJobId = samzaJobId;
    this.veniceControllerClient = new ControllerClient(veniceCluster, veniceUrl);
    this.time = time;
  }

  private VeniceWriter<byte[], byte[]> getVeniceWriter(String topic, String kafkaBootstrapServers) {
    Properties veniceWriterProperties = new Properties();
    veniceWriterProperties.put(KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrapServers);
    return new VeniceWriter<>(new VeniceProperties(veniceWriterProperties), topic, new DefaultSerializer(), new DefaultSerializer(), time);
  }

  @Override
  public void start() {

  }

  @Override
  public void stop() {
    for (VeniceWriter writer : writerMap.values()) {
      writer.close();
    }
  }

  @Override
  public void register(String source) {

  }

  @Override
  public void send(String source, OutgoingMessageEnvelope outgoingMessageEnvelope) {
    String store = outgoingMessageEnvelope.getSystemStream().getStream();
    String topic = storeToTopic.computeIfAbsent(store, s -> {
      VersionCreationResponse versionCreationResponse = null;
      for (int currentAttempt = 0; currentAttempt < 10; currentAttempt++) {
        versionCreationResponse = veniceControllerClient.requestTopicForWrites(s, 1, pushType, samzaJobId); // TODO, store size?
        if (!versionCreationResponse.isError()) {
          storeToKafkaServers.put(s, versionCreationResponse.getKafkaBootstrapServers());
          return versionCreationResponse.getKafkaTopic();
        } else {
          try {
            time.sleep(1000);
          } catch (InterruptedException e) {
            throw new VeniceException(e);
          }
        }
      }
      throw new SamzaException("Failed to get target version from Venice for store " + s + ": " + versionCreationResponse.getError());
    });
    Object keyObject = outgoingMessageEnvelope.getKey();
    Object valueObject = outgoingMessageEnvelope.getMessage();

    Schema keySchema = getSchemaFromObject(keyObject);

    Schema remoteKeySchema = keySchemas.computeIfAbsent(store, s -> {
      SchemaResponse keySchemaResponse = null;
      for (int currentAttempt = 0; currentAttempt < 10; currentAttempt++) {
        keySchemaResponse = veniceControllerClient.getKeySchema(s);
        if (!keySchemaResponse.isError()) {
          String schemaString = keySchemaResponse.getSchemaStr();
          return Schema.parse(schemaString);
        } else {
          try {
            time.sleep(1000);
          } catch (InterruptedException e) {
            throw new VeniceException(e);
          }
        }
      }
      throw new SamzaException("Failed to get Venice key schema for store " + s + ": " + keySchemaResponse.getError());
    });

    if (!remoteKeySchema.equals(keySchema)) {
      String serializedObject;
      if (keyObject instanceof byte[]){
        serializedObject = new String((byte[]) keyObject);
      } else if (keyObject instanceof ByteBuffer){
        serializedObject = new String(((ByteBuffer) keyObject).array());
      } else {
        serializedObject = keyObject.toString();
      }
      throw new SamzaException("Cannot write record to Venice store " + store + ", key object has schema " + keySchema
          + " which does not match Venice key schema " + remoteKeySchema + ".  Key object: " + serializedObject);
    }

    VeniceWriter<byte[], byte[]> writer =
        writerMap.computeIfAbsent(store, s -> getVeniceWriter(storeToTopic.get(s), storeToKafkaServers.get(s)));

    byte[] key = serializeObject(topic, keyObject);

    if (null == valueObject) {
      writer.delete(key);
    } else {
      Schema valueSchema = getSchemaFromObject(valueObject);
      int valueSchemaId = schemaIds.computeIfAbsent(new StoreAndSchema(store, valueSchema), storeAndSchema -> {
        SchemaResponse valueSchemaResponse = null;
        for (int currentAttempt = 0; currentAttempt < 10; currentAttempt++) {
          valueSchemaResponse =  veniceControllerClient.getValueSchemaID(storeAndSchema.getStoreName(),
              storeAndSchema.getSchema().toString());
          if (!valueSchemaResponse.isError()) {
            return valueSchemaResponse.getId();
          } else {
            try {
              time.sleep(1000);
            } catch (InterruptedException e) {
              throw new VeniceException(e);
            }
          }
        }
        throw new SamzaException("Failed to get Venice schema ID for store " + store + ": " + valueSchemaResponse.getError());
      });

      byte[] value = serializeObject(topic, valueObject);

      writer.put(key, value, valueSchemaId);
    }
  }

  @Override
  public void flush(String s) {
  }

  private static Schema getSchemaFromObject(Object object) {
    if (object instanceof IndexedRecord) {
      IndexedRecord keyAvro = (IndexedRecord) object;
      return keyAvro.getSchema();
    } else if (object instanceof CharSequence) { // convenience option.
      return STRING_SCHEMA;
    } else if (object instanceof Integer) {
      return INT_SCHEMA;
    } else if (object instanceof Long) {
      return LONG_SCHEMA;
    } else if (object instanceof Double) {
      return DOUBLE_SCHEMA;
    } else if (object instanceof Float) {
      return FLOAT_SCHEMA;
    } else if (object instanceof byte[] || object instanceof ByteBuffer) {
      return BYTES_SCHEMA;
    } else if (object instanceof Boolean) {
      return BOOL_SCHEMA;
    } else {
      throw new SamzaException("Venice System Producer only supports Avro objects, and primitives, found object of class: " + object.getClass().toString());
    }
  }

  protected byte[] serializeObject(String topic, Object input) {
    if (input instanceof IndexedRecord) {
      VeniceAvroGenericSerializer serializer = serializers.computeIfAbsent(
          ((IndexedRecord) input).getSchema().toString(), VeniceAvroGenericSerializer::new);
      return serializer.serialize(topic, input);
    } else if (input instanceof CharSequence) {
      return serializePrimitive(new Utf8(input.toString()), STRING_DATUM_WRITER);
    } else if (input instanceof Integer) {
      return serializePrimitive((Integer) input, INT_DATUM_WRITER);
    } else if (input instanceof Long) {
      return serializePrimitive((Long) input, LONG_DATUM_WRITER);
    } else if (input instanceof Double) {
      return serializePrimitive((Double) input, DOUBLE_DATUM_WRITER);
    } else if (input instanceof Float) {
      return serializePrimitive((Float) input, FLOAT_DATUM_WRITER);
    } else if (input instanceof ByteBuffer) {
      return serializePrimitive((ByteBuffer) input, BYTES_DATUM_WRITER);
    } else if (input instanceof byte[]) {
      return serializePrimitive(ByteBuffer.wrap((byte[]) input), BYTES_DATUM_WRITER);
    } else if (input instanceof Boolean) {
      return serializePrimitive((Boolean) input, BOOL_DATUM_WRITER);
    } else {
      throw new SamzaException("Can only serialize avro objects, and primitives, cannot serialize: " + input.getClass().toString());
    }
  }

  /**
   * @param input primitive object to be serialized (Utf8, int, ...)
   * @param writer DatumWriter to use for the serialization
   * @param <T> type of the input
   * @return avro binary serialized byte[]
   */
  private static <T> byte[] serializePrimitive(T input, DatumWriter<T> writer) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = new BinaryEncoder(out);
    try {
      writer.write(input, encoder);
    } catch (IOException e) {
      throw new RuntimeException("Failed to write intput: " + input + " to binary encoder", e);
    }
    return out.toByteArray();
  }

  /**
   * Used as key for schemaIds map
   */
  private static class StoreAndSchema {
    private final String storeName;
    private final Schema schema;

    String getStoreName() {
      return storeName;
    }

    Schema getSchema() {
      return schema;
    }

    StoreAndSchema(String storeName, Schema schema) {
      if (null == storeName) {
        throw new SamzaException("Store name cannot be null");
      }
      if (null == schema) {
        throw new SamzaException("Schema cannot be null");
      }
      this.storeName = storeName;
      this.schema = schema;
    }

    //Autogenerated equals and hashcode

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      StoreAndSchema that = (StoreAndSchema) o;

      if (!storeName.equals(that.storeName)) {
        return false;
      }
      return schema.equals(that.schema);
    }

    @Override
    public int hashCode() {
      int result = storeName.hashCode();
      result = 31 * result + schema.hashCode();
      return result;
    }
  }
}
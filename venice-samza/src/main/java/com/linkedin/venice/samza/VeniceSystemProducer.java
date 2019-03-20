package com.linkedin.venice.samza;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.controllerapi.ControllerApiConstants;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.serialization.avro.VeniceAvroSerializer;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.LinkedinAvroMigrationHelper;
import org.apache.avro.util.Utf8;
import org.apache.kafka.clients.producer.Callback;
import org.apache.log4j.Logger;
import org.apache.samza.SamzaException;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;

import static com.linkedin.venice.ConfigKeys.*;


public class VeniceSystemProducer implements SystemProducer {
  private static final Logger LOGGER = Logger.getLogger(VeniceSystemProducer.class);

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

  private final VeniceWriter<byte[], byte[]> veniceWriter;

  private final ConcurrentMap<Schema, Integer> valueSchemaIds = new VeniceConcurrentHashMap<>();

  private final Schema keySchema;

  /**
   * key is schema
   * value is Avro serializer
   */
  private final Map<String, VeniceAvroSerializer> serializers = new VeniceConcurrentHashMap<>();

  private final VersionCreationResponse versionCreationResponse;

  private final D2Client d2Client;
  private final D2ControllerClient controllerClient;
  private final String storeName;
  private final Time time;

  public VeniceSystemProducer(String veniceD2ZKHost, String d2ServiceName, String storeName,
      ControllerApiConstants.PushType pushType, String samzaJobId) {
    this(veniceD2ZKHost, d2ServiceName, storeName, pushType, samzaJobId, SystemTime.INSTANCE);
  }

  public VeniceSystemProducer(String veniceD2ZKHost, String d2ServiceName, String storeName,
      ControllerApiConstants.PushType pushType, String samzaJobId, Time time) {
    this.storeName = storeName;
    this.time = time;

    this.d2Client = new D2ClientBuilder().setZkHosts(veniceD2ZKHost).build();
    D2ClientUtils.startClient(d2Client);
    // Discover cluster
    D2ServiceDiscoveryResponse discoveryResponse = (D2ServiceDiscoveryResponse)
        controllerRequestWithRetry(() -> D2ControllerClient.discoverCluster(this.d2Client, d2ServiceName, this.storeName)
        );
    String clusterName = discoveryResponse.getCluster();
    LOGGER.info("Found cluster: " + clusterName + " for store: " + storeName);

    this.controllerClient = new D2ControllerClient(d2ServiceName, clusterName, this.d2Client);

    // Request all the necessary info from Venice Controller
    this.versionCreationResponse = (VersionCreationResponse)controllerRequestWithRetry(
        // sendStartOfPush must be true in order to support batch push to Venice from Samza app
        () -> this.controllerClient.requestTopicForWrites(this.storeName, 1, pushType, samzaJobId, true)
    );
    LOGGER.info("Got [store: " + this.storeName + "] VersionCreationResponse: " + this.versionCreationResponse);
    this.veniceWriter = getVeniceWriter(this.versionCreationResponse);

    SchemaResponse keySchemaResponse = (SchemaResponse)controllerRequestWithRetry(
        () -> this.controllerClient.getKeySchema(this.storeName)
    );
    LOGGER.info("Got [store: " + this.storeName + "] SchemaResponse for key schema: " + keySchemaResponse);
    this.keySchema = Schema.parse(keySchemaResponse.getSchemaStr());

    MultiSchemaResponse valueSchemaResponse = (MultiSchemaResponse)controllerRequestWithRetry(
        () -> this.controllerClient.getAllValueSchema(this.storeName)
    );
    LOGGER.info("Got [store: " + this.storeName + "] SchemaResponse for value schemas: " + valueSchemaResponse);
    for (MultiSchemaResponse.Schema valueSchema : valueSchemaResponse.getSchemas()) {
      valueSchemaIds.put(Schema.parse(valueSchema.getSchemaStr()), valueSchema.getId());
    }
  }

  protected ControllerResponse controllerRequestWithRetry(Supplier<ControllerResponse> supplier) {
    String errorMsg = "";
    for (int currentAttempt = 0; currentAttempt < 60; currentAttempt++) {
      ControllerResponse controllerResponse = supplier.get();
      if (!controllerResponse.isError()) {
        return controllerResponse;
      } else {
        try {
          time.sleep(1000);
        } catch (InterruptedException e) {
          throw new VeniceException(e);
        }
        errorMsg = controllerResponse.getError();
      }
    }
    throw new SamzaException("Failed to send request to Controller, error: " + errorMsg);
  }

  protected VeniceWriter<byte[], byte[]> getVeniceWriter(VersionCreationResponse store) {
    Properties veniceWriterProperties = new Properties();
    veniceWriterProperties.put(KAFKA_BOOTSTRAP_SERVERS, store.getKafkaBootstrapServers());
    return getVeniceWriter(store, veniceWriterProperties);
  }

  protected VeniceWriter<byte[], byte[]> getVeniceWriter(VersionCreationResponse store, Properties veniceWriterProperties) {
    return new VeniceWriterFactory(veniceWriterProperties).getBasicVeniceWriter(store.getKafkaTopic(), time);
  }

  @Override
  public void start() {

  }

  @Override
  public void stop() {
    veniceWriter.close();
    controllerClient.close();
    D2ClientUtils.shutdownClient(d2Client);
  }

  @Override
  public void register(String source) {

  }

  @Override
  public void send(String source, OutgoingMessageEnvelope outgoingMessageEnvelope) {
    String storeOfIncomingMessage = outgoingMessageEnvelope.getSystemStream().getStream();
    if (! storeOfIncomingMessage.equals(storeName)) {
      throw new SamzaException("The store of the incoming message: " + storeOfIncomingMessage +
          " is unexpected, and it should be " + storeName);
    }

    send(outgoingMessageEnvelope.getKey(), outgoingMessageEnvelope.getMessage());
  }

  protected CompletableFuture<Void> send(Object keyObject, Object valueObject) {

    String topic = versionCreationResponse.getKafkaTopic();
    Schema keyObjectSchema = getSchemaFromObject(keyObject);

    if (!keySchema.equals(keyObjectSchema)) {
      String serializedObject;
      if (keyObject instanceof byte[]){
        serializedObject = new String((byte[]) keyObject);
      } else if (keyObject instanceof ByteBuffer){
        serializedObject = new String(((ByteBuffer) keyObject).array());
      } else {
        serializedObject = keyObject.toString();
      }
      throw new SamzaException("Cannot write record to Venice store " + storeName + ", key object has schema " + keyObjectSchema
          + " which does not match Venice key schema " + keySchema + ".  Key object: " + serializedObject);
    }

    byte[] key = serializeObject(topic, keyObject);
    final CompletableFuture<Void> completableFuture = new CompletableFuture<>();
    final Callback callback = new VeniceWriter.CompletableFutureCallback(
        completableFuture);

    if (null == valueObject) {
      veniceWriter.delete(key, callback);
    } else {
      Schema valueObjectSchema = getSchemaFromObject(valueObject);
      int valueSchemaId = valueSchemaIds.computeIfAbsent(valueObjectSchema, valueSchema -> {
        SchemaResponse valueSchemaResponse = (SchemaResponse)controllerRequestWithRetry(
            () -> controllerClient.getValueSchemaID(storeName, valueSchema.toString())
        );
        LOGGER.info("Got [store: " + this.storeName + "] SchemaResponse for schema: " + valueSchema);
        return valueSchemaResponse.getId();
      });
      byte[] value = serializeObject(topic, valueObject);

      veniceWriter.put(key, value, valueSchemaId, callback);
    }
    return completableFuture;
  }

  public CompletableFuture<Void> put(Object keyObject, Object valueObject) {
    return send(keyObject, valueObject);
  }

  public CompletableFuture<Void> delete(Object keyObject) {
    return send(keyObject, null);
  }

  /**
   * Flushing the data to Venice store in case VeniceSystemProducer buffers message.
   *
   * @param s String representing the source of the message. Currently VeniceSystemProducer is not using this param.
   */
  @Override
  public void flush(String s) {
    veniceWriter.flush();
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
      VeniceAvroSerializer serializer = serializers.computeIfAbsent(
          ((IndexedRecord) input).getSchema().toString(), VeniceAvroSerializer::new);
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
    BinaryEncoder encoder = LinkedinAvroMigrationHelper.newBinaryEncoder(out);
    try {
      writer.write(input, encoder);
      encoder.flush();
    } catch (IOException e) {
      throw new RuntimeException("Failed to write intput: " + input + " to binary encoder", e);
    }
    return out.toByteArray();
  }
}
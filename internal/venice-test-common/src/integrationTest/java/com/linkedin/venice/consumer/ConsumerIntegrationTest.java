package com.linkedin.venice.consumer;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerAdapter;
import com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig;
import com.linkedin.venice.serialization.DefaultSerializer;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.specific.SpecificRecord;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public abstract class ConsumerIntegrationTest {
  private static final String TEST_KEY = "key1";
  private static final String NEW_FIELD_NAME = "newField";

  /**
   * There could be cases where there exists an unreleased schema which conflicts with this new protocol version,
   * but that should be supported.
   */
  public static final int NEW_PROTOCOL_VERSION =
      AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.currentProtocolVersion.get() + 1;

  public static final Schema NEW_PROTOCOL_SCHEMA;

  static {
    // TODO: Consider refactoring this so that it is not static, in order to test other evolution scenarios, such as:
    // - Adding a field in a nested record (should be fine...).
    // - Adding new types to a union (not supported gracefully at the moment...).

    // General info about the current protocol
    AvroProtocolDefinition protocolDef = AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE;
    Schema currentProtocolSchema = protocolDef.getCurrentProtocolVersionSchema();
    List<Schema.Field> protocolSchemaFields = currentProtocolSchema.getFields()
        .stream()
        .map(field -> AvroCompatibilityHelper.newField(field).build())
        .collect(Collectors.toList());

    // Generation of a new protocol version by adding an optional field to the current protocol version
    Schema newFieldSchema =
        Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT)));
    protocolSchemaFields.add(
        0,
        AvroCompatibilityHelper.newField(null)
            .setName(NEW_FIELD_NAME)
            .setSchema(newFieldSchema)
            .setOrder(Schema.Field.Order.ASCENDING)
            .setDefault(null)
            .build());
    NEW_PROTOCOL_SCHEMA = Schema.createRecord(
        currentProtocolSchema.getName(),
        currentProtocolSchema.getDoc(),
        currentProtocolSchema.getNamespace(),
        false);
    NEW_PROTOCOL_SCHEMA.setFields(protocolSchemaFields);
  }

  VeniceClusterWrapper cluster;
  String store;
  int version;
  AvroGenericStoreClient client;
  private ControllerClient controllerClient;
  private String topicName;

  @BeforeClass
  public void sharedSetUp() {
    cluster = ServiceFactory.getVeniceCluster();
    controllerClient =
        ControllerClient.constructClusterControllerClient(cluster.getClusterName(), cluster.getAllControllersURLs());

    extraBeforeClassSetUp(cluster, controllerClient);
  }

  void extraBeforeClassSetUp(VeniceClusterWrapper cluster, ControllerClient controllerClient) {
  }

  @BeforeMethod
  public void testSetUp() {
    store = Utils.getUniqueString("consumer_integ_test");
    version = 1;
    cluster.getNewStore(store);
    long streamingRewindSeconds = 25L;
    long streamingMessageLag = 2L;
    controllerClient.updateStore(
        store,
        new UpdateStoreQueryParams().setHybridRewindSeconds(streamingRewindSeconds)
            .setHybridOffsetLagThreshold(streamingMessageLag));
    topicName = Utils.getRealTimeTopicName(
        cluster.getLeaderVeniceController().getVeniceAdmin().getStore(cluster.getClusterName(), store));
    controllerClient.emptyPush(store, "test_push", 1);
    TestUtils.waitForNonDeterministicAssertion(15, TimeUnit.SECONDS, () -> {
      StoreResponse freshStoreResponse = controllerClient.getStore(store);
      Assert.assertFalse(freshStoreResponse.isError());
      Assert.assertEquals(
          freshStoreResponse.getStore().getCurrentVersion(),
          version,
          "The empty push has not activated the store.");
    });
    client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(store).setVeniceURL(cluster.getRandomRouterURL()));
  }

  @AfterMethod
  public void testCleanUp() {
    Utils.closeQuietlyWithErrorLogged(client);
  }

  @AfterClass
  public void sharedCleanUp() {
    Utils.closeQuietlyWithErrorLogged(cluster);
    Utils.closeQuietlyWithErrorLogged(controllerClient);
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testForwardCompatibility() throws ExecutionException, InterruptedException {
    // Verify that the regular writer can update the store
    try (VeniceWriter<String, String, byte[]> regularVeniceWriter = cluster.getVeniceWriter(topicName)) {
      writeAndVerifyRecord(regularVeniceWriter, client, "value1");
    }

    try (VeniceWriter<String, String, byte[]> veniceWriterWithNewerProtocol =
        getVeniceWriterWithNewerProtocol(getOverrideProtocolSchema(), topicName)) {
      writeAndVerifyRecord(veniceWriterWithNewerProtocol, client, "value2");
    }
  }

  VeniceWriterWithNewerProtocol getVeniceWriterWithNewerProtocol(Schema overrideProtocolSchema, String topicName) {
    Properties javaProps = new Properties();
    javaProps
        .put(ApacheKafkaProducerConfig.KAFKA_VALUE_SERIALIZER, KafkaValueSerializerWithNewerProtocol.class.getName());
    javaProps.put(ApacheKafkaProducerConfig.KAFKA_BOOTSTRAP_SERVERS, cluster.getPubSubBrokerWrapper().getAddress());
    VeniceProperties props = new VeniceProperties(javaProps);
    String stringSchema = "\"string\"";
    VeniceKafkaSerializer keySerializer = new VeniceAvroKafkaSerializer(stringSchema);
    VeniceKafkaSerializer valueSerializer = new VeniceAvroKafkaSerializer(stringSchema);
    VenicePartitioner partitioner = new DefaultVenicePartitioner(props);
    Time time = new SystemTime();

    VeniceWriterOptions veniceWriterOptions = new VeniceWriterOptions.Builder(topicName).setKeySerializer(keySerializer)
        .setValueSerializer(valueSerializer)
        .setWriteComputeSerializer(new DefaultSerializer())
        .setTime(time)
        .setPartitioner(partitioner)
        .build();
    return new VeniceWriterWithNewerProtocol(
        veniceWriterOptions,
        props,
        new ApacheKafkaProducerWithNewerProtocolAdapter(props),
        overrideProtocolSchema);
  }

  abstract Schema getOverrideProtocolSchema();

  private void writeAndVerifyRecord(
      VeniceWriter<String, String, byte[]> veniceWriter,
      AvroGenericStoreClient client,
      String testValue) throws ExecutionException, InterruptedException {
    veniceWriter.put(TEST_KEY, testValue, 1).get();
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      try {
        Object value = client.get(TEST_KEY).get();
        Assert.assertNotNull(
            value,
            "The key written by the " + veniceWriter.getClass().getSimpleName() + " is not in the store yet.");
        Assert.assertEquals(
            value.toString(),
            testValue,
            "The key written by the " + veniceWriter.getClass().getSimpleName() + " is not valid.");
      } catch (ExecutionException e) {
        Assert.fail("Caught exception: " + e.getMessage());
      }

    });
  }

  /**
   * A class which looks like a {@link KafkaMessageEnvelope} but which is more than that...
   *
   * A bit tricky, but we need to do these acrobatics in order to:
   *
   * 1. Keep using specific records inside the {@link VeniceWriter}
   * 2. Translate the position of fields between old and new KMEs.
   */
  static class NewKafkaMessageEnvelopeWithExtraField extends KafkaMessageEnvelope {
    public static final org.apache.avro.Schema SCHEMA$ = NEW_PROTOCOL_SCHEMA;
    private static final int newFieldIndex = getNewFieldIndex();
    public Integer newField;

    public NewKafkaMessageEnvelopeWithExtraField() {
    }

    @Override
    public org.apache.avro.Schema getSchema() {
      return SCHEMA$;
    }

    // Used by DatumWriter. Applications should not call.
    @Override
    public java.lang.Object get(int field$) {
      if (newFieldIndex == field$) {
        return newField;
      } else {
        Schema.Field newField = getSchema().getFields().get(field$);
        Schema.Field oldField = super.getSchema().getField(newField.name());
        if (oldField == null) {
          throw new IllegalStateException();
        }
        return super.get(oldField.pos());
      }
    }

    // Used by DatumReader. Applications should not call.
    @SuppressWarnings(value = "unchecked")
    @Override
    public void put(int field$, java.lang.Object value$) {
      if (newFieldIndex == field$) {
        this.newField = (java.lang.Integer) value$;
      } else {
        Schema.Field newField = getSchema().getFields().get(field$);
        Schema.Field oldField = super.getSchema().getField(newField.name());
        if (oldField == null) {
          throw new IllegalStateException();
        }
        super.put(oldField.pos(), value$);
      }
    }

    private static int getNewFieldIndex() {
      for (Schema.Field field: SCHEMA$.getFields()) {
        if (field.name().equals(NEW_FIELD_NAME)) {
          return field.pos();
        }
      }
      throw new IllegalStateException("Missing a field called '" + NEW_FIELD_NAME + "' in the schema!");
    }
  }

  private static class ApacheKafkaProducerWithNewerProtocolAdapter extends ApacheKafkaProducerAdapter {
    public ApacheKafkaProducerWithNewerProtocolAdapter(VeniceProperties props) {
      super(new ApacheKafkaProducerConfig(props, null, null, false));
    }
  }

  public static class KafkaValueSerializerWithNewerProtocol extends KafkaValueSerializer {
    @Override
    public byte[] serialize(String topic, KafkaMessageEnvelope object) {
      return serializeNewProtocol(object);
    }

    @Override
    public Schema getCompiledProtocol() {
      return NEW_PROTOCOL_SCHEMA;
    }
  }

  /**
   * Code partially copied from {@link InternalAvroSpecificSerializer#serialize(String, SpecificRecord)} because
   * that class has so many safeguards that it would be hard to mock it in such way that we can inject new schemas
   * that are not truly part of the project.
   */
  public static byte[] serializeNewProtocol(GenericRecord messageFromNewProtocol) {
    try {
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      Encoder encoder = AvroCompatibilityHelper.newBinaryEncoder(byteArrayOutputStream);

      byteArrayOutputStream.write(AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getMagicByte().get());
      byteArrayOutputStream.write((byte) NEW_PROTOCOL_VERSION);
      GenericDatumWriter writer = new GenericDatumWriter(messageFromNewProtocol.getSchema());
      writer.write(messageFromNewProtocol, encoder);
      encoder.flush();
      return byteArrayOutputStream.toByteArray();
    } catch (IOException e) {
      throw new VeniceMessageException("Failed to encode message: " + messageFromNewProtocol.toString(), e);
    }
  }
}

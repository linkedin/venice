package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.hadoop.VenicePushJob.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.hadoop.VenicePushJob.KAFKA_INPUT_MAX_RECORDS_PER_MAPPER;
import static com.linkedin.venice.hadoop.VenicePushJob.KAFKA_INPUT_TOPIC;
import static com.linkedin.venice.hadoop.VenicePushJob.KEY_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.SOURCE_KAFKA;
import static com.linkedin.venice.hadoop.VenicePushJob.VALUE_FIELD_PROP;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJPropsWithoutD2Routing;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestWriteUtils.writeComplexVsonFile;
import static com.linkedin.venice.utils.TestWriteUtils.writeMultiLevelVsonFile;
import static com.linkedin.venice.utils.TestWriteUtils.writeMultiLevelVsonFile2;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleVsonFile;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleVsonFileWithUserSchema;
import static com.linkedin.venice.utils.TestWriteUtils.writeVsonByteAndShort;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.schema.vson.VsonAvroSchemaAdapter;
import com.linkedin.venice.schema.vson.VsonSchema;
import com.linkedin.venice.utils.KeyAndValueSchemas;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestVsonStoreBatch {
  private static final int TEST_TIMEOUT = 60 * Time.MS_PER_SECOND;

  private VeniceClusterWrapper veniceCluster;
  private ControllerClient controllerClient;

  @BeforeClass
  public void setUp() {
    veniceCluster = ServiceFactory.getVeniceCluster();
    controllerClient = new ControllerClient(
        veniceCluster.getClusterName(),
        veniceCluster.getLeaderVeniceController().getControllerUrl());
  }

  @AfterClass
  public void cleanUp() {
    IOUtils.closeQuietly(controllerClient);
    IOUtils.closeQuietly(veniceCluster);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testVsonStoreWithSimpleRecords() throws Exception {
    testBatchStore(inputDir -> writeSimpleVsonFile(inputDir), props -> {
      props.setProperty(KEY_FIELD_PROP, "");
      props.setProperty(VALUE_FIELD_PROP, "");
    }, (avroClient, vsonClient, metricsRepository) -> {
      for (int i = 0; i < 100; i++) {
        // we need to explicitly call toString() because avro actually returns Utf8
        Assert.assertEquals(avroClient.get(i).get().toString(), String.valueOf(i + 100));
        Assert.assertEquals(vsonClient.get(i).get(), String.valueOf(i + 100));
      }
    });
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testVsonStoreWithMissingKeyField() throws Exception {
    testBatchStore(inputDir -> writeSimpleVsonFile(inputDir), props -> {
      props.remove(KEY_FIELD_PROP);
      props.setProperty(VALUE_FIELD_PROP, "");
    }, (avroClient, vsonClient, metricsRepository) -> {
      for (int i = 0; i < 100; i++) {
        // we need to explicitly call toString() because avro actually returns Utf8
        Assert.assertEquals(avroClient.get(i).get().toString(), String.valueOf(i + 100));
        Assert.assertEquals(vsonClient.get(i).get(), String.valueOf(i + 100));
      }
    });
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testVsonStoreWithMissingValueField() throws Exception {
    testBatchStore(inputDir -> writeSimpleVsonFile(inputDir), props -> {
      props.setProperty(KEY_FIELD_PROP, "");
      props.remove(VALUE_FIELD_PROP);
    }, (avroClient, vsonClient, metricsRepository) -> {
      for (int i = 0; i < 100; i++) {
        // we need to explicitly call toString() because avro actually returns Utf8
        Assert.assertEquals(avroClient.get(i).get().toString(), String.valueOf(i + 100));
        Assert.assertEquals(vsonClient.get(i).get(), String.valueOf(i + 100));
      }
    });
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testVsonStoreWithMissingKeyAndValueFields() throws Exception {
    testBatchStore(inputDir -> writeSimpleVsonFile(inputDir), props -> {
      props.remove(KEY_FIELD_PROP);
      props.remove(VALUE_FIELD_PROP);
    }, (avroClient, vsonClient, metricsRepository) -> {
      for (int i = 0; i < 100; i++) {
        // we need to explicitly call toString() because avro actually returns Utf8
        Assert.assertEquals(avroClient.get(i).get().toString(), String.valueOf(i + 100));
        Assert.assertEquals(vsonClient.get(i).get(), String.valueOf(i + 100));
      }
    });
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testVsonStoreWithComplexRecords() throws Exception {
    testBatchStore(inputDir -> writeComplexVsonFile(inputDir), props -> {
      props.setProperty(KEY_FIELD_PROP, "");
      props.setProperty(VALUE_FIELD_PROP, "");
    }, (avroClient, vsonClient, metricsRepository) -> {
      for (int i = 0; i < 100; i++) {
        GenericData.Record avroObject = (GenericData.Record) avroClient.get(i).get();
        Map vsonObject = (Map) vsonClient.get(i).get();

        Assert.assertEquals(avroObject.get("member_id"), i + 100);
        Assert.assertEquals(vsonObject.get("member_id"), i + 100);

        // we are expecting the receive null field if i % 10 == 0
        Assert.assertEquals(avroObject.get("score"), i % 10 != 0 ? (float) i : null);
        Assert.assertEquals(vsonObject.get("score"), i % 10 != 0 ? (float) i : null);
      }
    });
  }

  /**
   * single byte (int8) and short (int16) are represented as Fixed in Avro.
   * This test case make sure Venice can write and read them properly.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testVsonStoreCanProcessByteAndShort() throws Exception {
    testBatchStore(inputDir -> writeVsonByteAndShort(inputDir), props -> {
      props.setProperty(KEY_FIELD_PROP, "");
      props.setProperty(VALUE_FIELD_PROP, "");
    }, (avroClient, vsonClient, metricsRepository) -> {
      for (int i = 0; i < 100; i++) {
        Assert.assertEquals(vsonClient.get((byte) i).get(), (short) (i - 50));
      }
    });
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testVsonStoreMultiLevelRecordsSchema() throws Exception {
    testBatchStore(inputDir -> writeMultiLevelVsonFile(inputDir), props -> {
      props.setProperty(KEY_FIELD_PROP, "");
      props.setProperty(VALUE_FIELD_PROP, "");
    }, (avroClient, vsonClient, metricsRepository) -> {
      for (int i = 0; i < 100; i++) {
        GenericRecord record1 = (GenericRecord) ((GenericRecord) (avroClient.get(i).get())).get("level1");
        GenericRecord record21 = (GenericRecord) record1.get("level21");
        Assert.assertEquals(record21.get("field1"), i + 100);

        HashMap<String, Object> map = (HashMap<String, Object>) vsonClient.get(i).get();
        HashMap<String, Object> map1 = (HashMap<String, Object>) map.get("level1");
        HashMap<String, Object> map21 = (HashMap<String, Object>) map1.get("level21");
        Assert.assertEquals(map21.get("field1"), i + 100);
      }
    });
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testVsonStoreWithSelectedField() throws Exception {
    testBatchStore(inputDir -> {
      KeyAndValueSchemas schemas = writeComplexVsonFile(inputDir);
      // strip the value schema since this is selected filed
      Schema selectedValueSchema = VsonAvroSchemaAdapter.stripFromUnion(schemas.getValue()).getField("score").schema();
      return new KeyAndValueSchemas(schemas.getKey(), selectedValueSchema);
    }, props -> {
      props.setProperty(KEY_FIELD_PROP, "");
      props.setProperty(VALUE_FIELD_PROP, "score");
    }, (avroClient, vsonClient, metricsRepository) -> {
      for (int i = 0; i < 100; i++) {
        Assert.assertEquals(avroClient.get(i).get(), i % 10 != 0 ? (float) i : null);
        Assert.assertEquals(vsonClient.get(i).get(), i % 10 != 0 ? (float) i : null);
      }
    });
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testVsonStoreMultiLevelRecordsSchemaWithSelectedField() throws Exception {
    testBatchStore(inputDir -> {
      Pair<VsonSchema, VsonSchema> schemas = writeMultiLevelVsonFile2(inputDir);
      Schema keySchema = VsonAvroSchemaAdapter.parse(schemas.getFirst().toString());
      Schema selectedValueSchema = VsonAvroSchemaAdapter.parse(schemas.getSecond().recordSubtype("recs").toString());
      return new KeyAndValueSchemas(keySchema, selectedValueSchema);
    }, props -> {
      props.setProperty(KEY_FIELD_PROP, "");
      props.setProperty(VALUE_FIELD_PROP, "recs");
    }, (avroClient, vsonClient, metricsRepository) -> {
      for (int i = 0; i < 100; i++) {
        GenericRecord valueInnerRecord = (GenericRecord) ((List) avroClient.get(i).get()).get(0);
        Assert.assertEquals(valueInnerRecord.get("member_id"), i);
        Assert.assertEquals(valueInnerRecord.get("score"), (float) i);

        HashMap<String, Object> vsonValueInnerMap = (HashMap<String, Object>) ((List) vsonClient.get(i).get()).get(0);
        Assert.assertEquals(vsonValueInnerMap.get("member_id"), i);
        Assert.assertEquals(vsonValueInnerMap.get("score"), (float) i);
      }
    });
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testKafkaInputBatchJobWithVsonStoreMultiLevelRecordsSchemaWithSelectedField() throws Exception {
    TestBatch.VPJValidator validator = (avroClient, vsonClient, metricsRepository) -> {
      for (int i = 0; i < 100; i++) {
        GenericRecord valueInnerRecord = (GenericRecord) ((List) avroClient.get(i).get()).get(0);
        Assert.assertEquals(valueInnerRecord.get("member_id"), i);
        Assert.assertEquals(valueInnerRecord.get("score"), (float) i);

        HashMap<String, Object> vsonValueInnerMap = (HashMap<String, Object>) ((List) vsonClient.get(i).get()).get(0);
        Assert.assertEquals(vsonValueInnerMap.get("member_id"), i);
        Assert.assertEquals(vsonValueInnerMap.get("score"), (float) i);
      }
    };
    String storeName = testBatchStore(inputDir -> {
      Pair<VsonSchema, VsonSchema> schemas = writeMultiLevelVsonFile2(inputDir);
      Schema keySchema = VsonAvroSchemaAdapter.parse(schemas.getFirst().toString());
      Schema selectedValueSchema = VsonAvroSchemaAdapter.parse(schemas.getSecond().recordSubtype("recs").toString());
      return new KeyAndValueSchemas(keySchema, selectedValueSchema);
    }, props -> {
      props.setProperty(KEY_FIELD_PROP, "");
      props.setProperty(VALUE_FIELD_PROP, "recs");
    }, validator, new UpdateStoreQueryParams(), Optional.empty(), false);
    // Re-push with Kafka Input
    testBatchStore(
        inputDir -> new KeyAndValueSchemas(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.NULL)),
        properties -> {
          properties.setProperty(SOURCE_KAFKA, "true");
          properties.setProperty(KAFKA_INPUT_TOPIC, Version.composeKafkaTopic(storeName, 1));
          properties.setProperty(KAFKA_INPUT_BROKER_URL, veniceCluster.getKafka().getAddress());
          properties.setProperty(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5");
        },
        validator,
        new UpdateStoreQueryParams(),
        Optional.of(storeName),
        true);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testZstdCompressingVsonRecord() throws Exception {
    testBatchStore(inputDir -> {
      Pair<Schema, Schema> schemas = writeSimpleVsonFileWithUserSchema(inputDir);
      Schema selectedValueSchema = VsonAvroSchemaAdapter.stripFromUnion(schemas.getSecond()).getField("name").schema();
      return new KeyAndValueSchemas(schemas.getFirst(), selectedValueSchema);
    }, properties -> {
      properties.setProperty(VenicePushJob.KEY_FIELD_PROP, "");
      properties.setProperty(VenicePushJob.VALUE_FIELD_PROP, "name");
    },
        TestBatch.getSimpleFileWithUserSchemaValidatorForZstd(),
        new UpdateStoreQueryParams().setCompressionStrategy(CompressionStrategy.ZSTD_WITH_DICT));
  }

  private String testBatchStore(
      TestBatch.InputFileWriter inputFileWriter,
      Consumer<Properties> extraProps,
      TestBatch.VPJValidator dataValidator) throws Exception {
    return testBatchStore(inputFileWriter, extraProps, dataValidator, new UpdateStoreQueryParams());
  }

  private String testBatchStore(
      TestBatch.InputFileWriter inputFileWriter,
      Consumer<Properties> extraProps,
      TestBatch.VPJValidator dataValidator,
      UpdateStoreQueryParams storeParms) throws Exception {
    return testBatchStore(inputFileWriter, extraProps, dataValidator, storeParms, Optional.empty(), true);
  }

  private String testBatchStore(
      TestBatch.InputFileWriter inputFileWriter,
      Consumer<Properties> extraProps,
      TestBatch.VPJValidator dataValidator,
      UpdateStoreQueryParams storeParms,
      Optional<String> storeNameOptional,
      boolean deleteStoreAfterValidation) throws Exception {
    File inputDir = getTempDataDirectory();
    KeyAndValueSchemas schemas = inputFileWriter.write(inputDir);
    String storeName = storeNameOptional.isPresent() ? storeNameOptional.get() : Utils.getUniqueString("store");
    AvroGenericStoreClient avroClient = null;
    AvroGenericStoreClient vsonClient = null;

    try {
      String inputDirPath = "file://" + inputDir.getAbsolutePath();
      Properties props = defaultVPJPropsWithoutD2Routing(veniceCluster, inputDirPath, storeName);
      extraProps.accept(props);

      if (!storeNameOptional.isPresent()) {
        /**
         * If the caller passes an non-empty store name, we will assume the store already exists.
         * If this assumption is not valid for some test cases, the logic needs to be changed here.
         */
        createStoreForJob(
            veniceCluster.getClusterName(),
            schemas.getKey().toString(),
            schemas.getValue().toString(),
            props,
            storeParms).close();
      }

      TestWriteUtils.runPushJob("Test Batch push job", props);
      MetricsRepository metricsRepository = new MetricsRepository();
      avroClient = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName)
              .setVeniceURL(veniceCluster.getRandomRouterURL())
              .setMetricsRepository(metricsRepository) // metrics only available for Avro client...
      );
      vsonClient = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultVsonGenericClientConfig(storeName).setVeniceURL(veniceCluster.getRandomRouterURL()));
      veniceCluster.refreshAllRouterMetaData();
      dataValidator.validate(avroClient, vsonClient, metricsRepository);
    } finally {
      IOUtils.closeQuietly(avroClient);
      IOUtils.closeQuietly(vsonClient);
      if (deleteStoreAfterValidation) {
        controllerClient.enableStoreReadWrites(storeName, false);
        controllerClient.deleteStore(storeName);
      }
    }
    return storeName;
  }
}

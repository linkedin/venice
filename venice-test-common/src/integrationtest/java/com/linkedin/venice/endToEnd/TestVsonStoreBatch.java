package com.linkedin.venice.endToEnd;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.hadoop.KafkaPushJob;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.schema.vson.VsonAvroSchemaAdapter;
import com.linkedin.venice.schema.vson.VsonSchema;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.venice.hadoop.KafkaPushJob.KEY_FIELD_PROP;
import static com.linkedin.venice.hadoop.KafkaPushJob.VALUE_FIELD_PROP;
import static com.linkedin.venice.utils.TestPushUtils.*;
import static com.linkedin.venice.utils.TestPushUtils.createStoreForJob;


public class TestVsonStoreBatch {
  private static final int TEST_TIMEOUT = 60 * Time.MS_PER_SECOND;

  private VeniceClusterWrapper veniceCluster;
  private ControllerClient controllerClient;

  @BeforeClass
  public void setup() {
    veniceCluster = ServiceFactory.getVeniceCluster();
    controllerClient = new ControllerClient(veniceCluster.getClusterName(),
        veniceCluster.getMasterVeniceController().getControllerUrl());
  }

  @AfterClass
  public void cleanup() {
    if (veniceCluster != null) {
      veniceCluster.close();
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testVsonStoreWithSimpleRecords() throws Exception {
    testBatchStore(inputDir -> writeSimpleVsonFile(inputDir), props -> {
      props.setProperty(KEY_FIELD_PROP, "");
      props.setProperty(VALUE_FIELD_PROP, "");
    }, (avroClient, vsonClient, metricsRepository) -> {
      for (int i = 0; i < 100; i++) {
        //we need to explicitly call toString() because avro actually returns Utf8
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
      for (int i = 0; i < 100; i ++) {
        GenericData.Record avroObject = (GenericData.Record) avroClient.get(i).get();
        Map vsonObject = (Map) vsonClient.get(i).get();

        Assert.assertEquals(avroObject.get("member_id"), i + 100);
        Assert.assertEquals(vsonObject.get("member_id"), i + 100);

        //we are expecting the receive null field if i % 10 == 0
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
      for (int i = 0; i < 100; i ++) {
        Assert.assertEquals(vsonClient.get((byte) i).get(), (short) (i - 50));
      }
    });
  }


  @Test(timeOut = TEST_TIMEOUT)
  public void testVsonStoreMultiLevelRecordsSchema() throws Exception {
    testBatchStore(inputDir -> {
      Pair<Schema, Schema> schemas = writeMultiLevelVsonFile(inputDir);
      return new Pair<>(schemas.getFirst(), schemas.getSecond());
    }, props -> {
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
      Pair<Schema, Schema> schemas = writeComplexVsonFile(inputDir);
      //strip the value schema since this is selected filed
      Schema selectedValueSchema = VsonAvroSchemaAdapter.stripFromUnion(schemas.getSecond()).getField("score").schema();
      return new Pair<>(schemas.getFirst(), selectedValueSchema);
    }, props -> {
      props.setProperty(KEY_FIELD_PROP, "");
      props.setProperty(VALUE_FIELD_PROP, "score");
    }, (avroClient, vsonClient, metricsRepository) -> {
      for (int i = 0; i < 100; i ++) {
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
      return new Pair<>( keySchema, selectedValueSchema);
    }, props -> {
      props.setProperty(KEY_FIELD_PROP, "");
      props.setProperty(VALUE_FIELD_PROP, "recs");
    }, (avroClient, vsonClient, metricsRepository) -> {
      for (int i = 0; i < 100; i ++) {
        GenericRecord valueInnerRecord = (GenericRecord) ((List) avroClient.get(i).get()).get(0);
        Assert.assertEquals(valueInnerRecord.get("member_id"), i);
        Assert.assertEquals(valueInnerRecord.get("score"), (float) i);

        HashMap<String, Object> vsonValueInnerMap = (HashMap<String, Object>) ((List) vsonClient.get(i).get()).get(0);
        Assert.assertEquals(vsonValueInnerMap.get("member_id"), i);
        Assert.assertEquals(vsonValueInnerMap.get("score"), (float) i);
      }
    });
  }

  private void testBatchStore(TestBatch.InputFileWriter inputFileWriter, Consumer<Properties> extraProps, TestBatch.H2VValidator dataValidator) throws Exception {
    File inputDir = getTempDataDirectory();
    Pair<Schema, Schema> schemas = inputFileWriter.write(inputDir);
    String storeName = TestUtils.getUniqueString("store");
    AvroGenericStoreClient avroClient = null;
    AvroGenericStoreClient vsonClient = null;

    try {
      String inputDirPath = "file://" + inputDir.getAbsolutePath();
      Properties props = defaultH2VProps(veniceCluster, inputDirPath, storeName);
      extraProps.accept(props);

      createStoreForJob(veniceCluster, schemas.getFirst().toString(), schemas.getSecond().toString(), props, false,
          false);

      KafkaPushJob job = new KafkaPushJob("Test Batch push job", props);
      job.run();
      MetricsRepository metricsRepository = new MetricsRepository();
      avroClient = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName)
              .setVeniceURL(veniceCluster.getRandomRouterURL())
              .setMetricsRepository(metricsRepository) // metrics only available for Avro client...
      );
      vsonClient = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultVsonGenericClientConfig(storeName)
              .setVeniceURL(veniceCluster.getRandomRouterURL())
      );

      String controllerUrl = veniceCluster.getAllControllersURLs();

      TestUtils.waitForNonDeterministicCompletion(TEST_TIMEOUT/2, TimeUnit.SECONDS, () -> {
        int currentVersion = ControllerClient.getStore(controllerUrl, veniceCluster.getClusterName(), storeName).getStore().getCurrentVersion();
        // Refresh router metadata once new version (1 since there is just one push) is pushed, so that the router sees the latest store version.
        if (currentVersion == 1) {
          veniceCluster.refreshAllRouterMetaData();
        }
        return currentVersion == 1;
      });
      dataValidator.validate(avroClient, vsonClient, metricsRepository);
    } finally {
      IOUtils.closeQuietly(avroClient);
      IOUtils.closeQuietly(vsonClient);
      controllerClient.enableStoreReadWrites(storeName, false);
      controllerClient.deleteStore(storeName);
    }
  }

}

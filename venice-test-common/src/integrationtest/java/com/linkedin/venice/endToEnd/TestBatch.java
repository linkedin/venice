package com.linkedin.venice.endToEnd;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.KafkaPushJob;
import com.linkedin.venice.hadoop.pbnj.Sampler;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.schema.vson.VsonAvroSchemaAdapter;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.util.*;

import java.util.function.Consumer;
import javafx.util.Pair;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.*;

import static com.linkedin.venice.hadoop.KafkaPushJob.*;
import static com.linkedin.venice.utils.TestPushUtils.*; // TODO: remove this static import.

//TODO: write a H2VWrapper that can handle the whole flow

public class TestBatch {
  private static final Logger LOGGER = Logger.getLogger(TestBatch.class);
  private static final int TEST_TIMEOUT = 60 * Time.MS_PER_SECOND;
  private static final String STRING_SCHEMA = "\"string\"";

  private VeniceClusterWrapper veniceCluster;

  @BeforeClass
  public void setup() {
    veniceCluster = ServiceFactory.getVeniceCluster();
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
        }, (avroClient, vsonClient) -> {
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
    }, (avroClient, vsonClient) -> {
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
    }, (avroClient, vsonClient) -> {
      for (int i = 0; i < 100; i ++) {
        Assert.assertEquals(vsonClient.get((byte) i).get(), (short) (i - 50));
      }
    });
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testVsonStoreWithSelectedField() throws Exception {
    testBatchStore(inputDir -> {
      Pair<Schema, Schema> schemas = writeComplexVsonFile(inputDir);
      //strip the value schema since this is selected filed
      Schema selectedValueSchema = VsonAvroSchemaAdapter.stripFromUnion(schemas.getValue()).getField("score").schema();
      return new Pair<>(schemas.getKey(), selectedValueSchema);
    }, props -> {
      props.setProperty(KEY_FIELD_PROP, "");
      props.setProperty(VALUE_FIELD_PROP, "score");
    }, (avroClient, vsonClient) -> {
      for (int i = 0; i < 100; i ++) {
        Assert.assertEquals(avroClient.get(i).get(), i % 10 != 0 ? (float) i : null);
        Assert.assertEquals(vsonClient.get(i).get(), i % 10 != 0 ? (float) i : null);
      }
    });
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testDuplicateKey() throws Exception {
    try {
      testStoreWithDuplicateKeys(false);
      Assert.fail();
    } catch (VeniceException e) {
      //push is expected to fail because of duplicate key
    }

    testStoreWithDuplicateKeys(true);
  }

  private void testStoreWithDuplicateKeys(boolean isDuplicateKeyAllowed) throws Exception {
    testBatchStore(inputDir -> {
      Schema recordSchema = writeSimpleAvroFileWithDuplicateKey(inputDir);
      return new Pair<>(recordSchema.getField("id").schema(),
                        recordSchema.getField("name").schema());
        }, props -> {
          if (isDuplicateKeyAllowed) {
            props.setProperty(ALLOW_DUPLICATE_KEY, "true");
          }
        },
        (avroClient, vsonClient) -> {});
  }

  private void testBatchStore(InputFileWriter inputFileWriter, Consumer<Properties> extraProps, H2VValidator dataValidator) throws Exception {
    File inputDir = getTempDataDirectory();
    Pair<Schema, Schema> schemas = inputFileWriter.write(inputDir);
    String storeName = TestUtils.getUniqueString("store");

    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties props = defaultH2VProps(veniceCluster, inputDirPath, storeName);
    extraProps.accept(props);

    createStoreForJob(veniceCluster, schemas.getKey().toString(), schemas.getValue().toString(), props);

    KafkaPushJob job = new KafkaPushJob("Test Batch push job", props);
    job.run();

    AvroGenericStoreClient avroClient = ClientFactory.getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName)
        .setVeniceURL(veniceCluster.getRandomRouterURL()));
    AvroGenericStoreClient vsonClient = ClientFactory.getAndStartGenericAvroClient(ClientConfig.defaultVsonGenericClientConfig(storeName)
        .setVeniceURL(veniceCluster.getRandomRouterURL()));

    dataValidator.validate(avroClient, vsonClient);
  }

  private interface InputFileWriter {
    Pair<Schema, Schema> write(File inputDir) throws IOException;
  }

  private interface H2VValidator {
    void validate(AvroGenericStoreClient avroClient, AvroGenericStoreClient vsonClient) throws Exception;
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testRunMRJobAndPBNJ() throws Exception {
    testRunPushJobAndPBNJ(false);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testRunMapOnlyJobAndPBNJ() throws Exception {
    testRunPushJobAndPBNJ(true);
  }

  private void testRunPushJobAndPBNJ(boolean mapOnly) throws Exception {
    Utils.thisIsLocalhost();

    File inputDir = getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = TestUtils.getUniqueString("store");
    Properties props = defaultH2VProps(veniceCluster, inputDirPath, storeName);
    if (mapOnly) {
      props.setProperty(KafkaPushJob.VENICE_MAP_ONLY, "true");
    }
    props.setProperty(KafkaPushJob.PBNJ_ENABLE, "true");
    props.setProperty(KafkaPushJob.PBNJ_ROUTER_URL_PROP, veniceCluster.getRandomRouterURL());
    createStoreForJob(veniceCluster, recordSchema, props);

    KafkaPushJob job = new KafkaPushJob("Test push job", props);
    job.run();

    // Verify job properties
    Assert.assertEquals(job.getKafkaTopic(), Version.composeKafkaTopic(storeName, 1));
    Assert.assertEquals(job.getInputDirectory(), inputDirPath);
    String schema = "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"example.avro\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}]}";
    Assert.assertEquals(job.getFileSchemaString(), schema);
    Assert.assertEquals(job.getKeySchemaString(), STRING_SCHEMA);
    Assert.assertEquals(job.getValueSchemaString(), STRING_SCHEMA);
    Assert.assertEquals(job.getInputFileDataSize(), 3872);

    // Verify the data in Venice Store
    String routerUrl = veniceCluster.getRandomRouterURL();
    try(AvroGenericStoreClient<String, Object> client =
        ClientFactory.getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {
      for (int i = 1; i <= 100; ++i) {
        String expected = "test_name_" + i;
        String actual = client.get(Integer.toString(i)).get().toString(); /* client.get().get() returns a Utf8 object */
        Assert.assertEquals(actual, expected);
      }

      JobStatusQueryResponse jobStatus = ControllerClient.queryJobStatus(routerUrl, veniceCluster.getClusterName(), job.getKafkaTopic());
      Assert.assertEquals(jobStatus.getStatus(), ExecutionStatus.COMPLETED.toString(),
          "After job is complete, status should reflect that");
      // In this test we are allowing the progress to not reach the full capacity, but we still want to make sure
      // that most of the progress has completed
      Assert.assertTrue(jobStatus.getMessagesConsumed()*1.5 > jobStatus.getMessagesAvailable(),
          "Complete job should have progress");
    }
  }

  @DataProvider(name = "samplingRatios")
  public static Object[][] samplingRatios() {
    List<Object[]> returnList = new ArrayList<>();

    final long deterministicTotalRecords = 1000000;

    // Some decent values which should definitely work.
    returnList.add(new Object[]{deterministicTotalRecords, 0.001});
    returnList.add(new Object[]{deterministicTotalRecords, 0.002});
    returnList.add(new Object[]{deterministicTotalRecords, 0.005});
    returnList.add(new Object[]{deterministicTotalRecords, 0.01});
    returnList.add(new Object[]{deterministicTotalRecords, 0.02});
    returnList.add(new Object[]{deterministicTotalRecords, 0.05});
    returnList.add(new Object[]{deterministicTotalRecords, 0.1});
    returnList.add(new Object[]{deterministicTotalRecords, 0.2});
    returnList.add(new Object[]{deterministicTotalRecords, 0.5});
    returnList.add(new Object[]{deterministicTotalRecords, 0.9});
    returnList.add(new Object[]{deterministicTotalRecords, 1.0});

    // A few random record counts and ratios as well, just for the heck of it.
    while (returnList.size() < 20) {
      double randomRatio = Utils.round(Math.random(), 4);
      if (randomRatio > 0 && randomRatio < 1 && !returnList.contains(randomRatio)) {
        long nonDeterministicTotalRecords = (long) (1000000 * (1 + Math.random())); // 1M to 2M
        returnList.add(new Object[]{nonDeterministicTotalRecords, randomRatio});
      }
    }

    Object[][] valuesToReturn= new Object[returnList.size()][2];
    return returnList.toArray(valuesToReturn);
  }

  @Test(dataProvider = "samplingRatios")
  public void testSampler(long totalRecords, double expectedRatio) {
    LOGGER.info("totalRecords: " + totalRecords + ", expectedRatio: " + expectedRatio);

    long skippedRecords = 0, queriedRecords = 0;

    Sampler sampler = new Sampler(expectedRatio);

    Assert.assertFalse(sampler.checkWhetherToSkip(queriedRecords, skippedRecords),
        "The first call to checkWhetherToSkip should always allow the query.");
    queriedRecords++;
    for (int i = 1; i < totalRecords; i++) {
      long totalRecordsSoFar = queriedRecords + skippedRecords;
      Assert.assertEquals(totalRecordsSoFar, i,
          "A sampler should never increment the queriedRecords counter on its own.");

      boolean skip = sampler.checkWhetherToSkip(queriedRecords, skippedRecords);
      // LOGGER.info("Record " + i + " should skip: " + skip);
      if (skip) {
        skippedRecords++;
      } else {
        queriedRecords++;
      }
    }

    double actualRatio = 1.0 - ((double) skippedRecords) / ((double) totalRecords);
    double roundedActualRatio = Utils.round(actualRatio, 5);

    Assert.assertEquals(roundedActualRatio, expectedRatio, "The actual ratio does not match the expected ratio.");
  }
}

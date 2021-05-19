package com.linkedin.venice.endToEnd;

import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.partitioner.ConstantVenicePartitioner;
import com.linkedin.venice.utils.TestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.util.Collections;
import java.util.Properties;
import java.util.function.Consumer;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.venice.compression.CompressionStrategy.*;
import static com.linkedin.venice.hadoop.VenicePushJob.*;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.*;
import static com.linkedin.venice.utils.TestPushUtils.*;


public class DaVinciClientEndToEndTest {
  private static final Logger logger = Logger.getLogger(DaVinciClientEndToEndTest.class);
  private static final int TEST_TIMEOUT = 60 * Time.MS_PER_SECOND;
  private VeniceClusterWrapper cluster;

  @BeforeClass
  public void setup() {
    Utils.thisIsLocalhost();
    cluster = ServiceFactory.getVeniceCluster(1, 1, 1);
  }

  @AfterClass
  public void cleanup() {
    IOUtils.closeQuietly(cluster);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testCustomPartitioner() throws Exception {
    final int partitionId = 1;
    String storeName = TestUtils.getUniqueString("batch-store");
    Consumer<UpdateStoreQueryParams> paramsConsumer =
        params -> params.setPartitionerClass(ConstantVenicePartitioner.class.getName()).setCompressionStrategy(GZIP)
        .setPartitionerParams(
            Collections.singletonMap(ConstantVenicePartitioner.CONSTANT_PARTITION, String.valueOf(partitionId))
        )
        .setPartitionCount(3);
    setUpStore(storeName, paramsConsumer,
        properties -> properties.setProperty(VENICE_PARTITIONERS_PROP, ConstantVenicePartitioner.class.getName()));
    try (DaVinciClient<Object, Object> client = ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster)) {
      client.subscribe(Collections.singleton(partitionId)).get();
      for (int i = 1; i <= 100; ++i) {
        Object value = client.get(i).get();
        Assert.assertEquals(value.toString(), "name " + i);
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testCustomPartitionerWithDefaultParams() throws Exception {
    String storeName = TestUtils.getUniqueString("batch-store");
    setUpStore(storeName, params -> {}, properties -> {});
    try (DaVinciClient<Object, Object> client = ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster)) {
      client.subscribeAll().get();
      for (int i = 1; i <= 100; ++i) {
        Object value = client.get(i).get();
        Assert.assertEquals(value.toString(), "name " + i);
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testAmplificationFactor() throws Exception {
    final int partitionId = 1;
    final int amplificationFactor = 10;
    String storeName = TestUtils.getUniqueString("batch-store");
    Consumer<UpdateStoreQueryParams> paramsConsumer =
        params -> params.setAmplificationFactor(amplificationFactor)
            .setLeaderFollowerModel(true)
            .setCompressionStrategy(ZSTD_WITH_DICT)
            .setPartitionerClass(ConstantVenicePartitioner.class.getName())
            .setPartitionerParams(
                Collections.singletonMap(ConstantVenicePartitioner.CONSTANT_PARTITION, String.valueOf(partitionId)));
    Consumer<Properties> propertiesConsumer = properties ->
        properties.setProperty(VENICE_PARTITIONERS_PROP, ConstantVenicePartitioner.class.getName());
    setUpStore(storeName, paramsConsumer, propertiesConsumer);
    try (DaVinciClient<Object, Object> client = ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster)) {
      client.subscribe(Collections.singleton(partitionId)).get();
      for (int i = 1; i <= 100; ++i) {
        Object value = client.get(i).get();
        Assert.assertEquals(value.toString(), "name " + i);
      }
    }
  }

  public void setUpStore(String storeName, Consumer<UpdateStoreQueryParams> paramsConsumer,
      Consumer<Properties> propertiesConsumer) throws Exception {
    // Produce input data.
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    writeSimpleAvroFileWithIntToStringSchema(inputDir, true);

    // Setup H2V job properties.
    Properties h2vProperties = defaultH2VProps(cluster, inputDirPath, storeName);
    propertiesConsumer.accept(h2vProperties);
    // Create & update store for test.
    final int numPartitions = 3;
    UpdateStoreQueryParams params = new UpdateStoreQueryParams()
        .setPartitionCount(numPartitions); // Update the partition count.
    paramsConsumer.accept(params);
    try (ControllerClient controllerClient =
        createStoreForJob(cluster, DEFAULT_KEY_SCHEMA, "\"string\"", h2vProperties)) {
      ControllerResponse response = controllerClient.updateStore(storeName, params);
      Assert.assertFalse(response.isError(), response.getError());

      // Push data through H2V bridge.
      runH2V(h2vProperties, 1, cluster);
    }
  }

  private static void runH2V(Properties h2vProperties, int expectedVersionNumber, VeniceClusterWrapper cluster) {
    long h2vStart = System.currentTimeMillis();
    String jobName = TestUtils.getUniqueString("batch-job-" + expectedVersionNumber);
    TestPushUtils.runPushJob(jobName, h2vProperties);
    String storeName = (String) h2vProperties.get(VenicePushJob.VENICE_STORE_NAME_PROP);
    cluster.waitVersion(storeName, expectedVersionNumber);
    logger.info("**TIME** H2V" + expectedVersionNumber + " takes " + (System.currentTimeMillis() - h2vStart));
  }
}

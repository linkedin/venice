package com.linkedin.venice.endToEnd;

import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.hadoop.KafkaPushJob;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.partitioner.ConstantVenicePartitioner;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.util.Collections;
import java.util.Properties;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.venice.hadoop.KafkaPushJob.*;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.*;
import static com.linkedin.venice.utils.TestPushUtils.*;


public class DaVinciClientEndToEndTest {
  private static final Logger logger = Logger.getLogger(DaVinciClientEndToEndTest.class);

  @Test(timeOut = 60000)
  public void testCustomPartitionerInBatchStore() throws Exception {
    final int partitionId = 1;

    Utils.thisIsLocalhost();
    try (VeniceClusterWrapper cluster = ServiceFactory.getVeniceCluster(1, 1, 1)) {
      String storeName = TestUtils.getUniqueString("batch-store");

      // Produce input data.
      File inputDir = getTempDataDirectory();
      String inputDirPath = "file://" + inputDir.getAbsolutePath();
      writeSimpleAvroFileWithIntToIntSchema(inputDir, true);

      // Setup H2V job properties.
      Properties h2vProperties = defaultH2VProps(cluster, inputDirPath, storeName);
      h2vProperties.setProperty(VENICE_PARTITIONERS_PROP, ConstantVenicePartitioner.class.getName());

      // Create & update store for test.
      try (ControllerClient controllerClient = createStoreForJob(cluster, DEFAULT_KEY_SCHEMA, DEFAULT_VALUE_SCHEMA, h2vProperties)) {
        controllerClient.updateStore(storeName, new UpdateStoreQueryParams()
            .setPartitionCount(3) // Update the partition count to default partition count (3).
            .setPartitionerClass(ConstantVenicePartitioner.class.getName())
            .setPartitionerParams(
                Collections.singletonMap(ConstantVenicePartitioner.CONSTANT_PARTITION, String.valueOf(partitionId))
            )
        );

        // Push data through H2V bridge.
        runH2V(h2vProperties, 1, cluster);
      }

      try (DaVinciClient<Object, Object> client = ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster)) {
        client.subscribe(Collections.singleton(partitionId)).get();
        for (int i = 1; i <= 100; ++i) {
          Object value = client.get(i).get();
          Assert.assertEquals(value, i);
        }
      }
    }
  }

  private static void runH2V(Properties h2vProperties, int expectedVersionNumber, VeniceClusterWrapper cluster) throws Exception {
    long h2vStart = System.currentTimeMillis();
    String jobName = TestUtils.getUniqueString("batch-job-" + expectedVersionNumber);
    KafkaPushJob job = new KafkaPushJob(jobName, h2vProperties);
    job.run();
    String storeName = (String) h2vProperties.get(KafkaPushJob.VENICE_STORE_NAME_PROP);
    cluster.waitVersion(storeName, expectedVersionNumber);
    logger.info("**TIME** H2V" + expectedVersionNumber + " takes " + (System.currentTimeMillis() - h2vStart));
  }

}

package com.linkedin.venice.endToEnd;

import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.hadoop.KafkaPushJob;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.pushstatus.PushStatusValue;
import com.linkedin.venice.schema.WriteComputeSchemaAdapter;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.hadoop.KafkaPushJob.*;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.*;
import static com.linkedin.venice.utils.TestPushUtils.*;


public class PushStatusStoreTest {
  private VeniceClusterWrapper cluster;

  @BeforeClass
  public void setup() {
    Properties extraProperties = new Properties();
    extraProperties.setProperty(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(1L));
    Utils.thisIsLocalhost();
    cluster = ServiceFactory.getVeniceCluster(
        1,
        1,
        1,
        1,
        10000,
        false,
        false,
        extraProperties);
  }

  @AfterClass
  public void cleanup() {
    IOUtils.closeQuietly(cluster);
  }

  @Test
  public void testKafkaPushJob() throws Exception {
    String storeName = TestUtils.getUniqueString("store");
    String owner = "test";
    String zkSharedPushStatusStoreName = VeniceSystemStoreUtils.getSharedZkNameForDaVinciPushStatusStore(cluster.getClusterName());
    String pushStatusStoreName = VeniceSystemStoreUtils.getDaVinciPushStatusStoreName(storeName);
    ControllerClient controllerClient = cluster.getControllerClient();
    controllerClient.createNewZkSharedStoreWithDefaultConfigs(zkSharedPushStatusStoreName, owner);
    controllerClient.updateStore(zkSharedPushStatusStoreName, new UpdateStoreQueryParams()
        .setLeaderFollowerModel(true)
        .setWriteComputationEnabled(true)
        .setHybridRewindSeconds(10)
        .setHybridOffsetLagThreshold(10));
    controllerClient.newZkSharedStoreVersion(zkSharedPushStatusStoreName);
    controllerClient.createNewStore(storeName, owner, DEFAULT_KEY_SCHEMA, DEFAULT_VALUE_SCHEMA).isError();
    controllerClient.updateStore(storeName, new UpdateStoreQueryParams()
        .setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA));
    controllerClient.createDaVinciPushStatusStore(storeName);
    int valueSchemaId = controllerClient.getValueSchemaID(pushStatusStoreName, PushStatusValue.SCHEMA$.toString()).getId();
    Schema writeComputeSchema = WriteComputeSchemaAdapter.parse(PushStatusValue.SCHEMA$.toString()).getTypes().get(0);
    controllerClient.addDerivedSchema(pushStatusStoreName, valueSchemaId,
        writeComputeSchema.toString());

    // Produce input data.
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    writeSimpleAvroFileWithIntToIntSchema(inputDir, true);

    // Setup H2V job properties.
    Properties h2vProperties = defaultH2VProps(cluster, inputDirPath, storeName);
    // setup initial version
    runH2V(h2vProperties, 1, cluster);

    DaVinciClient daVinciClient = ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster);
    daVinciClient.subscribeAll().get();
    runH2V(h2vProperties, 2, cluster);

    // clean up
    controllerClient.close();
    daVinciClient.close();
  }

  @Test
  public void testIncrementalPush() throws Exception {
    String storeName = TestUtils.getUniqueString("store");
    String owner = "test";
    String zkSharedPushStatusStoreName = VeniceSystemStoreUtils.getSharedZkNameForDaVinciPushStatusStore(cluster.getClusterName());
    String pushStatusStoreName = VeniceSystemStoreUtils.getDaVinciPushStatusStoreName(storeName);
    ControllerClient controllerClient = cluster.getControllerClient();
    controllerClient.createNewZkSharedStoreWithDefaultConfigs(zkSharedPushStatusStoreName, owner);
    controllerClient.updateStore(zkSharedPushStatusStoreName, new UpdateStoreQueryParams()
        .setLeaderFollowerModel(true)
        .setWriteComputationEnabled(true)
        .setHybridRewindSeconds(10)
        .setHybridOffsetLagThreshold(10));
    controllerClient.newZkSharedStoreVersion(zkSharedPushStatusStoreName);
    controllerClient.createNewStore(storeName, owner, DEFAULT_KEY_SCHEMA, DEFAULT_VALUE_SCHEMA).isError();
    controllerClient.updateStore(storeName, new UpdateStoreQueryParams()
        .setIncrementalPushEnabled(true)
        .setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA));
    controllerClient.createDaVinciPushStatusStore(storeName);
    int valueSchemaId = controllerClient.getValueSchemaID(pushStatusStoreName, PushStatusValue.SCHEMA$.toString()).getId();
    Schema writeComputeSchema = WriteComputeSchemaAdapter.parse(PushStatusValue.SCHEMA$.toString()).getTypes().get(0);
    controllerClient.addDerivedSchema(pushStatusStoreName, valueSchemaId,
        writeComputeSchema.toString());

    // Produce input data.
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    writeSimpleAvroFileWithIntToIntSchema(inputDir, true);

    // Setup H2V job properties.
    Properties h2vProperties = defaultH2VProps(cluster, inputDirPath, storeName);
    // setup initial version
    runH2V(h2vProperties, 1, cluster);

    DaVinciClient daVinciClient = ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster);
    daVinciClient.subscribeAll().get();
    h2vProperties.setProperty(INCREMENTAL_PUSH, "true");
    runH2V(h2vProperties, 1, cluster);

    // clean up
    controllerClient.close();
    daVinciClient.close();
  }

  private static void runH2V(Properties h2vProperties, int expectedVersionNumber, VeniceClusterWrapper cluster) {
    long h2vStart = System.currentTimeMillis();
    String jobName = TestUtils.getUniqueString("batch-job-" + expectedVersionNumber);
    KafkaPushJob job = new KafkaPushJob(jobName, h2vProperties);
    job.run();
    String storeName = (String) h2vProperties.get(KafkaPushJob.VENICE_STORE_NAME_PROP);
    cluster.waitVersion(storeName, expectedVersionNumber);
    logger.info("**TIME** H2V" + expectedVersionNumber + " takes " + (System.currentTimeMillis() - h2vStart));
  }

}

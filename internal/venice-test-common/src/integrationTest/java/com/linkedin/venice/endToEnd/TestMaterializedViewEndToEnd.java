package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.CHILD_DATA_CENTER_KAFKA_URL_PREFIX;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_PARENT_DATA_CENTER_REGION_NAME;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.views.MaterializedView.MATERIALIZED_VIEW_TOPIC_SUFFIX;
import static com.linkedin.venice.views.VeniceView.VIEW_TOPIC_SEPARATOR;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.ViewConfig;
import com.linkedin.venice.meta.ViewParameters;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.views.MaterializedView;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestMaterializedViewEndToEnd {
  private static final int TEST_TIMEOUT = 2 * Time.MS_PER_MINUTE;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, 1).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);

  private List<VeniceMultiClusterWrapper> childDatacenters;
  private List<VeniceControllerWrapper> parentControllers;
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private String clusterName;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    Properties serverProperties = new Properties();
    serverProperties.setProperty(ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(1));
    serverProperties.put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, false);
    serverProperties.put(
        CHILD_DATA_CENTER_KAFKA_URL_PREFIX + "." + DEFAULT_PARENT_DATA_CENTER_REGION_NAME,
        "localhost:" + TestUtils.getFreePort());
    multiRegionMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
        2,
        1,
        1,
        1,
        2,
        1,
        2,
        Optional.empty(),
        Optional.empty(),
        Optional.of(serverProperties),
        false);

    childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
    parentControllers = multiRegionMultiClusterWrapper.getParentControllers();
    clusterName = CLUSTER_NAMES[0];
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    multiRegionMultiClusterWrapper.close();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testLFIngestionWithMaterializedView() throws IOException {
    // Create a non-A/A store with materialized view and run batch push job with 100 records
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    Properties props =
        TestWriteUtils.defaultVPJProps(parentControllers.get(0).getControllerUrl(), inputDirPath, storeName);
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(false)
        .setHybridRewindSeconds(500)
        .setHybridOffsetLagThreshold(8)
        .setChunkingEnabled(true)
        .setNativeReplicationEnabled(true)
        .setNativeReplicationSourceFabric(childDatacenters.get(0).getRegionName())
        .setPartitionCount(3);
    try (ControllerClient controllerClient =
        IntegrationTestPushUtils.createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParms)) {
      String testViewName = "MaterializedViewTest";
      ViewParameters.Builder viewParamBuilder = new ViewParameters.Builder(testViewName).setPartitionCount(6);
      UpdateStoreQueryParams updateViewParam = new UpdateStoreQueryParams().setViewName(testViewName)
          .setViewClassName(MaterializedView.class.getCanonicalName())
          .setViewClassParams(viewParamBuilder.build());
      controllerClient
          .retryableRequest(5, controllerClient1 -> controllerClient.updateStore(storeName, updateViewParam));
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, () -> {
        Map<String, ViewConfig> viewConfigMap = controllerClient.getStore(storeName).getStore().getViewConfigs();
        Assert.assertEquals(viewConfigMap.size(), 1);
        Assert.assertEquals(
            viewConfigMap.get(testViewName).getViewClassName(),
            MaterializedView.class.getCanonicalName());
        Assert.assertEquals(viewConfigMap.get(testViewName).getViewParameters().size(), 3);
      });

      TestWriteUtils.runPushJob("Run push job", props);
      // TODO we will verify the actual content once the DVC consumption part of the view topic is completed.
      // For now just check for topic existence and that they contain some records.
      String viewTopicName = Version.composeKafkaTopic(storeName, 1) + VIEW_TOPIC_SEPARATOR + testViewName
          + MATERIALIZED_VIEW_TOPIC_SUFFIX;
      String versionTopicName = Version.composeKafkaTopic(storeName, 1);
      for (VeniceMultiClusterWrapper veniceClusterWrapper: childDatacenters) {
        VeniceHelixAdmin admin = veniceClusterWrapper.getRandomController().getVeniceHelixAdmin();
        PubSubTopic viewPubSubTopic = admin.getPubSubTopicRepository().getTopic(viewTopicName);
        PubSubTopic versionPubSubTopic = admin.getPubSubTopicRepository().getTopic(versionTopicName);
        Assert.assertTrue(admin.getTopicManager().containsTopic(viewPubSubTopic));
        long records = 0;
        long versionTopicRecords = 0;
        Int2LongMap viewTopicOffsetMap = admin.getTopicManager().getTopicLatestOffsets(viewPubSubTopic);
        Int2LongMap versionTopicOffsetMap = admin.getTopicManager().getTopicLatestOffsets(versionPubSubTopic);
        Assert.assertEquals(versionTopicOffsetMap.keySet().size(), 3, "Unexpected version partition count");
        Assert.assertEquals(viewTopicOffsetMap.keySet().size(), 6, "Unexpected view partition count");
        for (long endOffset: viewTopicOffsetMap.values()) {
          records += endOffset;
        }
        for (long endOffset: versionTopicOffsetMap.values()) {
          versionTopicRecords += endOffset;
        }
        Assert.assertTrue(versionTopicRecords > 100, "Version topic records size: " + versionTopicRecords);
        if (!veniceClusterWrapper.getRegionName().equals(childDatacenters.get(0).getRegionName())) {
          Assert.assertTrue(records > 100, "View topic records size: " + records);
        }
      }
    }
  }
}

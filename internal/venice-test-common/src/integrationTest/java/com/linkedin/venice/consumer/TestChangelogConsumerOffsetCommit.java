package com.linkedin.venice.consumer;

import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.CONSUMER_METRIC_ENTITIES;
import static com.linkedin.venice.ConfigKeys.KAFKA_GROUP_ID_CONFIG;
import static com.linkedin.venice.stats.ClientType.CHANGE_DATA_CAPTURE_CLIENT;
import static com.linkedin.venice.stats.VeniceMetricsRepository.getVeniceMetricsRepository;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V1_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;

import com.linkedin.davinci.consumer.ChangeEvent;
import com.linkedin.davinci.consumer.ChangelogClientConfig;
import com.linkedin.davinci.consumer.VeniceChangeCoordinate;
import com.linkedin.davinci.consumer.VeniceChangelogConsumer;
import com.linkedin.davinci.consumer.VeniceChangelogConsumerClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.integration.utils.IntegrationTestUtils;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Verifies that changelog consumers with {@link ChangelogClientConfig#setConsumerOffsetCommitEnabled(boolean)} commit
 * the positions they have consumed to the broker under the configured group id, without joining the group.
 */
public class TestChangelogConsumerOffsetCommit {
  private static final int TEST_TIMEOUT = 3 * Time.MS_PER_MINUTE;
  private static final int PARTITION_COUNT = 3;
  private static final int RECORD_COUNT = 100;

  private ChangelogConsumerTestFixture fixture;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    fixture = new ChangelogConsumerTestFixture(false);
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(fixture);
  }

  @AfterMethod(alwaysRun = true)
  public void cleanupAfterTest() {
    fixture.cleanupAfterTest();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testAfterImageConsumerCommitsConsumedPositions() throws Exception {
    testConsumerCommitsConsumedPositions(false);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testDaVinciBasedConsumerCommitsConsumedPositions() throws Exception {
    testConsumerCommitsConsumedPositions(true);
  }

  private void testConsumerCommitsConsumedPositions(boolean daVinciBased) throws Exception {
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToNameRecordV1Schema(inputDir, RECORD_COUNT);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("offset-commit-store");
    fixture.addStoreToDelete(storeName);
    Properties props = TestWriteUtils.defaultVPJProps(
        fixture.getParentControllers().get(0).getControllerUrl(),
        inputDirPath,
        storeName,
        fixture.getClusterWrapper().getPubSubClientProperties());
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    ControllerClient setupControllerClient = createStoreForJob(
        fixture.getClusterName(),
        keySchemaStr,
        NAME_RECORD_V1_SCHEMA.toString(),
        props,
        ChangelogConsumerTestUtils.buildDefaultStoreParams().setPartitionCount(PARTITION_COUNT));
    fixture.addCloseable(setupControllerClient);
    ChangelogConsumerTestUtils.waitForMetaSystemStoreToBeReady(
        storeName,
        fixture.getChildControllerClientRegion0(),
        fixture.getClusterWrapper());
    IntegrationTestPushUtils.runVPJ(props, 1, fixture.getChildControllerClientRegion0());

    String groupId = Utils.getUniqueString("changelog-offset-commit-group");
    Properties consumerProperties = ChangelogConsumerTestUtils.buildConsumerProperties(
        fixture.getMultiRegionMultiClusterWrapper(),
        fixture.getLocalKafka(),
        fixture.getClusterName(),
        fixture.getLocalZkServer());
    consumerProperties.put(KAFKA_GROUP_ID_CONFIG, groupId);
    ChangelogClientConfig globalChangelogClientConfig = ChangelogConsumerTestUtils
        .buildBaseChangelogClientConfig(consumerProperties, fixture.getLocalZkServer().getAddress(), 1)
        .setD2Client(fixture.getD2Client())
        .setBootstrapFileSystemPath(Utils.getUniqueString(inputDirPath))
        .setIsNewStatelessClientEnabled(daVinciBased)
        .setConsumerOffsetCommitEnabled(true)
        // Commits are forced below via commitOffsets(), so the periodic interval does not interfere.
        .setConsumerOffsetCommitIntervalMs(TimeUnit.HOURS.toMillis(1));
    VeniceChangelogConsumerClientFactory factory = new VeniceChangelogConsumerClientFactory(
        globalChangelogClientConfig,
        getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true));
    VeniceChangelogConsumer<Utf8, Utf8> changelogConsumer = factory.getChangelogConsumer(storeName, "0");
    fixture.addCloseable(changelogConsumer);

    changelogConsumer.subscribeAll().get();
    Map<String, PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> polledChangeEvents = new HashMap<>();
    TestUtils.waitForNonDeterministicAssertion(120, TimeUnit.SECONDS, true, true, () -> {
      IntegrationTestUtils.pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, changelogConsumer);
      Assert.assertEquals(polledChangeEvents.size(), RECORD_COUNT);
    });

    changelogConsumer.commitOffsets();

    String versionTopic = Version.composeKafkaTopic(storeName, 1);
    Properties adminProperties = new Properties();
    adminProperties.put("bootstrap.servers", fixture.getLocalKafka().getAddress());
    try (AdminClient admin = AdminClient.create(adminProperties)) {
      Map<TopicPartition, OffsetAndMetadata> committed =
          admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get(30, TimeUnit.SECONDS);
      for (int partition = 0; partition < PARTITION_COUNT; partition++) {
        OffsetAndMetadata offsetAndMetadata = committed.get(new TopicPartition(versionTopic, partition));
        Assert.assertNotNull(offsetAndMetadata, "Missing committed offset for partition " + partition);
        Assert.assertTrue(offsetAndMetadata.offset() > 0, "Committed offset should be positive: " + offsetAndMetadata);
      }

      // Committing must not make the consumer a member of the group.
      ConsumerGroupDescription description =
          admin.describeConsumerGroups(Collections.singleton(groupId)).describedGroups().get(groupId).get();
      Assert.assertTrue(description.members().isEmpty(), "Group should have no members: " + description);
    }
  }
}

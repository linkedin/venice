package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.CLUSTER_ENCRYPTION_ENABLED;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.runVPJ;
import static com.linkedin.venice.utils.TestWriteUtils.DEFAULT_USER_DATA_RECORD_COUNT;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V1_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithStringToNameRecordV1Schema;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * End-to-end coverage tying {@link VenicePushJob}'s writer-encryption selection to the store's
 * {@code pubSubEncryptionKeyUrn}. The push job only learns about the key URN after an {@code UpdateStore}
 * admin message has round-tripped through the parent controller's admin topic, replicated to the child
 * region's admin topic, and been applied to the store's metadata there, so these tests run a real
 * parent + child controller topology and exercise the full path: parent admin topic -> child admin topic ->
 * store config -> {@code PubSubEncryptionUtils} key lookup -> {@code VeniceWriterFactory}'s
 * producer-encryption flag. They guard against that flag being derived incorrectly (e.g. hardcoded instead
 * of based on whether a key URN lookup is actually available).
 */
public class TestPushJobWithEncryptionKeyUrn {
  private static final int TEST_TIMEOUT = 90 * Time.MS_PER_SECOND;

  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private String clusterName;
  private VeniceClusterWrapper childCluster;
  private ControllerClient childControllerClient;

  @BeforeClass
  public void setUp() {
    Utils.thisIsLocalhost();
    Properties parentControllerProperties = new Properties();
    parentControllerProperties.setProperty(CLUSTER_ENCRYPTION_ENABLED, "true");
    VeniceMultiRegionClusterCreateOptions options =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(1)
            .numberOfClusters(1)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfServers(1)
            .numberOfRouters(1)
            .replicationFactor(1)
            .sslToStorageNodes(false)
            .sslToKafka(false)
            .parentControllerProperties(parentControllerProperties)
            .build();
    multiRegionMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(options);
    clusterName = multiRegionMultiClusterWrapper.getClusterNames()[0];
    childCluster = multiRegionMultiClusterWrapper.getChildRegions().get(0).getClusters().get(clusterName);
    childControllerClient = new ControllerClient(
        clusterName,
        multiRegionMultiClusterWrapper.getChildRegions().get(0).getControllerConnectString());
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(childControllerClient, multiRegionMultiClusterWrapper);
  }

  /**
   * A store in an encryption-enabled cluster defaults to {@code encryptionEnabled=true} with no key URN.
   * VenicePushJob must fail fast rather than push with an incomplete encryption configuration.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testPushFailsWhenEncryptionEnabledStoreHasNoKeyUrn() throws Exception {
    File inputDir = Utils.getTempDataDirectory();
    writeSimpleAvroFileWithStringToNameRecordV1Schema(inputDir);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("encryption-missing-key-store");

    Properties props = defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    try (ControllerClient controllerClient =
        createStoreForJob(childCluster, STRING_SCHEMA.toString(), NAME_RECORD_V1_SCHEMA.toString(), props)) {
      Assert.assertTrue(
          controllerClient.getStore(storeName).getStore().isEncryptionEnabled(),
          "A newly created store in an encryption-enabled cluster must default to encryptionEnabled=true");
      Assert.assertEquals(controllerClient.getStore(storeName).getStore().getPubSubEncryptionKeyUrn(), "");
    }

    // Confirm store creation round-tripped from the parent controller's admin topic to the child region's
    // admin topic before exercising the push-job failure path.
    TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
      Assert.assertTrue(childControllerClient.getStore(storeName).getStore().isEncryptionEnabled());
      Assert.assertEquals(childControllerClient.getStore(storeName).getStore().getPubSubEncryptionKeyUrn(), "");
    });

    try (VenicePushJob job = new VenicePushJob("test-push-missing-key-urn", props)) {
      VeniceException exception = Assert.expectThrows(VeniceException.class, job::run);
      Assert.assertTrue(
          exception.getMessage().contains("pubSubEncryptionKeyUrn"),
          "Unexpected exception message: " + exception.getMessage());
    }
  }

  /**
   * Once an {@code UpdateStore} admin message (consumed off the admin topic) applies a key URN to an
   * encryption-enabled store, VenicePushJob picks up the URN, builds a non-null key lookup, and the
   * resulting VeniceWriterFactory enables producer encryption end to end: the push succeeds and the
   * data pushed is readable back through the router.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testPushSucceedsAfterAdminTopicAppliesKeyUrn() throws Exception {
    File inputDir = Utils.getTempDataDirectory();
    writeSimpleAvroFileWithStringToNameRecordV1Schema(inputDir);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("encryption-with-key-store");
    String keyUrn = "urn:li:dataEncryptionKey:test-" + storeName;

    Properties props = defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    try (ControllerClient controllerClient =
        createStoreForJob(childCluster, STRING_SCHEMA.toString(), NAME_RECORD_V1_SCHEMA.toString(), props)) {
      ControllerResponse updateResponse =
          controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setPubSubEncryptionKeyUrn(keyUrn));
      Assert.assertFalse(updateResponse.isError(), "Updating the key URN should succeed: " + updateResponse.getError());

      // The URN is only visible on the child region once the UpdateStore admin message has round-tripped
      // through the parent controller's admin topic, replicated to the child region's admin topic, and
      // been applied there by the child controller's AdminConsumptionTask.
      TestUtils.waitForNonDeterministicAssertion(
          TEST_TIMEOUT,
          TimeUnit.MILLISECONDS,
          () -> Assert
              .assertEquals(childControllerClient.getStore(storeName).getStore().getPubSubEncryptionKeyUrn(), keyUrn));

      runVPJ(props, 1, childControllerClient);
    }

    try (AvroGenericStoreClient<String, GenericRecord> avroClient = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(childCluster.getRandomRouterURL()))) {
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, true, true, () -> {
        for (int i = 1; i <= DEFAULT_USER_DATA_RECORD_COUNT; i++) {
          GenericRecord value = avroClient.get(Integer.toString(i)).get();
          Assert.assertNotNull(value, "Missing value for key: " + i);
          Assert.assertEquals(value.get("firstName").toString(), "first_name_" + i);
          Assert.assertEquals(value.get("lastName").toString(), "last_name_" + i);
        }
      });
    }
  }
}

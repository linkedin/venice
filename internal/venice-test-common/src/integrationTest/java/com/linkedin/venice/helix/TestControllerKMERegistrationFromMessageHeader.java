package com.linkedin.venice.helix;

import static com.linkedin.venice.ConfigKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS;
import static com.linkedin.venice.ConfigKeys.DEFAULT_NUMBER_OF_PARTITION_FOR_HYBRID;
import static com.linkedin.venice.ConfigKeys.DEFAULT_PARTITION_SIZE;
import static com.linkedin.venice.ConfigKeys.KME_REGISTRATION_FROM_MESSAGE_HEADER_ENABLED;

import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestControllerKMERegistrationFromMessageHeader {
  private static final Logger LOGGER = LogManager.getLogger(TestControllerKMERegistrationFromMessageHeader.class);
  private static final int TEST_TIMEOUT = 90_000; // ms
  private static final int NUMBER_OF_CHILD_DATACENTERS = 1;
  private static final int NUMBER_OF_CLUSTERS = 1;

  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);

  private List<VeniceMultiClusterWrapper> childDatacenters;
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;

  @BeforeClass
  public void setUp() {
    Properties controllerProps = new Properties();
    controllerProps.put(DEFAULT_NUMBER_OF_PARTITION_FOR_HYBRID, 2);
    controllerProps.put(DEFAULT_MAX_NUMBER_OF_PARTITIONS, 3);
    controllerProps.put(DEFAULT_PARTITION_SIZE, 1024);
    controllerProps.put(KME_REGISTRATION_FROM_MESSAGE_HEADER_ENABLED, true);
    VeniceMultiRegionClusterCreateOptions.Builder optionsBuilder =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(NUMBER_OF_CHILD_DATACENTERS)
            .numberOfClusters(NUMBER_OF_CLUSTERS)
            .numberOfParentControllers(1)
            .numberOfChildControllers(3)
            .numberOfServers(1)
            .numberOfRouters(1)
            .replicationFactor(1)
            .forkServer(false)
            .parentControllerProperties(controllerProps)
            .childControllerProperties(controllerProps);
    multiRegionMultiClusterWrapper =
        ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(optionsBuilder.build());

    childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testKMERegistrationThroughAdminTopicChannel() {
    String clusterName = CLUSTER_NAMES[0];
    String storeName = Utils.getUniqueString("store");

    VeniceControllerWrapper pController =
        multiRegionMultiClusterWrapper.getLeaderParentControllerWithRetries(clusterName);

    VeniceMultiClusterWrapper child = childDatacenters.get(0);

    VeniceControllerWrapper leaderController = child.getLeaderController(clusterName);
    KafkaValueSerializer valueSerializer =
        leaderController.getController().getVeniceControllerService().getKafkaValueSerializer();
    HelixReadWriteSchemaRepositoryAdapter adapter =
        (HelixReadWriteSchemaRepositoryAdapter) (leaderController.getVeniceHelixAdmin()
            .getHelixVeniceClusterResources(clusterName)
            .getSchemaRepository());
    HelixReadWriteSchemaRepository repo =
        (HelixReadWriteSchemaRepository) adapter.getReadWriteRegularStoreSchemaRepository();

    // Wait until the latest schema appears in child colo's schema repository.
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
      Assert.assertTrue(
          repo.getValueSchema(
              AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getSystemStoreName(),
              AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getCurrentProtocolVersion()) != null);
    });

    // Remove the latest schema from child controller's local value serializer and remove it from child colo's schema
    // repository (ZK).
    repo.forceRemoveValueSchema(
        AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getSystemStoreName(),
        AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getCurrentProtocolVersion());
    valueSerializer.removeAllSchemas();
    LOGGER.info("all schemas are removed");

    // Verify that the latest version of the protocol is deleted in ZK.
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      Assert.assertTrue(
          repo.getValueSchema(
              AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getSystemStoreName(),
              AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getCurrentProtocolVersion()) == null);
    });

    /*
     * Calling parent controller's create store action which will trigger an admin message which contains
     * the latest schema in its header, child controller when it encounters the new schema in the message header,
     * would register the new schema into the child colo's schema repo as well as add to its local serializer.
     */

    pController.getVeniceAdmin().createStore(clusterName, storeName, "", "\"string\"", "\"string\"", false);

    // Verify that schema is registered in the child colo's schema repo.
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
      Assert.assertEquals(
          repo.getValueSchema(
              AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getSystemStoreName(),
              AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getCurrentProtocolVersion()).getId(),
          AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getCurrentProtocolVersion());
    });

    // Verify that store is created successfully in the child colo from the child controller's view.
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      Assert.assertTrue(leaderController.getVeniceAdmin().getStore(clusterName, storeName).getName().equals(storeName));
    });
  }
}

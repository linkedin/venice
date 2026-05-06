package com.linkedin.venice.helix;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_STATE_PROTOCOL_SCHEMA_STARTUP_REGISTRATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS;
import static com.linkedin.venice.ConfigKeys.DEFAULT_NUMBER_OF_PARTITION_FOR_HYBRID;
import static com.linkedin.venice.ConfigKeys.DEFAULT_PARTITION_SIZE;

import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestControllerStateProtocolSchemaStartupRegistration {
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
    controllerProps.put(CONTROLLER_STATE_PROTOCOL_SCHEMA_STARTUP_REGISTRATION_ENABLED, true);
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
  public void testStateProtocolSchemasRegisteredAtChildControllerStartup() {
    String systemSchemaCluster = CLUSTER_NAMES[0];
    VeniceMultiClusterWrapper child = childDatacenters.get(0);
    VeniceControllerWrapper systemSchemaLeader = child.getLeaderController(systemSchemaCluster);

    HelixReadWriteSchemaRepositoryAdapter adapter =
        (HelixReadWriteSchemaRepositoryAdapter) systemSchemaLeader.getVeniceHelixAdmin()
            .getHelixVeniceClusterResources(systemSchemaCluster)
            .getSchemaRepository();
    HelixReadWriteSchemaRepository repo =
        (HelixReadWriteSchemaRepository) adapter.getReadWriteRegularStoreSchemaRepository();

    // With the flag enabled, every child controller startup runs ControllerClientBackedSystemSchemaInitializer
    // for PARTITION_STATE and STORE_VERSION_STATE. The current protocol version of each schema must be
    // registered in the system schema cluster.
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
      Assert.assertNotNull(
          repo.getValueSchema(
              AvroProtocolDefinition.PARTITION_STATE.getSystemStoreName(),
              AvroProtocolDefinition.PARTITION_STATE.getCurrentProtocolVersion()),
          "PARTITION_STATE current schema should be registered after child controller startup");
      Assert.assertNotNull(
          repo.getValueSchema(
              AvroProtocolDefinition.STORE_VERSION_STATE.getSystemStoreName(),
              AvroProtocolDefinition.STORE_VERSION_STATE.getCurrentProtocolVersion()),
          "STORE_VERSION_STATE current schema should be registered after child controller startup");
    });
  }
}

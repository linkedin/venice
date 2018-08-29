package com.linkedin.venice.endToEnd;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.StoreMigrationResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.KafkaPushJob;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.venice.utils.TestPushUtils.*;


public class TestStoreMigration {
  private static final Logger logger = Logger.getLogger(TestStoreMigration.class);
  private static final int MAX_RETRY = 5;
  private static final int NUM_OF_CONTROLLERS = 3; // number of controller cannot be more than 3

  @Test
  public void testSchemaPreservation() {
    String valueSchema1 = "{\"type\":\"record\",\"name\":\"HashtagPoolValue\",\"namespace\":\"com.linkedin.hashtags\",\"fields\":[{\"name\":\"hashtags\",\"type\":{\"type\":\"array\",\"items\":\"string\"},\"doc\":\"The list of hashtags in this pool\"}]}";
    String valueSchema2 = "{\"type\":\"record\",\"name\":\"HashtagPoolValue\",\"namespace\":\"com.linkedin.hashtags\",\"fields\":[{\"name\":\"hashtags\",\"type\":{\"type\":\"array\",\"items\":\"string\"},\"doc\":\"The list of hashtags in this pool\"},{\"name\":\"poolFeatures\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"FeatureVector\",\"fields\":[{\"name\":\"features\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"Feature\",\"fields\":[{\"name\":\"qualifiedName\",\"type\":{\"type\":\"array\",\"items\":\"string\"},\"doc\":\"A list of strings to represent name of a unique feature. The array allows for grouping features hierarchically into sections.\"},{\"name\":\"value\",\"type\":\"double\",\"doc\":\"Value of a raw feature, range from -inf to inf\"}]}},\"doc\":\"Array of features\"}]}],\"doc\":\"The feature vector shared across all the hashtags within this pool.\",\"default\":null}]}";

    boolean sharedControllerTested = false;
    boolean diffControllerTested = false;
    int retry = 0;

    while (!(sharedControllerTested && diffControllerTested) && retry++ < MAX_RETRY) {
      if (NUM_OF_CONTROLLERS == 1) {
        // When NUM_OF_CONTROLLERS == 1, controller will always be shared
        diffControllerTested = true;
      }

      String store0 = TestUtils.getUniqueString("test-store0"); // Store in src cluster
      String store1 = TestUtils.getUniqueString("test-store1"); // Store in dest cluster

      VeniceMultiClusterWrapper multiClusterWrapper = ServiceFactory.getVeniceMultiClusterWrapper(3, NUM_OF_CONTROLLERS, 2, 2);

      String[] clusterNames = multiClusterWrapper.getClusterNames();
      Assert.assertTrue(clusterNames.length >= 2, "For this test there must be at least two clusters");

      Arrays.sort(clusterNames);
      String srcClusterName = clusterNames[0];  // venice-cluster0-XXXXXXXXX
      String destClusterName = clusterNames[1]; // venice-cluster1-XXXXXXXXX

      if (multiClusterWrapper.getMasterController(srcClusterName)
          .getControllerUrl()
          .equals(multiClusterWrapper.getMasterController(destClusterName).getControllerUrl())) {
        if (sharedControllerTested) {
          // tested this case already
          multiClusterWrapper.close();
          continue;
        }
        sharedControllerTested = true;
      } else {
        if (diffControllerTested) {
          // tested this case already
          multiClusterWrapper.close();
          continue;
        }
        diffControllerTested = true;
      }

      Admin randomVeniceAdmin = multiClusterWrapper.getRandomController().getVeniceAdmin();
      Admin srcAdmin = multiClusterWrapper.getMasterController(srcClusterName).getVeniceAdmin();
      Admin destAdmin = multiClusterWrapper.getMasterController(destClusterName).getVeniceAdmin();

      // Create store and add a second schema
      srcAdmin.addStore(srcClusterName, store0, "tester", "\"string\"", valueSchema1);
      srcAdmin.addValueSchema(srcClusterName, store0, valueSchema2);
      destAdmin.addStore(destClusterName, store1, "tester", "\"string\"", valueSchema1);
      destAdmin.addValueSchema(destClusterName, store1, valueSchema2);
      Assert.assertEquals(randomVeniceAdmin.discoverCluster(store0).getFirst(), srcClusterName);
      Assert.assertEquals(randomVeniceAdmin.discoverCluster(store1).getFirst(), destClusterName);

      // Copy store0 from src to dest
      String destRouterUrl = multiClusterWrapper.getClusters().get(destClusterName).getRandomRouterURL();
      ControllerClient destControllerClient = new ControllerClient(destClusterName, destRouterUrl);
      StoreMigrationResponse storeMigrationResponse = destControllerClient.migrateStore(store0, srcClusterName);
      Assert.assertFalse(storeMigrationResponse.isError(), storeMigrationResponse.getError());

      // Compare schemas
      Assert.assertEquals(destAdmin.getKeySchema(destClusterName, store0).getSchema().toString(),
          srcAdmin.getKeySchema(srcClusterName, store0).getSchema().toString());
      Assert.assertEquals(destAdmin.getValueSchemas(destClusterName, store0),
          srcAdmin.getValueSchemas(srcClusterName, store0));

      multiClusterWrapper.close();
    }
  }

  @Test
  public void testDataMigration() {
    boolean sharedControllerTested = false;
    boolean diffControllerTested = false;
    int retry = 0;

    while (!(sharedControllerTested && diffControllerTested) && retry++ < MAX_RETRY) {
      if (NUM_OF_CONTROLLERS == 1) {
        // When NUM_OF_CONTROLLERS == 1, controller will always be shared
        diffControllerTested = true;
      }
      String store0 = TestUtils.getUniqueString("test-store0"); // Store in src cluster
      String store1 = TestUtils.getUniqueString("test-store1"); // Store in dest cluster

      VeniceMultiClusterWrapper multiClusterWrapper = ServiceFactory.getVeniceMultiClusterWrapper(3, NUM_OF_CONTROLLERS, 2, 2);
      String[] clusterNames = multiClusterWrapper.getClusterNames();
      Assert.assertTrue(clusterNames.length >= 2, "For this test there must be at least two clusters");

      Arrays.sort(clusterNames);
      String srcClusterName = clusterNames[0];  // venice-cluster0-XXXXXXXXX
      String destClusterName = clusterNames[1]; // venice-cluster1-XXXXXXXXX

      Admin randomVeniceAdmin = multiClusterWrapper.getRandomController().getVeniceAdmin();
      Admin srcAdmin = multiClusterWrapper.getMasterController(srcClusterName).getVeniceAdmin();
      Admin destAdmin = multiClusterWrapper.getMasterController(destClusterName).getVeniceAdmin();

      if (multiClusterWrapper.getMasterController(srcClusterName)
          .getControllerUrl()
          .equals(multiClusterWrapper.getMasterController(destClusterName).getControllerUrl())) {
        if (sharedControllerTested) {
          // tested this case already
          multiClusterWrapper.close();
          continue;
        }
        sharedControllerTested = true;
      } else {
        if (diffControllerTested) {
          // tested this case already
          multiClusterWrapper.close();
          continue;
        }
        diffControllerTested = true;
      }

      // Create one store in each cluster
      srcAdmin.addStore(srcClusterName, store0, "tester", "\"string\"", "\"string\"");
      srcAdmin.updateStore(srcClusterName, store0, new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA));
      destAdmin.addStore(destClusterName, store1, "tester", "\"string\"", "\"string\"");
      destAdmin.updateStore(destClusterName, store1, new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA));
      Assert.assertEquals(randomVeniceAdmin.discoverCluster(store0).getFirst(), srcClusterName);
      Assert.assertEquals(randomVeniceAdmin.discoverCluster(store1).getFirst(), destClusterName);

      String srcRouterUrl = multiClusterWrapper.getClusters().get(srcClusterName).getRandomRouterURL();
      String destRouterUrl = multiClusterWrapper.getClusters().get(destClusterName).getRandomRouterURL();
      ControllerClient srcControllerClient = new ControllerClient(srcClusterName, srcRouterUrl);
      ControllerClient destControllerClient = new ControllerClient(destClusterName, destRouterUrl);

      // Populate store0
      populateStore(srcRouterUrl, srcClusterName, store0, 1);
      populateStore(srcRouterUrl, srcClusterName, store0, 2);
      Assert.assertTrue(srcAdmin.getStore(srcClusterName, store0).containsVersion(1));
      Assert.assertTrue(srcAdmin.getStore(srcClusterName, store0).containsVersion(2));
      Assert.assertEquals(srcAdmin.getStore(srcClusterName, store0).getCurrentVersion(), 2);

      // Move store0 from src to dest
      moveStoreOneWay(store0, srcClusterName, srcControllerClient, destControllerClient);
      Assert.assertFalse(srcAdmin.hasStore(srcClusterName, store0));
      Assert.assertTrue(destAdmin.hasStore(destClusterName, store0));
      Assert.assertTrue(destAdmin.getStore(destClusterName, store0).containsVersion(2));
      Assert.assertEquals(destAdmin.getStore(destClusterName, store0).getCurrentVersion(), 2);
      verifyStoreData(store0, destRouterUrl);
      Assert.assertEquals(randomVeniceAdmin.discoverCluster(store0).getFirst(), destClusterName);
      Assert.assertEquals(randomVeniceAdmin.discoverCluster(store1).getFirst(), destClusterName);

      // Move store0 back to src
      moveStoreOneWay(store0, destClusterName, destControllerClient, srcControllerClient);
      Assert.assertFalse(destAdmin.hasStore(destClusterName, store0));
      Assert.assertTrue(srcAdmin.hasStore(srcClusterName, store0));
      Assert.assertTrue(srcAdmin.getStore(srcClusterName, store0).containsVersion(2));
      Assert.assertEquals(srcAdmin.getStore(srcClusterName, store0).getCurrentVersion(), 2);
      verifyStoreData(store0, srcRouterUrl);
      Assert.assertEquals(randomVeniceAdmin.discoverCluster(store0).getFirst(), srcClusterName);
      Assert.assertEquals(randomVeniceAdmin.discoverCluster(store1).getFirst(), destClusterName);

      // Move store0 back and forth multiple times
      moveStoreBackAndForth(store0, srcClusterName, destClusterName, srcControllerClient, destControllerClient, 3);
      Assert.assertFalse(destAdmin.hasStore(destClusterName, store0));
      Assert.assertTrue(srcAdmin.hasStore(srcClusterName, store0));
      Assert.assertTrue(srcAdmin.getStore(srcClusterName, store0).containsVersion(2));
      Assert.assertEquals(srcAdmin.getStore(srcClusterName, store0).getCurrentVersion(), 2);
      verifyStoreData(store0, srcRouterUrl);
      Assert.assertEquals(randomVeniceAdmin.discoverCluster(store0).getFirst(), srcClusterName);
      Assert.assertEquals(randomVeniceAdmin.discoverCluster(store1).getFirst(), destClusterName);

      // Add one more version to store0 then test again
      populateStore(srcRouterUrl, srcClusterName, store0, 3);
      Assert.assertFalse(destAdmin.hasStore(destClusterName, store0));
      Assert.assertTrue(srcAdmin.hasStore(srcClusterName, store0));
      Assert.assertTrue(srcAdmin.getStore(srcClusterName, store0).containsVersion(3));
      Assert.assertEquals(srcAdmin.getStore(srcClusterName, store0).getCurrentVersion(), 3);
      moveStoreBackAndForth(store0, srcClusterName, destClusterName, srcControllerClient, destControllerClient, 2);
      Assert.assertFalse(destAdmin.hasStore(destClusterName, store0));
      Assert.assertTrue(srcAdmin.hasStore(srcClusterName, store0));
      Assert.assertTrue(srcAdmin.getStore(srcClusterName, store0).containsVersion(3));
      Assert.assertEquals(srcAdmin.getStore(srcClusterName, store0).getCurrentVersion(), 3);
      verifyStoreData(store0, srcRouterUrl);
      Assert.assertEquals(randomVeniceAdmin.discoverCluster(store0).getFirst(), srcClusterName);
      Assert.assertEquals(randomVeniceAdmin.discoverCluster(store1).getFirst(), destClusterName);

      // Test backward: push to store1 in dest then move to src
      populateStore(destRouterUrl, destClusterName, store1, 1);
      Assert.assertFalse(srcAdmin.hasStore(srcClusterName, store1));
      Assert.assertTrue(destAdmin.hasStore(destClusterName, store1));
      Assert.assertTrue(destAdmin.getStore(destClusterName, store1).containsVersion(1));
      Assert.assertEquals(destAdmin.getStore(destClusterName, store1).getCurrentVersion(), 1);
      Assert.assertEquals(srcAdmin.getStore(srcClusterName, store0).getCurrentVersion(), 3);
      moveStoreOneWay(store1, destClusterName, destControllerClient, srcControllerClient);
      Assert.assertFalse(destAdmin.hasStore(destClusterName, store1));
      Assert.assertTrue(srcAdmin.hasStore(srcClusterName, store1));
      Assert.assertTrue(srcAdmin.getStore(srcClusterName, store1).containsVersion(1));
      Assert.assertEquals(srcAdmin.getStore(srcClusterName, store1).getCurrentVersion(), 1);
      Assert.assertEquals(srcAdmin.getStore(srcClusterName, store0).getCurrentVersion(), 3);
      verifyStoreData(store1, srcRouterUrl);
      Assert.assertEquals(randomVeniceAdmin.discoverCluster(store0).getFirst(), srcClusterName);
      Assert.assertEquals(randomVeniceAdmin.discoverCluster(store1).getFirst(), srcClusterName);

      // Finally move everything to dest
      // Move store0 to dest
      moveStoreOneWay(store0, srcClusterName, srcControllerClient, destControllerClient);
      Assert.assertFalse(srcAdmin.hasStore(srcClusterName, store0));
      Assert.assertTrue(destAdmin.hasStore(destClusterName, store0));
      Assert.assertFalse(destAdmin.hasStore(destClusterName, store1));
      Assert.assertTrue(srcAdmin.hasStore(srcClusterName, store1));
      Assert.assertTrue(destAdmin.getStore(destClusterName, store0).containsVersion(3));
      Assert.assertEquals(destAdmin.getStore(destClusterName, store0).getCurrentVersion(), 3);
      verifyStoreData(store0, destRouterUrl);
      Assert.assertEquals(randomVeniceAdmin.discoverCluster(store0).getFirst(), destClusterName);
      Assert.assertEquals(randomVeniceAdmin.discoverCluster(store1).getFirst(), srcClusterName);

      // Move store1
      moveStoreOneWay(store1, srcClusterName, srcControllerClient, destControllerClient);
      Assert.assertFalse(srcAdmin.hasStore(srcClusterName, store0));
      Assert.assertFalse(srcAdmin.hasStore(srcClusterName, store1));
      Assert.assertTrue(destAdmin.hasStore(destClusterName, store0));
      Assert.assertTrue(destAdmin.hasStore(destClusterName, store1));
      Assert.assertTrue(destAdmin.getStore(destClusterName, store0).containsVersion(3));
      Assert.assertTrue(destAdmin.getStore(destClusterName, store1).containsVersion(1));
      Assert.assertEquals(destAdmin.getStore(destClusterName, store0).getCurrentVersion(), 3);
      Assert.assertEquals(destAdmin.getStore(destClusterName, store1).getCurrentVersion(), 1);
      verifyStoreData(store1, destRouterUrl);
      Assert.assertEquals(randomVeniceAdmin.discoverCluster(store0).getFirst(), destClusterName);
      Assert.assertEquals(randomVeniceAdmin.discoverCluster(store1).getFirst(), destClusterName);

      // Push another version, make sure everything works
      populateStore(destRouterUrl, destClusterName, store0, 4);
      populateStore(destRouterUrl, destClusterName, store1, 2);
      Assert.assertTrue(destAdmin.getStore(destClusterName, store0).containsVersion(4));
      Assert.assertTrue(destAdmin.getStore(destClusterName, store1).containsVersion(2));
      Assert.assertEquals(destAdmin.getStore(destClusterName, store0).getCurrentVersion(), 4);
      Assert.assertEquals(destAdmin.getStore(destClusterName, store1).getCurrentVersion(), 2);

      multiClusterWrapper.close();
    }
  }

  @Test
  public void testTopicDeletion() {
    boolean sharedControllerTested = false;
    boolean diffControllerTested = false;
    int retry = 0;

    while (!(sharedControllerTested && diffControllerTested) && retry++ < MAX_RETRY) {
      if (NUM_OF_CONTROLLERS == 1) {
        // When numOfControllers == 1, controller will always be shared
        diffControllerTested = true;
      }
        String store0 = TestUtils.getUniqueString("test-store0"); // Store in src cluster
        String store1 = TestUtils.getUniqueString("test-store1"); // Store in dest cluster

      VeniceMultiClusterWrapper multiClusterWrapper = ServiceFactory.getVeniceMultiClusterWrapper(3, NUM_OF_CONTROLLERS, 2, 2);
      String[] clusterNames = multiClusterWrapper.getClusterNames();
      Assert.assertTrue(clusterNames.length >= 2, "For this test there must be at least two clusters");

      Arrays.sort(clusterNames);
      String srcClusterName = clusterNames[0];  // venice-cluster0-XXXXXXXXX
      String destClusterName = clusterNames[1]; // venice-cluster1-XXXXXXXXX

      VeniceControllerWrapper randomController = multiClusterWrapper.getRandomController();
      String randomControllerUrl = randomController.getControllerUrl();
      Admin randomVeniceAdmin = randomController.getVeniceAdmin();
      Admin srcAdmin = multiClusterWrapper.getMasterController(srcClusterName).getVeniceAdmin();
      Admin destAdmin = multiClusterWrapper.getMasterController(destClusterName).getVeniceAdmin();

      if (multiClusterWrapper.getMasterController(srcClusterName)
          .getControllerUrl()
          .equals(multiClusterWrapper.getMasterController(destClusterName).getControllerUrl())) {
        if (sharedControllerTested) {
          // tested this case already
          multiClusterWrapper.close();
          continue;
        }
        sharedControllerTested = true;
      } else {
        if (diffControllerTested) {
          // tested this case already
          multiClusterWrapper.close();
          continue;
        }
        diffControllerTested = true;
      }

      // Create one store in each cluster
      srcAdmin.addStore(srcClusterName, store0, "tester", "\"string\"", "\"string\"");
      srcAdmin.updateStore(srcClusterName, store0, new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA));
      destAdmin.addStore(destClusterName, store1, "tester", "\"string\"", "\"string\"");
      destAdmin.updateStore(destClusterName, store1, new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA));
      Assert.assertEquals(randomVeniceAdmin.discoverCluster(store0).getFirst(), srcClusterName);
      Assert.assertEquals(randomVeniceAdmin.discoverCluster(store1).getFirst(), destClusterName);

      String srcRouterUrl = multiClusterWrapper.getClusters().get(srcClusterName).getRandomRouterURL();
      String destRouterUrl = multiClusterWrapper.getClusters().get(destClusterName).getRandomRouterURL();
      ControllerClient srcControllerClient = new ControllerClient(srcClusterName, srcRouterUrl);
      ControllerClient destControllerClient = new ControllerClient(destClusterName, destRouterUrl);

      // Populate store
      populateStore(srcRouterUrl, srcClusterName, store0, 1);
      populateStore(srcRouterUrl, srcClusterName, store0, 2);
      populateStore(srcRouterUrl, srcClusterName, store0, 3);

      // The first topic should have been deleted, this is the default behavior
      KafkaBrokerWrapper kafka = multiClusterWrapper.getKafkaBrokerWrapper();
      String kafkaAddr = kafka.getHost() + ":" + kafka.getPort();
      Assert.assertFalse(getExistingTopics(kafkaAddr).contains(store0 + "_v1"), "This topic should be deleted");

      // Move store0 from src to dest
      StoreMigrationResponse storeMigrationResponse = destControllerClient.migrateStore(store0, srcClusterName);
      Assert.assertFalse(storeMigrationResponse.isError(), storeMigrationResponse.getError());
      Utils.sleep(3000);

      // Both original and new cluster should be able to serve read traffic
      Assert.assertTrue(srcAdmin.hasStore(srcClusterName, store0));
      Assert.assertTrue(destAdmin.hasStore(destClusterName, store0));
      Assert.assertTrue(srcAdmin.getStore(srcClusterName  , store0).containsVersion(3));
      Assert.assertTrue(destAdmin.getStore(destClusterName, store0).containsVersion(3));
      Assert.assertEquals(srcAdmin.getStore(srcClusterName, store0).getCurrentVersion(), 3);
      Assert.assertEquals(destAdmin.getStore(destClusterName, store0).getCurrentVersion(), 3);
      verifyStoreData(store0, srcRouterUrl);
      verifyStoreData(store0, destRouterUrl);

      // Store discovery should point to the new cluster
      D2ServiceDiscoveryResponse discoveryResponse = destControllerClient.discoverCluster(randomControllerUrl, store0);
      String newCluster = discoveryResponse.getCluster();
      Assert.assertEquals(newCluster, destClusterName);

      // Topic deletion should have been disabled during store migration
      populateStore(destRouterUrl, newCluster, store0, 4);
      Assert.assertTrue(srcAdmin.getStore(srcClusterName, store0).isMigrating());
      Assert.assertTrue(destAdmin.getStore(destClusterName, store0).isMigrating());
      Assert.assertTrue(getExistingTopics(kafkaAddr).contains(store0 + "_v2"), "This topic should not be deleted");
      Assert.assertTrue(getExistingTopics(kafkaAddr).contains(store0 + "_v3"));
      Assert.assertTrue(getExistingTopics(kafkaAddr).contains(store0 + "_v4"));

      // Test one more time
      populateStore(destRouterUrl, newCluster, store0, 5);
      Assert.assertTrue(srcAdmin.getStore(srcClusterName, store0).isMigrating());
      Assert.assertTrue(destAdmin.getStore(destClusterName, store0).isMigrating());
      Assert.assertTrue(getExistingTopics(kafkaAddr).contains(store0 + "_v2"), "This topic should not be deleted");
      Assert.assertTrue(getExistingTopics(kafkaAddr).contains(store0 + "_v3"), "This topic should not be deleted");
      Assert.assertTrue(getExistingTopics(kafkaAddr).contains(store0 + "_v4"));
      Assert.assertTrue(getExistingTopics(kafkaAddr).contains(store0 + "_v5"));

      // Delete old store, but topics should remain
      srcControllerClient.updateStore(store0, new UpdateStoreQueryParams().setEnableReads(false).setEnableWrites(false));
      srcControllerClient.deleteStore(store0);
      Assert.assertFalse(srcAdmin.hasStore(srcClusterName, store0));
      Assert.assertTrue(destAdmin.hasStore(destClusterName, store0));
      Assert.assertTrue(destAdmin.getStore(destClusterName, store0).isMigrating());
      Assert.assertTrue(getExistingTopics(kafkaAddr).contains(store0 + "_v2"), "This topic should not be deleted");
      Assert.assertTrue(getExistingTopics(kafkaAddr).contains(store0 + "_v3"), "This topic should not be deleted");
      Assert.assertTrue(getExistingTopics(kafkaAddr).contains(store0 + "_v4"));
      Assert.assertTrue(getExistingTopics(kafkaAddr).contains(store0 + "_v5"));

      // After migration, reset the flag, push again, and old topics should be deleted
      destControllerClient.updateStore(store0, new UpdateStoreQueryParams().setStoreMigration(false));
      populateStore(destRouterUrl, newCluster, store0, 6);
      Assert.assertFalse(srcAdmin.hasStore(srcClusterName, store0));
      Assert.assertFalse(destAdmin.getStore(destClusterName, store0).isMigrating());
      Assert.assertFalse(getExistingTopics(kafkaAddr).contains(store0 + "_v2"), "This topic should have been deleted");
      Assert.assertFalse(getExistingTopics(kafkaAddr).contains(store0 + "_v3"), "This topic should have been deleted");
      Assert.assertFalse(getExistingTopics(kafkaAddr).contains(store0 + "_v4"), "This topic should have been deleted");
      Assert.assertTrue(getExistingTopics(kafkaAddr).contains(store0 + "_v5"));
      Assert.assertTrue(getExistingTopics(kafkaAddr).contains(store0 + "_v6"));

      // Check store status
      Assert.assertTrue(destAdmin.getStore(destClusterName, store0).containsVersion(6));
      Assert.assertEquals(destAdmin.getStore(destClusterName, store0).getCurrentVersion(), 6);
      verifyStoreData(store0, destRouterUrl);

      multiClusterWrapper.close();
    }
  }

  private void moveStoreBackAndForth(String storeName,
      String srcClusterName,
      String destClusterName,
      ControllerClient srcControllerClient,
      ControllerClient destControllerClient,
      int times) {
    for (int i = 0; i < times; i++) {
      moveStoreOneWay(storeName, srcClusterName, srcControllerClient, destControllerClient);
      moveStoreOneWay(storeName, destClusterName, destControllerClient, srcControllerClient);
    }
  }

  private void moveStoreOneWay(String storeName,
      String srcClusterName,
      ControllerClient srcControllerClient,
      ControllerClient destControllerClient) {
    StoreMigrationResponse storeMigrationResponse = destControllerClient.migrateStore(storeName, srcClusterName);
    Assert.assertFalse(storeMigrationResponse.isError(), storeMigrationResponse.getError());
    Utils.sleep(3000);
    srcControllerClient.updateStore(storeName, new UpdateStoreQueryParams().setEnableReads(false).setEnableWrites(false));
    srcControllerClient.deleteStore(storeName);
  }

  public static void printStoresInClusters(VeniceMultiClusterWrapper multiClusterWrapper) {
    String[] clusters = multiClusterWrapper.getClusterNames();
    Arrays.sort(clusters);

    String msg = "\n============= Cluster to Stores ===============\n";
    for (String cluster : clusters) {
      Admin admin = multiClusterWrapper.getMasterController(cluster).getVeniceAdmin();
      msg += cluster + ": ";
      String storeNames = admin.getAllStores(cluster).stream().map(s -> s.getName()).collect(Collectors.joining(" "));
      msg += storeNames + "\n";
    }
    msg += "===============================================";
    System.out.println(msg);
  }

  public static void printClusterToController(VeniceMultiClusterWrapper multiClusterWrapper) {
    String msg = "\n============ Cluster to Controller ============\n";
    for (VeniceClusterWrapper cluster : multiClusterWrapper.getClusters()
        .values()
        .stream()
        .sorted(Comparator.comparing(VeniceClusterWrapper::getClusterName))
        .toArray(VeniceClusterWrapper[]::new)) {

      String clusterName = cluster.getClusterName();
      String controllerUrl = multiClusterWrapper.getMasterController(clusterName).getControllerUrl();
      msg += clusterName + " : " + controllerUrl + "\n";
    }
    msg += "===============================================";
    System.out.println(msg);
  }


  public static void printMulticlusterInfo(VeniceMultiClusterWrapper multiClusterWrapper) {
    String msg = "\n============= Cluster basic info ================";
    msg += "\nClusters: " + Arrays.toString(multiClusterWrapper.getClusterNames());
    msg += "\nControllers: " + multiClusterWrapper.getControllerConnectString();
    msg += "\nKafka: " + multiClusterWrapper.getKafkaBrokerWrapper().getSSLAddress();
    msg += "\nZK: " + multiClusterWrapper.getZkServerWrapper().getAddress();
    msg += "\n=================================================";
    System.out.println(msg);
  }

  public static Set getExistingTopics(String kafkaAddr) {
    Properties props = new Properties();
    props.setProperty("bootstrap.servers", kafkaAddr);
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

    KafkaConsumer kafkaConsumer = new KafkaConsumer(props);
    return kafkaConsumer.listTopics().keySet();
  }

  public static void printExistingTopics(String kafkaAddr) {
    Set topics = getExistingTopics(kafkaAddr);
    String msg = "\n==============  Existing topics  ==============\n";
    msg += topics;
    msg += "\n===============================================";

    System.out.println(msg);
  }

  public static void populateStore(String routerUrl, String clusterName, String storeName, int expectedVersion) {
    // Push
    try {
      Utils.thisIsLocalhost();

      File inputDir = getTempDataDirectory();
      Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir);
      String inputDirPath = "file:" + inputDir.getAbsolutePath();
      String schema = "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"example.avro\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}]}";
      Properties props = defaultH2VProps(routerUrl, inputDirPath, storeName);
      props.setProperty(KafkaPushJob.PBNJ_ENABLE, "true");
      props.setProperty(KafkaPushJob.PBNJ_ROUTER_URL_PROP, routerUrl);

      KafkaPushJob job = new KafkaPushJob("Test push job", props);
      job.run();

      // Verify job properties
      Assert.assertEquals(job.getKafkaTopic(), Version.composeKafkaTopic(storeName, expectedVersion));
      Assert.assertEquals(job.getInputDirectory(), inputDirPath);
      Assert.assertEquals(job.getFileSchemaString(), schema);
      Assert.assertEquals(job.getKeySchemaString(), STRING_SCHEMA);
      Assert.assertEquals(job.getValueSchemaString(), STRING_SCHEMA);
      Assert.assertEquals(job.getInputFileDataSize(), 3872);

      // Verify the data in Venice Store
      Utils.sleep(3000);
      verifyPushJobStatus(clusterName, routerUrl, job);
      verifyStoreData(storeName, routerUrl);
    } catch (Exception e) {
      logger.error(e);
      throw new VeniceException(e);
    }
  }

  public static void verifyStoreData(String storeName, String routerUrl) {
    try (AvroGenericStoreClient<String, Object> client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {
      for (int i = 1; i <= 100; ++i) {
        String expected = "test_name_" + i;
        String actual = client.get(Integer.toString(i)).get().toString(); /* client.get().get() returns a Utf8 object */
        Assert.assertEquals(actual, expected);
      }
    } catch (Exception e) {
      throw new VeniceException(e);
    }
  }

  public static void verifyPushJobStatus(String clusterName, String routerUrl, KafkaPushJob job) {
    ControllerClient controllerClient = new ControllerClient(clusterName, routerUrl);

    JobStatusQueryResponse jobStatus = controllerClient.queryJobStatus(job.getKafkaTopic());
    Assert.assertEquals(jobStatus.getStatus(), ExecutionStatus.COMPLETED.toString(),
        "After job is complete, status should reflect that");
    // In this test we are allowing the progress to not reach the full capacity, but we still want to make sure
    // that most of the progress has completed
    Assert.assertTrue(jobStatus.getMessagesConsumed() * 1.5 > jobStatus.getMessagesAvailable(),
        "Complete job should have progress");
  }
}

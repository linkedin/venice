package com.linkedin.venice.endToEnd;

import com.linkedin.davinci.client.DaVinciConfig;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.ConstantVenicePartitioner;
import com.linkedin.venice.samza.VeniceSystemFactory;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;

import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.RemoteReadPolicy;

import java.util.function.Consumer;
import org.apache.commons.io.IOUtils;
import org.apache.samza.system.SystemProducer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.*;
import static org.testng.Assert.*;


public class DaVinciClientTest {
  private static final int KEY_COUNT = 10;
  private static final int TEST_TIMEOUT = 60_000; // ms
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
  public void testBatchStore() throws Exception {
    String storeName = cluster.createStore(KEY_COUNT);

    try (DaVinciClient<Integer, Integer> client = ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster)) {
      client.subscribeToAllPartitions().get();
      for (int k = 0; k < KEY_COUNT; ++k) {
        assertEquals(client.get(k).get(), (Integer) 1);
      }

      for (int i = 0; i < 2; ++i) {
        Integer expectedValue = cluster.createVersion(storeName, KEY_COUNT);
        TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
          for (int k = 0; k < KEY_COUNT; ++k) {
            assertEquals(client.get(k).get(), expectedValue);
          }
        });
      }

      // test non-existing key
      assertNull(client.get(KEY_COUNT + 1).get());

      // test read from a store that was deleted concurrently
      try (ControllerClient controllerClient = cluster.getControllerClient()) {
        ControllerResponse response = controllerClient.disableAndDeleteStore(storeName);
        assertFalse(response.isError(), response.getError());

        TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
          assertThrows(VeniceNoStoreException.class, () -> client.get(KEY_COUNT / 3).get());
        });
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testHybridStore() throws Exception {
    final int partition = 1;
    final int partitionCount = 2;
    String storeName = TestUtils.getUniqueString("store");
    Consumer<UpdateStoreQueryParams> paramsConsumer =
        params -> params.setPartitionerClass(ConstantVenicePartitioner.class.getName())
            .setPartitionCount(partitionCount)
            .setPartitionerClass(ConstantVenicePartitioner.class.getName())
            .setPartitionerParams(
                Collections.singletonMap(ConstantVenicePartitioner.CONSTANT_PARTITION, String.valueOf(partition))
            );
    setUpHybridStore(storeName, paramsConsumer);

    try (DaVinciClient<Integer, Integer> client = ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster)) {
      // subscribe to a partition without data
      int emptyPartition = (partition + 1) % partitionCount;
      client.subscribe(Collections.singleton(emptyPartition)).get();
      Utils.sleep(1000);
      for (int i = 0; i < KEY_COUNT; i++) {
        final int key = i;
        assertThrows(VeniceClientException.class, () -> client.get(key).get());
      }
      client.unsubscribe(Collections.singleton(emptyPartition));

      // subscribe to a partition with data
      client.subscribe(Collections.singleton(partition)).get();
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        Set<Integer> keySet = new HashSet<>();
        for (Integer i = 0; i < KEY_COUNT; i++) {
          assertEquals(client.get(i).get(), i);
          keySet.add(i);
        }

        Map<Integer, Integer> valueMap = client.batchGet(keySet).get();
        assertNotNull(valueMap);
        for (Integer i = 0; i < KEY_COUNT; i++) {
          assertEquals(valueMap.get(i), i);
        }
      });
    }
  }

  @Test
  public void testAmplificationFactorInHybridStore() throws Exception {
    final int partition = 1;
    final int partitionCount = 2;
    final int amplificationFactor = 10;
    String storeName = TestUtils.getUniqueString("store");
    Consumer<UpdateStoreQueryParams> paramsConsumer =
        params -> params.setPartitionerClass(ConstantVenicePartitioner.class.getName())
            .setPartitionCount(partitionCount)
            .setPartitionerClass(ConstantVenicePartitioner.class.getName())
            .setAmplificationFactor(amplificationFactor)
            .setPartitionerParams(
                Collections.singletonMap(ConstantVenicePartitioner.CONSTANT_PARTITION, String.valueOf(partition))
            );
    setUpHybridStore(storeName, paramsConsumer);

    try (DaVinciClient<Integer, Integer> client = ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster)) {
      client.subscribe(Collections.singleton(partition)).get();
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        for (Integer i = 0; i < KEY_COUNT; i++) {
          assertEquals(client.get(i).get(), i);
        }
      });

      // unsubscribe to a partition with data
      client.unsubscribe(Collections.singleton(partition));
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        for (Integer i = 0; i < KEY_COUNT; i++) {
          final int key = i;
          assertThrows(VeniceClientException.class, () -> client.get(key).get());
        }
      });

      // unsubscribe not subscribed partitions
      Set<Integer> partitionSet = new HashSet<>();
      for (int i = 0; i < partitionCount; i++) {
        partitionSet.add(i);
      }
      assertThrows(VeniceException.class, () -> client.unsubscribe(partitionSet));

      // re-subscribe to a partition with data to test sub a unsub-ed partition
      client.subscribeToAllPartitions().get();
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        for (Integer i = 0; i < KEY_COUNT; i++) {
          assertEquals(client.get(i).get(), i);
        }
      });

      // test unsubscribe from all partitions
      client.unsubscribeFromAllPartitions();
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        for (Integer i = 0; i < KEY_COUNT; i++) {
          final int key = i;
          assertThrows(VeniceClientException.class, () -> client.get(key).get());
        }
      });
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testBootstrap() throws Exception {
    String storeName = cluster.createStore(KEY_COUNT);
    // Specify db base path for clients.
    String testDataFilePath = TestUtils.getTempDataDirectory().getAbsolutePath();
    try (DaVinciClient<Object, Object> client = ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster, testDataFilePath)) {
      client.subscribeToAllPartitions().get();
      for (int k = 0; k < KEY_COUNT; ++k) {
        assertEquals(client.get(k).get(), 1);
      }
    }

    try (DaVinciClient<Object, Object> client = ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster, testDataFilePath)) {
      for (int k = 0; k < KEY_COUNT; ++k) {
        assertEquals(client.get(k).get(), 1);
      }
    }
    // Create a new version for testing.
    cluster.createVersion(storeName, KEY_COUNT);
    try (DaVinciClient<Object, Object> client = ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster, testDataFilePath)) {
      Assert.assertThrows(VeniceNoStoreException.class, () -> client.get(0).get());
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testNonLocalRead() throws Exception {
    String storeName = cluster.createStore(KEY_COUNT);

    VeniceProperties backendConfig = new PropertyBuilder()
                                         .put(ConfigKeys.DATA_BASE_PATH, TestUtils.getTempDataDirectory().getAbsolutePath())
                                         .put(ConfigKeys.PERSISTENCE_TYPE, PersistenceType.ROCKS_DB)
                                         .build();

    DaVinciConfig daVinciConfig = new DaVinciConfig();
    daVinciConfig.setRemoteReadPolicy(RemoteReadPolicy.QUERY_REMOTELY);

    Set<Object> keySet = new HashSet<>();
    try (DaVinciClient<Object, Object> client = ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster, daVinciConfig, backendConfig)) {
      client.subscribe(Collections.singleton(0)).get();
      // With QUERY_REMOTELY enabled, all key-value pairs should be found.
      for (int k = 0; k < KEY_COUNT; ++k) {
        assertEquals(client.get(k).get(), 1);
        keySet.add(k);
      }

      Map<Object, Object> valueMap = client.batchGet(keySet).get();
      assertNotNull(valueMap);
      for (int k = 0; k < KEY_COUNT; ++k) {
        assertEquals(valueMap.get(k), 1);
      }
    }

    daVinciConfig.setRemoteReadPolicy(RemoteReadPolicy.FAIL_FAST);
    try (DaVinciClient<Object, Object> client = ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster, daVinciConfig, backendConfig)) {
      // We only subscribe to 1/3 of the partitions so some data will not be present locally.
      client.subscribe(Collections.singleton(0)).get();
      Assert.assertThrows(VeniceClientException.class, () -> client.batchGet(keySet).get());
    }

    // Update the store to use non-default partitioner and it shall fail during initialization even with QUERY_REMOTELY enabled.
    try (ControllerClient client = cluster.getControllerClient()) {
      ControllerResponse response = client.updateStore(
          storeName,
          new UpdateStoreQueryParams()
              .setPartitionerClass(ConstantVenicePartitioner.class.getName())
              .setPartitionerParams(
                  Collections.singletonMap(ConstantVenicePartitioner.CONSTANT_PARTITION, String.valueOf(2))
              )
      );
      if (response.isError()) {
        Assert.fail(response.getError());
      }
    }
    daVinciConfig.setRemoteReadPolicy(RemoteReadPolicy.QUERY_REMOTELY);
    Assert.assertThrows(VeniceClientException.class, () -> ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster, daVinciConfig, backendConfig));
  }

  private void setUpHybridStore(String storeName, Consumer<UpdateStoreQueryParams> paramsConsumer) throws Exception {
    UpdateStoreQueryParams params = new UpdateStoreQueryParams()
        .setHybridRewindSeconds(10)
        .setHybridOffsetLagThreshold(10);
    paramsConsumer.accept(params);
    try (ControllerClient client = cluster.getControllerClient()) {
      client.createNewStore(storeName, "owner", DEFAULT_KEY_SCHEMA, DEFAULT_VALUE_SCHEMA);
      client.updateStore(
          storeName,
          params
      );
      cluster.createVersion(storeName, DEFAULT_KEY_SCHEMA, DEFAULT_VALUE_SCHEMA, Stream.of());
      SystemProducer producer = TestPushUtils.getSamzaProducer(cluster, storeName, Version.PushType.STREAM,
          Pair.create(VeniceSystemFactory.VENICE_PARTITIONERS, ConstantVenicePartitioner.class.getName()));
      for (int i = 0; i < KEY_COUNT; i++) {
        TestPushUtils.sendStreamingRecord(producer, storeName, i, i);
      }
      producer.stop();
    }
  }
}

package com.linkedin.venice.endToEnd;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.ConstantVenicePartitioner;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.samza.VeniceSystemFactory;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.TestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;

import com.linkedin.davinci.client.DaVinciClient;

import java.io.File;
import java.util.HashMap;
import java.util.Properties;
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

import static com.linkedin.venice.hadoop.KafkaPushJob.*;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.*;
import static com.linkedin.venice.utils.TestPushUtils.*;
import static org.testng.Assert.*;


public class DaVinciClientTest {
  private static final int KEY_COUNT = 10;
  private static final int TEST_TIMEOUT = 15_000; // ms
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

    try (ControllerClient client = cluster.getControllerClient()) {
      client.createNewStore(storeName, "owner", DEFAULT_KEY_SCHEMA, DEFAULT_VALUE_SCHEMA);
      client.updateStore(
          storeName,
          new UpdateStoreQueryParams()
              .setHybridRewindSeconds(10)
              .setHybridOffsetLagThreshold(10)
              .setPartitionerClass(ConstantVenicePartitioner.class.getName())
              .setPartitionerParams(
                  Collections.singletonMap(ConstantVenicePartitioner.CONSTANT_PARTITION, String.valueOf(partition))
              )
              .setPartitionCount(partitionCount)
      );
      cluster.createVersion(storeName, DEFAULT_KEY_SCHEMA, DEFAULT_VALUE_SCHEMA, Stream.of());
      SystemProducer producer = TestPushUtils.getSamzaProducer(cluster, storeName, Version.PushType.STREAM,
          Pair.create(VeniceSystemFactory.VENICE_PARTITIONERS, ConstantVenicePartitioner.class.getName()));
      for (int i = 0; i < KEY_COUNT; i++) {
        TestPushUtils.sendStreamingRecord(producer, storeName, i, i);
      }
      producer.stop();
    }

    try (DaVinciClient<Integer, Integer> client = ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster)) {
      // subscribe to a partition without data
      int emptyPartition = (partition + 1) % partitionCount;
      client.subscribe(Collections.singleton(emptyPartition)).get();
      Utils.sleep(1000);
      for (int i = 0; i < KEY_COUNT; i++) {
        final int key = i;
        assertThrows(VeniceClientException.class, () -> client.get(key).get());
      }
      client.unsubscribe(Collections.singleton(emptyPartition)).get();

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
}

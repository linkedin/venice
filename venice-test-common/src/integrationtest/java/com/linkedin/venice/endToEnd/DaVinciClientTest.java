package com.linkedin.venice.endToEnd;

import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.ConstantVenicePartitioner;
import com.linkedin.venice.samza.VeniceSystemFactory;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.TestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.samza.system.SystemProducer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.*;
import static org.testng.Assert.*;


public class DaVinciClientTest {
  private static final Logger logger = Logger.getLogger(DaVinciClientTest.class);

  private static final int TEST_TIMEOUT = 15000; // ms
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
  public void testExistingVersionAccess() throws Exception {
    final int keyCount = 10;
    String storeName = cluster.createStore(keyCount);
    try (DaVinciClient<Integer, Integer> client =
             ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster)) {

      Schema.Parser parser = new Schema.Parser();
      Assert.assertEquals(parser.parse(DEFAULT_KEY_SCHEMA), client.getKeySchema());
      Assert.assertEquals(parser.parse(DEFAULT_VALUE_SCHEMA), client.getLatestValueSchema());

      client.subscribeToAllPartitions().get();
      for (int i = 0; i < keyCount; ++i) {
        Object value = client.get(i).get();
        Assert.assertNotNull(value);
        Assert.assertEquals(value, 1);
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testSystemProducer() throws Exception {
    final int partitionCount = 2;
    final int keyCount = 10;
    final int partition = 1;
    String storeName = TestUtils.getUniqueString("store");
    try (ControllerClient client = cluster.getControllerClient()) {
      client.createNewStore(storeName, "owner", DEFAULT_KEY_SCHEMA, DEFAULT_VALUE_SCHEMA);
      client.updateStore(storeName,
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
      for (int i = 0; i < keyCount; i++) {
        TestPushUtils.sendStreamingRecord(producer, storeName, i, i);
      }
      producer.stop();
    }
    try (DaVinciClient<Object, Object> client = ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster)) {
      // subscribe to a partition does not hold any data
      int emptyPartition = (partition + 1) % partitionCount;
      client.subscribe(Collections.singleton(emptyPartition)).get();
      Utils.sleep(1000);
      for (int i = 0; i < keyCount; i++) {
        final int key = i;
        assertThrows(VeniceClientException.class, () -> client.get(key).get());
      }
      client.unsubscribe(Collections.singleton(emptyPartition)).get();

      // subscribe to a partition with data
      client.subscribe(Collections.singleton(partition)).get();
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {

        Set<Object> keySet = new HashSet<>();
        for (int i = 0; i < keyCount; i++) {
          keySet.add(i);
          Object actualValue;
          try {
            actualValue = client.get(i).get();
          } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
          }
          assertEquals(actualValue, i);
        }

        Map<Object, Object> actualValue;
        try {
          actualValue = client.batchGet(keySet).get();
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }

        assertNotNull(actualValue);
        for (int i = 0; i < keyCount; i++) {
          assertEquals(actualValue.get(i), i);
        }
      });
    }
  }
}

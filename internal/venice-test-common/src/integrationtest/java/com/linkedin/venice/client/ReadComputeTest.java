package com.linkedin.venice.client;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.client.store.ComputeGenericRecord;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import java.util.Collections;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ReadComputeTest {
  @Test
  public void testProjection() throws Exception {
    try (VeniceClusterWrapper cluster =
        ServiceFactory.getVeniceCluster(new VeniceClusterCreateOptions.Builder().build())) {
      Schema schema = AvroCompatibilityHelper.parse(
          "{\"name\":\"test_record\",\"type\":\"record\",\"fields\":[{\"name\":\"x\", \"type\":\"int\"}, {\"name\":\"y\", \"type\":\"double\"}, {\"name\":\"z\", \"type\":\"string\"}]}");
      GenericData.Record record = new GenericData.Record(schema);
      record.put("x", 1);
      record.put("y", 2.);
      record.put("z", "3");
      String storeName1 = cluster.createStore(1, record);
      try (AvroGenericStoreClient<Integer, GenericRecord> client = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName1)
              .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
              .setVeniceURL(cluster.getZk().getAddress()))) {
        ComputeGenericRecord result =
            client.compute().project("z").execute(Collections.singleton(0)).get().values().stream().findAny().get();
        Assert.assertNull(result.get("x"));
        Assert.assertNull(result.get("y"));
        Assert.assertEquals(result.get("z").toString(), record.get("z").toString());
      }
    }
  }
}

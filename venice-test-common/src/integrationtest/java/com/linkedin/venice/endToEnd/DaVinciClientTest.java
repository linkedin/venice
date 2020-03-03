package com.linkedin.venice.endToEnd;

import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.utils.Utils;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.*;


public class DaVinciClientTest {
  private static final Logger logger = Logger.getLogger(DaVinciClientTest.class);

  private static final int KEY_COUNT = 10;
  private static final int TEST_TIMEOUT = 15000; // ms
  private String storeName;
  private VeniceClusterWrapper cluster;

  @BeforeClass
  public void setup() throws Exception {
    Utils.thisIsLocalhost();
    cluster = ServiceFactory.getVeniceCluster(1, 1, 1);
    storeName = cluster.createStore(KEY_COUNT);
  }

  @AfterClass
  public void cleanup() {
    IOUtils.closeQuietly(cluster);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testExistingVersionAccess() throws Exception {
    try (DaVinciClient<Object, Object> client =
             ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster)) {

      Schema.Parser parser = new Schema.Parser();
      Assert.assertEquals(parser.parse(DEFAULT_KEY_SCHEMA), client.getKeySchema());
      Assert.assertEquals(parser.parse(DEFAULT_VALUE_SCHEMA), client.getLatestValueSchema());

      client.subscribeToAllPartitions().get(30, TimeUnit.SECONDS);
      for (int i = 0; i < KEY_COUNT; ++i) {
        Object value = client.get(i).get();
        Assert.assertNotNull(value);
        Assert.assertEquals(value, 1);
      }
    }
  }
}

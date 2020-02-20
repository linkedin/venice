package com.linkedin.venice.endToEnd;

import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Utils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class DaVinciClientTest {

  private static final String KEY_SCHEMA =  "\"string\"";
  private static final String VALUE_SCHEMA = "{\"type\": \"record\", \"name\": \"dummy_schema\", \"fields\": []}";

  private final Schema.Parser parser = new Schema.Parser();
  private VeniceClusterWrapper cluster;
  private String storeName;

  @BeforeClass
  public void setup() {
    Utils.thisIsLocalhost();
    cluster = ServiceFactory.getVeniceCluster(1, 1, 1);
    VersionCreationResponse response = cluster.getNewStoreVersion(KEY_SCHEMA, VALUE_SCHEMA);
    storeName = Version.parseStoreFromKafkaTopicName(response.getKafkaTopic());
  }

  @AfterClass
  public void cleanup() {
    IOUtils.closeQuietly(cluster);
  }

  @Test
  public void testStart() {
    try (DaVinciClient<String, GenericRecord> daVinciClient =
        ServiceFactory.getAndStartGenericRecordAvroDaVinciClient(storeName, cluster)) {
      Assert.assertEquals(parser.parse(KEY_SCHEMA), daVinciClient.getKeySchema());
      Assert.assertEquals(parser.parse(VALUE_SCHEMA), daVinciClient.getLatestValueSchema());
    }
  }
}
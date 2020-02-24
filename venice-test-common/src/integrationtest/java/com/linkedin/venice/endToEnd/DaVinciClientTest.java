package com.linkedin.venice.endToEnd;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;

import com.linkedin.davinci.client.DaVinciClient;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class DaVinciClientTest {
  private static final Logger logger = Logger.getLogger(DaVinciClientTest.class);

  private static final int KEY_COUNT = 10;
  private static final int TEST_TIMEOUT = 15000; // ms
  private static final String KEY_SCHEMA =  "\"string\"";
  private static final String VALUE_SCHEMA = "{\"type\": \"record\", \"name\": \"dummy_schema\", \"fields\": []}";

  private String storeName;
  private VeniceClusterWrapper cluster;

  @BeforeClass
  public void setup() throws Exception {
    Utils.thisIsLocalhost();
    cluster = ServiceFactory.getVeniceCluster(1, 1, 1);
    VersionCreationResponse response = cluster.getNewStoreVersion(KEY_SCHEMA, VALUE_SCHEMA);
    storeName = Version.parseStoreFromKafkaTopicName(response.getKafkaTopic());

    TestUtils.VeniceTestWriterFactory writerFactory = TestUtils.getVeniceTestWriterFactory(cluster.getKafka().getAddress());
    try (
        VeniceKafkaSerializer keySerializer = new VeniceAvroKafkaSerializer(KEY_SCHEMA);
        VeniceKafkaSerializer valueSerializer = new VeniceAvroKafkaSerializer(VALUE_SCHEMA);
        VeniceWriter<Object, Object, byte[]> writer = writerFactory.createVeniceWriter(response.getKafkaTopic(), keySerializer, valueSerializer)) {

      GenericRecord record = new GenericData.Record(new Schema.Parser().parse(VALUE_SCHEMA));
      int valueSchemaId = HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID;

      writer.broadcastStartOfPush(Collections.emptyMap());
      for (int i = 0; i < KEY_COUNT; ++i) {
        writer.put(String.valueOf(i), record, valueSchemaId).get();
      }
      writer.broadcastEndOfPush(Collections.emptyMap());
    }

    TestUtils.waitForNonDeterministicCompletion(30, TimeUnit.SECONDS, () -> {
      StoreResponse storeResponse = ControllerClient.getStore(cluster.getAllControllersURLs(), cluster.getClusterName(), storeName);
      if (storeResponse.isError() || storeResponse.getStore().getCurrentVersion() == Store.NON_EXISTING_VERSION) {
        return false;
      }
      cluster.refreshAllRouterMetaData();
      return true;
    });
  }

  @AfterClass
  public void cleanup() {
    IOUtils.closeQuietly(cluster);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testExistingVersionAccess() throws Exception {
    try (DaVinciClient<String, GenericRecord> client =
             ServiceFactory.getAndStartGenericRecordAvroDaVinciClient(storeName, cluster)) {

      Schema.Parser parser = new Schema.Parser();
      Assert.assertEquals(parser.parse(KEY_SCHEMA), client.getKeySchema());
      Assert.assertEquals(parser.parse(VALUE_SCHEMA), client.getLatestValueSchema());

      client.subscribeToAllPartitions().get(30, TimeUnit.SECONDS);
      for (int i = 0; i < KEY_COUNT; ++i) {
        Assert.assertNotNull(client.get(String.valueOf(i)).get());
      }
    }
  }
}

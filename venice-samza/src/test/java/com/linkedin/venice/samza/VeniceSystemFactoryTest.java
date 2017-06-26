package com.linkedin.venice.samza;

import com.linkedin.cfg.impl.Utils;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.AvroStoreClientFactory;
import com.linkedin.venice.controllerapi.ControllerApiConstants;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.utils.TestUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.samza.config.MapConfig;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.venice.samza.VeniceSystemFactory.*;
import static org.testng.Assert.*;


public class VeniceSystemFactoryTest {

  //for a record like: {'string': 'somestring', 'number': 3.14}
  private static final String VALUE_SCHEMA = "{\"fields\":[{\"type\":[\"double\",\"null\"],\"name\":\"number\"},{\"type\":[\"string\",\"null\"],\"name\":\"string\"}],\"type\":\"record\",\"name\":\"testRecord\"}";
  private static final String KEY_SCHEMA = "\"string\"";
  private VeniceClusterWrapper venice;

  @BeforeClass
  private void setUp() {
    venice = ServiceFactory.getVeniceCluster();
  }

  @AfterClass
  private void tearDown() {
    Utils.close(venice);
  }

  /**
   * Write a record using the Samza SystemProducer for Venice, then verify we can read that record.
   */
  @Test
  public void testGetProducer() throws Exception {
    String veniceSystemName = "venice"; //This is the Samza system name for use by the Samza API.

    String storeName = TestUtils.getUniqueString("store");
    ControllerClient client = new ControllerClient(venice.getClusterName(), venice.getRandomRouterURL());
    client.createNewStore(storeName, "owner", KEY_SCHEMA, VALUE_SCHEMA);

    //Configure and create a SystemProducer for Venice
    Map<String, String> samzaConfig = new HashMap<>();
    String configPrefix = SYSTEMS_PREFIX + veniceSystemName + DOT;
    samzaConfig.put(configPrefix + VENICE_PUSH_TYPE, ControllerApiConstants.PushType.BATCH.toString());
    samzaConfig.put(configPrefix + VENICE_URL, venice.getRandomRouterURL());
    samzaConfig.put(configPrefix + VENICE_CLUSTER, venice.getClusterName());
    samzaConfig.put(JOB_ID, "i001");
    VeniceSystemFactory factory = new VeniceSystemFactory();
    SystemProducer veniceProducer = factory.getProducer("venice", new MapConfig(samzaConfig), null);

    //Create an AVRO record
    GenericRecord record = new GenericData.Record(Schema.parse(VALUE_SCHEMA));
    record.put("string", "somestring");
    record.put("number", 3.14);
    OutgoingMessageEnvelope envelope = new OutgoingMessageEnvelope(
        new SystemStream(veniceSystemName, storeName),
        "keystring",
        record);

    //Send the record to Venice using the SystemProducer, and activate the version
    veniceProducer.send(storeName, envelope);
    client.writeEndOfPush(storeName, 1); // presumably we created version 1.
    TestUtils.waitForNonDeterministicCompletion(5, TimeUnit.SECONDS, () -> client.getStore(storeName).getStore().getCurrentVersion() == 1);

    //read the record out of Venice
    AvroGenericStoreClient<String, GenericRecord> storeClient = AvroStoreClientFactory.getAndStartAvroGenericStoreClient(venice.getRandomRouterURL(), storeName);
    GenericRecord recordFromVenice = storeClient.get("keystring").get(1, TimeUnit.SECONDS);

    //verify we got the right record
    assertEquals(recordFromVenice.get("string").toString(), "somestring");
    assertEquals(recordFromVenice.get("number"), 3.14);

    client.close();
    veniceProducer.stop();
  }
}
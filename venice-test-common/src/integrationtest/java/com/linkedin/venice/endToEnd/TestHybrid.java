package com.linkedin.venice.endToEnd;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerApiConstants;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.KafkaPushJob;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.samza.VeniceSystemFactory;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.log4j.Logger;
import org.apache.samza.config.MapConfig;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.venice.samza.VeniceSystemFactory.*;
import static com.linkedin.venice.utils.TestPushUtils.*;


public class TestHybrid {
  private static final Logger logger = Logger.getLogger(TestHybrid.class);

  // This test validates the hybrid batch + streaming semantics and verifies that configured rewind time works as expected.
  // TODO: The test fails if the rewind drops us inside of a DIV segment.  This is because the DIV consumption logic needs to be improved.
  // set `multiDivStream` to false to test rewind inside a DIV segment.  Example stack trace below:

  // 2017-07-17 10:42:34,691 ERROR StoreBufferService$StoreBufferDrainer:122 - Got exception during processing consumer record: ConsumerRecord(topic = hybrid-store-1500313298602-1681399193_v3, partition = 0, offset = 110, LogAppendTime = 1500313354689, checksum = 3095490156, serialized key size = 4, serialized value size = 40, key = KafkaKey(PUT or DELETE, 043132), value = {"messageType": 0, "producerMetadata": {"producerGUID": [116, 105, -116, 64, -16, 121, 71, -8, -113, -126, -17, -63, 56, 91, -108, 2], "segmentNumber": 0, "messageSequenceNumber": 13, "messageTimestamp": 1500313349198}, "payloadUnion": {"schemaId": 1, "putValue": {"bytes": "stream_12"}}})
  // java.lang.IllegalStateException: Cannot initialize a new segment on anything else but a START_OF_SEGMENT control message.
  // at com.linkedin.venice.kafka.validation.ProducerTracker.initializeNewSegment(ProducerTracker.java:193)
  // at com.linkedin.venice.kafka.validation.ProducerTracker.trackSegment(ProducerTracker.java:145)
  // at com.linkedin.venice.kafka.validation.ProducerTracker.addMessage(ProducerTracker.java:93)
  // at com.linkedin.venice.kafka.consumer.StoreIngestionTask.validateMessage(StoreIngestionTask.java:880)
  // at com.linkedin.venice.kafka.consumer.StoreIngestionTask.internalProcessConsumerRecord(StoreIngestionTask.java:822)
  // at com.linkedin.venice.kafka.consumer.StoreIngestionTask.processConsumerRecord(StoreIngestionTask.java:655)
  // at com.linkedin.venice.kafka.consumer.StoreBufferService$StoreBufferDrainer.run(StoreBufferService.java:120)

  @Test
  public void testHybridEndToEnd() throws Exception {
    Utils.thisIsLocalhost(); //required for development certificates which are registered to 'localhost' hostname
    VeniceClusterWrapper venice = ServiceFactory.getVeniceCluster(1,1,1,1, 1000000, true);
    long streamingRewindSeconds = 25L;
    long streamingMessageLag = 2L;
    // this test needs to be able to pass with this set to false
    // because in real-usage the rewind will start replicating a portion of a DIV segment
    // and that portion will NOT begin with a START OF SEGMENT control message.
    boolean multiDivStream = true;

    String storeName = TestUtils.getUniqueString("hybrid-store");
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir); // records 1-100
    Properties h2vProperties = defaultH2VProps(venice, inputDirPath, storeName);

    //Create store and make it a hybrid store
    createStoreForJob(venice, recordSchema, h2vProperties);
    ControllerClient controllerClient = new ControllerClient(venice.getClusterName(), venice.getRandomRouterURL());
    controllerClient.updateStore(storeName, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
        Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(streamingRewindSeconds), Optional.of(streamingMessageLag));

    //Do an H2V push
    runH2V(h2vProperties, 1, controllerClient);

    //Verify some records (note, records 1-100 have been pushed)
    AvroGenericStoreClient client =
        ClientFactory.genericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(venice.getRandomRouterURL()));
    for (int i=1;i<10;i++){
      String key = Integer.toString(i);
      Assert.assertEquals(client.get(key).get().toString(), "test_name_" + key);
    }

    //write streaming records
    SystemProducer veniceProducer = getSamzaProducer(venice);
    for (int i=1; i<=10; i++) {
      sendStreamingRecord(veniceProducer, storeName, i);
    }
    if (multiDivStream) {
      veniceProducer.stop(); //close out the DIV segment
    }

    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      try {
        Assert.assertEquals(client.get("2").get().toString(),"stream_2");
      } catch (Exception e) {
        throw new VeniceException(e);
      }
    });

    runH2V(h2vProperties, 2, controllerClient);

    // Verify streaming record in second version
    Assert.assertEquals(client.get("2").get().toString(),"stream_2");
    Assert.assertEquals(client.get("19").get().toString(), "test_name_19");

    logger.info("***** Sleeping to get outside of rewind time: " + streamingRewindSeconds + " seconds");
    Utils.sleep(TimeUnit.MILLISECONDS.convert(streamingRewindSeconds, TimeUnit.SECONDS));

    // Write more streaming records
    if (multiDivStream) {
      veniceProducer = getSamzaProducer(venice); // new producer, new DIV segment.
    }
    for (int i=10; i<=20; i++) {
      sendStreamingRecord(veniceProducer, storeName, i);
    }
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      try {
        Assert.assertEquals(client.get("19").get().toString(),"stream_19");
      } catch (Exception e) {
        throw new VeniceException(e);
      }
    });

    // Run H2V a third Time
    runH2V(h2vProperties, 3, controllerClient);

    // Verify new streaming record in third version
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      try {
        Assert.assertEquals(client.get("19").get().toString(),"stream_19");
      } catch (Exception e) {
        throw new VeniceException(e);
      }
    });
    // But not old streaming record (because we waited the rewind time)
    Assert.assertEquals(client.get("2").get().toString(),"test_name_2");

    StoreResponse storeResponse = controllerClient.getStore(storeName);
    List<Integer> versions = storeResponse.getStore().getVersions()
        .stream().map(version -> version.getNumber()).collect(Collectors.toList());

    Assert.assertFalse(versions.contains(1), "After version 3 comes online, version 1 should be retired");
    Assert.assertTrue(versions.contains(2));
    Assert.assertTrue(versions.contains(3));


    veniceProducer.stop();
    venice.close();

  }

  /**
   * Blocking, waits for new version to go online
   */
  private static void runH2V(Properties h2vProperties, int expectedVersionNumber, ControllerClient controllerClient) throws Exception {
    long h2vStart = System.currentTimeMillis();
    String jobName = TestUtils.getUniqueString("hybrid-job-" + expectedVersionNumber);
    KafkaPushJob job = new KafkaPushJob(jobName, h2vProperties);
    job.run();
    TestUtils.waitForNonDeterministicCompletion(5, TimeUnit.SECONDS,
        () -> controllerClient.getStore((String) h2vProperties.get(KafkaPushJob.VENICE_STORE_NAME_PROP))
            .getStore().getCurrentVersion() == expectedVersionNumber);
    logger.info("**TIME** H2V" + expectedVersionNumber + " takes " + (System.currentTimeMillis() - h2vStart));
  }

  private static SystemProducer getSamzaProducer(VeniceClusterWrapper venice){
    Map<String, String> samzaConfig = new HashMap<>();
    String configPrefix = SYSTEMS_PREFIX + "venice" + DOT;
    samzaConfig.put(configPrefix + VENICE_PUSH_TYPE, ControllerApiConstants.PushType.STREAM.toString());
    samzaConfig.put(configPrefix + VENICE_URL, venice.getRandomRouterURL());
    samzaConfig.put(configPrefix + VENICE_CLUSTER, venice.getClusterName());
    samzaConfig.put(JOB_ID, TestUtils.getUniqueString("venice-push-id"));
    VeniceSystemFactory factory = new VeniceSystemFactory();
    SystemProducer veniceProducer = factory.getProducer("venice", new MapConfig(samzaConfig), null);
    return veniceProducer;
  }

  private static void sendStreamingRecord(SystemProducer producer, String storeName, int recordId){
    //Send an AVRO record
    OutgoingMessageEnvelope envelope = new OutgoingMessageEnvelope(
        new SystemStream("venice", storeName),
        Integer.toString(recordId),
        "stream_" + recordId);
    producer.send(storeName, envelope);
  }

}

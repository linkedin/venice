package com.linkedin.venice.pushstatushelper;

import static com.linkedin.venice.common.PushStatusStoreUtils.SERVER_INCREMENTAL_PUSH_PREFIX;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.common.PushStatusStoreUtils;
import com.linkedin.venice.logger.TestLogAppender;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.pushstatus.PushStatusKey;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.update.UpdateBuilder;
import com.linkedin.venice.writer.update.UpdateBuilderImpl;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class PushStatusStoreWriterTest {
  private PushStatusStoreVeniceWriterCache veniceWriterCacheMock;
  private VeniceWriter veniceWriterMock;
  private PushStatusStoreWriter pushStatusStoreWriter;
  private static final String instanceName = "instanceX";
  private static final String storeName = "venice-test-push-status-store";
  private static final int storeVersion = 42;
  private static final String incPushVersion = "inc_push_test_version_1";
  private static final int derivedSchemaId = 1;
  private static final int valueSchemaId =
      AvroProtocolDefinition.PUSH_STATUS_SYSTEM_SCHEMA_STORE.getCurrentProtocolVersion();
  private static final Schema valueSchema =
      AvroProtocolDefinition.PUSH_STATUS_SYSTEM_SCHEMA_STORE.getCurrentProtocolVersionSchema();
  private static final Schema updateSchema =
      WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchema);

  @BeforeMethod
  public void setUp() {
    veniceWriterCacheMock = mock(PushStatusStoreVeniceWriterCache.class);
    veniceWriterMock = mock(VeniceWriter.class);
    pushStatusStoreWriter =
        new PushStatusStoreWriter(veniceWriterCacheMock, instanceName, valueSchemaId, derivedSchemaId, updateSchema);
    when(veniceWriterCacheMock.prepareVeniceWriter(storeName)).thenReturn(veniceWriterMock);
  }

  private GenericRecord getAddIncrementalPushUpdateRecord() {
    UpdateBuilder updateBuilder = new UpdateBuilderImpl(updateSchema);
    updateBuilder.setEntriesToAddToMapField(
        "instances",
        Collections.singletonMap(incPushVersion, START_OF_INCREMENTAL_PUSH_RECEIVED.getValue()));
    return updateBuilder.build();
  }

  private GenericRecord getRemoveIncrementalPushUpdateRecord() {
    UpdateBuilder updateBuilder = new UpdateBuilderImpl(updateSchema);
    updateBuilder.setKeysToRemoveFromMapField("instances", Collections.singletonList(incPushVersion));
    return updateBuilder.build();
  }

  private GenericRecord getHeartbeatRecord(long heartbeatTimestamp) {
    UpdateBuilder updateBuilder = new UpdateBuilderImpl(updateSchema);
    updateBuilder.setNewFieldValue("reportTimestamp", heartbeatTimestamp);
    return updateBuilder.build();

  }

  @Test(description = "Expect an update call for adding current inc-push to ongoing-inc-pushes when status is SOIP")
  public void testWritePushStatusWhenStatusIsSOIP() {
    PushStatusKey serverPushStatusKey = PushStatusStoreUtils
        .getServerIncrementalPushKey(storeVersion, 1, incPushVersion, SERVER_INCREMENTAL_PUSH_PREFIX);
    PushStatusKey ongoPushStatusKey = PushStatusStoreUtils.getOngoingIncrementalPushStatusesKey(storeVersion);

    pushStatusStoreWriter.writePushStatus(
        storeName,
        storeVersion,
        1,
        START_OF_INCREMENTAL_PUSH_RECEIVED,
        Optional.of(incPushVersion),
        Optional.of(SERVER_INCREMENTAL_PUSH_PREFIX));

    verify(veniceWriterMock).update(eq(serverPushStatusKey), any(), eq(valueSchemaId), eq(derivedSchemaId), eq(null));
    verify(veniceWriterCacheMock, times(2)).prepareVeniceWriter(storeName);
    verify(veniceWriterMock).update(
        eq(ongoPushStatusKey),
        eq(getAddIncrementalPushUpdateRecord()),
        eq(valueSchemaId),
        eq(derivedSchemaId),
        eq(null));
  }

  @Test
  public void testAddToSupposedlyOngoingIncrementalPushVersions() {
    PushStatusKey statusKey = PushStatusStoreUtils.getOngoingIncrementalPushStatusesKey(storeVersion);
    pushStatusStoreWriter.addToSupposedlyOngoingIncrementalPushVersions(
        storeName,
        storeVersion,
        incPushVersion,
        START_OF_INCREMENTAL_PUSH_RECEIVED);
    verify(veniceWriterCacheMock).prepareVeniceWriter(storeName);
    verify(veniceWriterMock).update(
        eq(statusKey),
        eq(getAddIncrementalPushUpdateRecord()),
        eq(valueSchemaId),
        eq(derivedSchemaId),
        eq(null));
  }

  @Test
  public void testRemoveFromOngoingIncrementalPushVersions() {
    PushStatusKey statusKey = PushStatusStoreUtils.getOngoingIncrementalPushStatusesKey(storeVersion);
    pushStatusStoreWriter.removeFromSupposedlyOngoingIncrementalPushVersions(storeName, storeVersion, incPushVersion);
    verify(veniceWriterCacheMock).prepareVeniceWriter(storeName);
    verify(veniceWriterMock).update(
        eq(statusKey),
        eq(getRemoveIncrementalPushUpdateRecord()),
        eq(valueSchemaId),
        eq(derivedSchemaId),
        eq(null));
  }

  @Test
  public void testDeletePushStatus() {
    int partitionCount = 4;
    pushStatusStoreWriter.deletePushStatus(storeName, storeVersion, Optional.empty(), partitionCount);
    verify(veniceWriterCacheMock).prepareVeniceWriter(storeName);
    for (int i = 0; i < partitionCount; i++) {
      PushStatusKey statusKey = PushStatusStoreUtils.getPushKey(storeVersion, 0, Optional.empty());
      verify(veniceWriterMock).delete(eq(statusKey), eq(null));
    }
  }

  @Test
  public void testWriteHeartbeat() {
    long heartbeat = 12345L;
    PushStatusKey statusKey = PushStatusStoreUtils.getHeartbeatKey(instanceName);
    pushStatusStoreWriter.writeHeartbeat(storeName, heartbeat);
    verify(veniceWriterCacheMock).prepareVeniceWriter(storeName);
    verify(veniceWriterMock)
        .update(eq(statusKey), eq(getHeartbeatRecord(heartbeat)), eq(valueSchemaId), eq(derivedSchemaId), eq(null));
  }

  @Test
  public void testWriteHeartbeatForBootstrappingInstance() {
    PushStatusKey statusKey = PushStatusStoreUtils.getHeartbeatKey(instanceName);
    pushStatusStoreWriter.writeHeartbeatForBootstrappingInstance(storeName);
    verify(veniceWriterCacheMock).prepareVeniceWriter(storeName);
    verify(veniceWriterMock)
        .update(eq(statusKey), eq(getHeartbeatRecord(-1)), eq(valueSchemaId), eq(derivedSchemaId), eq(null));
  }

  @Test
  public void testPushStatusStoreWriterLogging() throws Exception {
    // Add the test appender to the class
    TestLogAppender testLogAppender = new TestLogAppender("TestLogAppender", PatternLayout.createDefaultLayout());
    testLogAppender.start();

    // Add appender to the logger
    Logger logger = (Logger) LogManager.getLogger(PushStatusStoreWriter.class);
    logger.addAppender(testLogAppender);

    // Mock the produce callback behavior
    Field callbackField = PushStatusStoreWriter.class.getDeclaredField("PUSH_STATUS_UPDATE_LOGGER_CALLBACK");
    callbackField.setAccessible(true);
    PubSubProducerCallback callback = (PubSubProducerCallback) callbackField.get(null);

    // Mock the produce result
    PubSubProduceResult produceResult = Mockito.mock(PubSubProduceResult.class);
    Mockito.when(produceResult.getTopic()).thenReturn("test-topic");
    Mockito.when(produceResult.getOffset()).thenReturn(12345L);

    // Test successful completion path
    callback.onCompletion(produceResult, null);

    // Verify logging
    String capturedLog = testLogAppender.getLog();
    Assert.assertTrue(capturedLog.contains("Updated push status into topic test-topic at offset 12345"));

    // Test failure path
    Exception exception = new Exception("Test error");
    callback.onCompletion(produceResult, exception);

    // Verify logging
    capturedLog = testLogAppender.getLog();
    Assert.assertTrue(capturedLog.contains("Failed to update push status"));
  }
}

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
import com.linkedin.venice.pushstatus.NoOp;
import com.linkedin.venice.pushstatus.PushStatusKey;
import com.linkedin.venice.pushstatus.PushStatusValueWriteOpRecord;
import com.linkedin.venice.pushstatus.instancesMapOps;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.writer.VeniceWriter;
import java.util.Collections;
import java.util.Optional;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class PushStatusStoreWriterTest {
  private PushStatusStoreVeniceWriterCache veniceWriterCacheMock;
  private VeniceWriter veniceWriterMock;
  private PushStatusStoreWriter pushStatusStoreWriter;
  private final static String storeName = "venice-test-push-status-store";
  private final static int storeVersion = 42;
  private final static String incPushVersion = "inc_push_test_version_1";
  private final static int derivedSchemaId = 42;
  private final static int protoVersion =
      AvroProtocolDefinition.PUSH_STATUS_SYSTEM_SCHEMA_STORE.getCurrentProtocolVersion();

  @BeforeMethod
  public void setUp() {
    veniceWriterCacheMock = mock(PushStatusStoreVeniceWriterCache.class);
    veniceWriterMock = mock(VeniceWriter.class);
    pushStatusStoreWriter = new PushStatusStoreWriter(veniceWriterCacheMock, "instanceX", derivedSchemaId);

    when(veniceWriterCacheMock.prepareVeniceWriter(storeName)).thenReturn(veniceWriterMock);
  }

  private PushStatusValueWriteOpRecord getWriteComputeRecord() {
    PushStatusValueWriteOpRecord writeOpRecord = new PushStatusValueWriteOpRecord();
    instancesMapOps instances = new instancesMapOps();
    instances.mapDiff = Collections.emptyList();
    instances.mapUnion = Collections.singletonMap(incPushVersion, START_OF_INCREMENTAL_PUSH_RECEIVED.getValue());
    writeOpRecord.instances = instances;
    writeOpRecord.reportTimestamp = new NoOp();
    return writeOpRecord;
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

    verify(veniceWriterMock).update(eq(serverPushStatusKey), any(), eq(protoVersion), eq(derivedSchemaId), eq(null));
    verify(veniceWriterCacheMock, times(2)).prepareVeniceWriter(storeName);
    verify(veniceWriterMock)
        .update(eq(ongoPushStatusKey), eq(getWriteComputeRecord()), eq(protoVersion), eq(derivedSchemaId), eq(null));
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
    verify(veniceWriterMock)
        .update(eq(statusKey), eq(getWriteComputeRecord()), eq(protoVersion), eq(derivedSchemaId), eq(null));
  }

  @Test
  public void testRemoveFromOngoingIncrementalPushVersions() {
    PushStatusKey statusKey = PushStatusStoreUtils.getOngoingIncrementalPushStatusesKey(storeVersion);
    PushStatusValueWriteOpRecord writeOpRecord = new PushStatusValueWriteOpRecord();
    instancesMapOps instances = new instancesMapOps();
    instances.mapDiff = Collections.singletonList(incPushVersion);
    instances.mapUnion = Collections.emptyMap();
    writeOpRecord.instances = instances;
    writeOpRecord.reportTimestamp = new NoOp();

    pushStatusStoreWriter.removeFromSupposedlyOngoingIncrementalPushVersions(storeName, storeVersion, incPushVersion);
    verify(veniceWriterCacheMock).prepareVeniceWriter(storeName);
    verify(veniceWriterMock).update(eq(statusKey), eq(writeOpRecord), eq(protoVersion), eq(derivedSchemaId), eq(null));
  }
}

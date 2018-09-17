package com.linkedin.venice.router.api.path;

import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.api.RouterKey;
import com.linkedin.venice.utils.MockTime;
import com.linkedin.venice.utils.Time;
import java.util.Collection;
import javax.annotation.Nonnull;
import org.apache.http.client.methods.HttpUriRequest;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class TestVenicePath {
  private static final int SMART_LONG_TAIL_RETRY_ABORT_THRESHOLD_MS = 20;
  private static final String STORAGE_NODE1 = "s1";
  private static final String STORAGE_NODE2 = "s2";

  private static class SmartRetryVenicePath extends VenicePath {
    public SmartRetryVenicePath(Time time) {
      super("fake_resource_v1", true, SMART_LONG_TAIL_RETRY_ABORT_THRESHOLD_MS, time);
    }

    @Override
    public RequestType getRequestType() {
      return null;
    }

    @Override
    public VenicePath substitutePartitionKey(RouterKey s) {
      return null;
    }

    @Override
    public VenicePath substitutePartitionKey(@Nonnull Collection<RouterKey> s) {
      return null;
    }

    @Override
    public HttpUriRequest composeRouterRequest(String storageNodeUri) {
      return null;
    }

    @Nonnull
    @Override
    public String getLocation() {
      return null;
    }
  }

  @Test
  public void testRetryAbortBecauseOfTimeConstraint() {
    MockTime time = new MockTime();
    time.setTime(1);
    SmartRetryVenicePath orgPath = new SmartRetryVenicePath(time);
    orgPath.setLongTailRetryThresholdMs(20);
    assertTrue(orgPath.canRequestStorageNode(STORAGE_NODE1));
    assertTrue(orgPath.canRequestStorageNode(STORAGE_NODE2));
    orgPath.recordOriginalRequestStartTimestamp();
    orgPath.markStorageNodeAsFast(STORAGE_NODE1);

    SmartRetryVenicePath retryPath = new SmartRetryVenicePath(time);
    retryPath.setRetryRequest();
    retryPath.setupRetryRelatedInfo(orgPath);
    time.sleep(SMART_LONG_TAIL_RETRY_ABORT_THRESHOLD_MS + orgPath.getLongTailRetryThresholdMs() + 1);
    assertTrue(retryPath.canRequestStorageNode(STORAGE_NODE1));
    assertTrue(retryPath.isRetryRequestTooLate());
  }

  @Test
  public void testRetryAbortBecauseOfSlowStorageNode() {
    MockTime time = new MockTime();
    time.setTime(1);
    SmartRetryVenicePath orgPath = new SmartRetryVenicePath(time);
    orgPath.setLongTailRetryThresholdMs(20);
    assertTrue(orgPath.canRequestStorageNode(STORAGE_NODE1));
    assertTrue(orgPath.canRequestStorageNode(STORAGE_NODE2));
    orgPath.recordOriginalRequestStartTimestamp();

    SmartRetryVenicePath retryPath = new SmartRetryVenicePath(time);
    retryPath.setRetryRequest();
    retryPath.setupRetryRelatedInfo(orgPath);
    time.sleep(1);
    assertFalse(retryPath.isRetryRequestTooLate());
    assertFalse(retryPath.canRequestStorageNode(STORAGE_NODE1));
  }

  @Test
  public void testRetryLogicWhenMetBothCriterions() {
    MockTime time = new MockTime();
    time.setTime(1);
    SmartRetryVenicePath orgPath = new SmartRetryVenicePath(time);
    orgPath.setLongTailRetryThresholdMs(20);
    assertTrue(orgPath.canRequestStorageNode(STORAGE_NODE1));
    assertTrue(orgPath.canRequestStorageNode(STORAGE_NODE2));
    orgPath.recordOriginalRequestStartTimestamp();
    orgPath.markStorageNodeAsFast(STORAGE_NODE1);

    SmartRetryVenicePath retryPath1 = new SmartRetryVenicePath(time);
    retryPath1.setRetryRequest();
    retryPath1.setupRetryRelatedInfo(orgPath);
    time.sleep(1);
    assertFalse(retryPath1.isRetryRequestTooLate());
    assertTrue(retryPath1.canRequestStorageNode(STORAGE_NODE1));

    // Retry to an unknown storage node
    SmartRetryVenicePath retryPath2 = new SmartRetryVenicePath(time);
    retryPath2.setRetryRequest();
    retryPath2.setupRetryRelatedInfo(orgPath);
    assertFalse(retryPath2.isRetryRequestTooLate());
    assertTrue(retryPath2.canRequestStorageNode(STORAGE_NODE1));
  }
}
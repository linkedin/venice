package com.linkedin.venice.router.api.path;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.meta.RetryManager;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.api.RouterKey;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.utils.TestMockTime;
import com.linkedin.venice.utils.Time;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collection;
import javax.annotation.Nonnull;
import org.apache.http.client.methods.HttpUriRequest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestVenicePath {
  private static final int SMART_LONG_TAIL_RETRY_ABORT_THRESHOLD_MS = 20;
  private static final String STORAGE_NODE1 = "s1";
  private static final String STORAGE_NODE2 = "s2";

  private static class SmartRetryVenicePath extends VenicePath {
    private final String ROUTER_REQUEST_VERSION =
        Integer.toString(ReadAvroProtocolDefinition.SINGLE_GET_ROUTER_REQUEST_V1.getProtocolVersion());

    public SmartRetryVenicePath(Time time, RetryManager retryManager) {
      super("fake_resource", 1, "fake_resource_v1", true, SMART_LONG_TAIL_RETRY_ABORT_THRESHOLD_MS, time, retryManager);
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
    public HttpUriRequest composeRouterRequestInternal(String storageNodeUri) {
      return null;
    }

    @Override
    public HttpMethod getHttpMethod() {
      return HttpMethod.GET;
    }

    @Override
    public byte[] getBody() {
      return null;
    }

    public String getVeniceApiVersionHeader() {
      return ROUTER_REQUEST_VERSION;
    }

    @Nonnull
    @Override
    public String getLocation() {
      return "fake_location";
    }
  }

  private RetryManager retryManager;
  private MetricsRepository metricsRepository;

  @BeforeMethod
  public void setup() {
    metricsRepository = new MetricsRepository();
    // retry manager is disabled by default
    retryManager = new RetryManager(metricsRepository, "test-retry-manager", 0, 0);
  }

  @Test
  public void testRetryAbortBecauseOfTimeConstraint() {
    TestMockTime time = new TestMockTime();
    time.setTime(1);
    SmartRetryVenicePath orgPath = new SmartRetryVenicePath(time, retryManager);
    orgPath.setLongTailRetryThresholdMs(20);
    assertTrue(orgPath.canRequestStorageNode(STORAGE_NODE1));
    assertTrue(orgPath.canRequestStorageNode(STORAGE_NODE2));
    orgPath.recordOriginalRequestStartTimestamp();
    orgPath.markStorageNodeAsFast(STORAGE_NODE1);

    SmartRetryVenicePath retryPath = new SmartRetryVenicePath(time, retryManager);
    retryPath.setRetryRequest();
    retryPath.setupRetryRelatedInfo(orgPath);
    time.sleep(SMART_LONG_TAIL_RETRY_ABORT_THRESHOLD_MS + orgPath.getLongTailRetryThresholdMs() + 1);
    assertTrue(retryPath.canRequestStorageNode(STORAGE_NODE1));
    assertTrue(retryPath.isRetryRequestTooLate());
  }

  @Test
  public void testRetryAbortBecauseOfSlowStorageNode() {
    TestMockTime time = new TestMockTime();
    time.setTime(1);
    SmartRetryVenicePath orgPath = new SmartRetryVenicePath(time, retryManager);
    orgPath.setLongTailRetryThresholdMs(20);
    assertTrue(orgPath.canRequestStorageNode(STORAGE_NODE1));
    orgPath.requestStorageNode(STORAGE_NODE1);
    assertTrue(
        orgPath.canRequestStorageNode(STORAGE_NODE1),
        STORAGE_NODE1 + " should be a good node for the original request even it hasn't been marked as fast.");
    assertTrue(orgPath.canRequestStorageNode(STORAGE_NODE2));
    orgPath.recordOriginalRequestStartTimestamp();

    SmartRetryVenicePath retryPath = new SmartRetryVenicePath(time, retryManager);
    retryPath.setRetryRequest();
    retryPath.setupRetryRelatedInfo(orgPath);
    time.sleep(1);
    assertFalse(retryPath.isRetryRequestTooLate());
    assertFalse(retryPath.canRequestStorageNode(STORAGE_NODE1));
    assertTrue(retryPath.canRequestStorageNode(STORAGE_NODE2));
  }

  @Test
  public void testSlowNodeIgnoredWhen5XXcodeReturned() {
    TestMockTime time = new TestMockTime();
    time.setTime(1);
    SmartRetryVenicePath orgPath = new SmartRetryVenicePath(time, retryManager);
    orgPath.setLongTailRetryThresholdMs(20);
    assertTrue(orgPath.canRequestStorageNode(STORAGE_NODE1));
    orgPath.requestStorageNode(STORAGE_NODE1);

    SmartRetryVenicePath retryPath = new SmartRetryVenicePath(time, retryManager);
    retryPath.setupRetryRelatedInfo(orgPath);
    retryPath.setRetryRequest(HttpResponseStatus.BAD_REQUEST);
    time.sleep(1);
    assertFalse(retryPath.isRetryRequestTooLate());
    assertFalse(retryPath.canRequestStorageNode(STORAGE_NODE1));

    // although the slow node set contains node 1, due to 500 status, the set would be ignored and node 1 can be
    // requested
    retryPath.setRetryRequest(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    assertTrue(retryPath.canRequestStorageNode(STORAGE_NODE1));
  }

  @Test
  public void testRetryLogicWhenMetBothCriteria() {
    TestMockTime time = new TestMockTime();
    time.setTime(1);
    SmartRetryVenicePath orgPath = new SmartRetryVenicePath(time, retryManager);
    orgPath.setLongTailRetryThresholdMs(20);
    assertTrue(orgPath.canRequestStorageNode(STORAGE_NODE1));
    assertTrue(orgPath.canRequestStorageNode(STORAGE_NODE2));
    orgPath.recordOriginalRequestStartTimestamp();
    orgPath.markStorageNodeAsFast(STORAGE_NODE1);

    SmartRetryVenicePath retryPath1 = new SmartRetryVenicePath(time, retryManager);
    retryPath1.setRetryRequest();
    retryPath1.setupRetryRelatedInfo(orgPath);
    time.sleep(1);
    assertFalse(retryPath1.isRetryRequestTooLate());
    assertTrue(retryPath1.canRequestStorageNode(STORAGE_NODE1));

    // Retry to an unknown storage node
    SmartRetryVenicePath retryPath2 = new SmartRetryVenicePath(time, retryManager);
    retryPath2.setRetryRequest();
    retryPath2.setupRetryRelatedInfo(orgPath);
    assertFalse(retryPath2.isRetryRequestTooLate());
    assertTrue(retryPath2.canRequestStorageNode(STORAGE_NODE1));
  }
}

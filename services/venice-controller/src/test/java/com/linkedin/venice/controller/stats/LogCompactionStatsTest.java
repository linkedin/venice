package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.REPUSH_STORE_TRIGGER_SOURCE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.controller.AbstractTestVeniceParentHelixAdmin;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.controller.logcompaction.LogCompactionService;
import com.linkedin.venice.controller.server.StoresRoutes;
import com.linkedin.venice.controllerapi.ControllerApiConstants;
import com.linkedin.venice.controllerapi.RepushJobResponse;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.RepushStoreTriggerSource;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.OpenTelemetryDataPointTestUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import spark.QueryParamsMap;
import spark.Request;
import spark.Response;
import spark.Route;


public class LogCompactionStatsTest extends AbstractTestVeniceParentHelixAdmin {
  private static final String TEST_METRIC_PREFIX = "log_compaction";
  private static final String TEST_CLUSTER_NAME = AbstractTestVeniceParentHelixAdmin.clusterName;
  private static final String TEST_STORE_NAME = "log-compaction-stats-test-store";
  private static final String TEST_EXECUTION_ID = "test-execution-id";
  private InMemoryMetricReader inMemoryMetricReader;

  private Route repushStoreRoute; // route to test repushStore() endpoint
  private LogCompactionService logCompactionService; // mock log compaction service

  @BeforeClass
  public void setUp() throws Exception {
    // add all the metrics that are used in the test
    Collection<MetricEntity> metricEntities =
        Arrays.asList(ControllerMetricEntity.REPUSH_STORE_ENDPOINT_CALL_COUNT.getMetricEntity());

    // setup metric reader to validate metric emission
    this.inMemoryMetricReader = InMemoryMetricReader.create();
    VeniceMetricsRepository metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(metricEntities)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());

    setupInternalMocks();
    doReturn(true).when(getConfig()).isLogCompactionEnabled(); // enable log compaction to initialise LogCompactionStats
                                                               // in VeniceParentHelixAdmin

    initializeParentAdmin(Optional.empty(), Optional.of(metricsRepository)); // initialises VeniceParentHelixAdmin
                                                                             // parentAdmin for testing
    when(getParentAdmin().isLeaderControllerFor(TEST_CLUSTER_NAME)).thenReturn(true);

    this.repushStoreRoute =
        new StoresRoutes(false, Optional.empty(), mock(PubSubTopicRepository.class)).repushStore(getParentAdmin());

    VeniceControllerMultiClusterConfig parentMultiClusterConfigs = getParentAdmin().getMultiClusterConfigs();
    when(parentMultiClusterConfigs.isLogCompactionEnabled()).thenReturn(true);
    when(parentMultiClusterConfigs.getLogCompactionIntervalMS()).thenReturn(TimeUnit.HOURS.toMillis(1));
    this.logCompactionService = new LogCompactionService(
        getParentAdmin(),
        clusterName,
        getParentAdmin().getControllerConfig(clusterName),
        metricsRepository);
  }

  /**
   * Test case:
     * Trigger source: manual
     * Response: VeniceParentHelixAdmin#repushStore() receives successful RepushJobResponse
   */
  @Test
  public void testEmitRepushStoreCallCountManualSuccessMetric() throws Exception {
    Attributes expectedAttributes = Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), TEST_STORE_NAME)
        .put(
            REPUSH_STORE_TRIGGER_SOURCE.getDimensionNameInDefaultFormat(),
            RepushStoreTriggerSource.MANUAL.getDimensionValue())
        .put(
            VENICE_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(),
            VeniceResponseStatusCategory.SUCCESS.getDimensionValue())
        .build();

    // mock successful response
    when(getInternalAdmin().repushStore(any())).thenReturn(new RepushJobResponse(TEST_STORE_NAME, TEST_EXECUTION_ID));

    // trigger manual repush
    RepushJobResponse repushJobResponse = ObjectMapperFactory.getInstance()
        .readValue(
            repushStoreRoute.handle(mockRequestForRepushStore(), mock(Response.class)).toString(),
            RepushJobResponse.class);

    // test validation
    Assert.assertFalse(repushJobResponse.isError());
    validateMetricEmission(
        ControllerMetricEntity.REPUSH_STORE_ENDPOINT_CALL_COUNT.getMetricName(),
        1,
        expectedAttributes);
  }

  /**
   * Test case:
     * Trigger source: manual
     * Response: VeniceParentHelixAdmin#repushStore() receives RepushJobResponse with error flag set to true
   */
  @Test
  public void testEmitRepushStoreCallCountManualFailWithErrorResponseMetric() throws Exception {
    Attributes expectedAttributes = Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), TEST_STORE_NAME)
        .put(
            REPUSH_STORE_TRIGGER_SOURCE.getDimensionNameInDefaultFormat(),
            RepushStoreTriggerSource.MANUAL.getDimensionValue())
        .put(
            VENICE_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(),
            VeniceResponseStatusCategory.FAIL.getDimensionValue())
        .build();

    // mock failed response
    RepushJobResponse errorResponse = new RepushJobResponse(TEST_STORE_NAME, TEST_EXECUTION_ID);
    errorResponse.setError("test case: repushStore() receives error response");
    when(getInternalAdmin().repushStore(any())).thenReturn(errorResponse);

    // trigger manual repush
    RepushJobResponse repushJobResponse = ObjectMapperFactory.getInstance()
        .readValue(
            repushStoreRoute.handle(mockRequestForRepushStore(), mock(Response.class)).toString(),
            RepushJobResponse.class);

    // test validation
    Assert.assertTrue(repushJobResponse.isError());
    validateMetricEmission(
        ControllerMetricEntity.REPUSH_STORE_ENDPOINT_CALL_COUNT.getMetricName(),
        1,
        expectedAttributes);
  }

  /**
   * Test case:
     * Trigger source: manual
     * Response: VeniceParentHelixAdmin#repushStore() receives exception from VeniceHelixAdmin#repushStore()
   */
  @Test
  public void testEmitRepushStoreCallCountManualFailWithExceptionMetric() throws Exception {
    Attributes expectedAttributes = Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), TEST_STORE_NAME)
        .put(
            REPUSH_STORE_TRIGGER_SOURCE.getDimensionNameInDefaultFormat(),
            RepushStoreTriggerSource.MANUAL.getDimensionValue())
        .put(
            VENICE_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(),
            VeniceResponseStatusCategory.FAIL.getDimensionValue())
        .build();

    // mock failed response
    when(getInternalAdmin().repushStore(any())).thenThrow(mock(Exception.class));

    // trigger manual repush
    RepushJobResponse repushJobResponse = ObjectMapperFactory.getInstance()
        .readValue(
            repushStoreRoute.handle(mockRequestForRepushStore(), mock(Response.class)).toString(),
            RepushJobResponse.class);

    // test validation
    Assert.assertTrue(repushJobResponse.isError());
    validateMetricEmission(
        ControllerMetricEntity.REPUSH_STORE_ENDPOINT_CALL_COUNT.getMetricName(),
        1,
        expectedAttributes);
  }

  /**
   * Test case:
     * Trigger source: scheduled
     * Response: VeniceParentHelixAdmin#repushStore() receives successful RepushJobResponse
   *
   * Note: Only the happy test case is implemented for scheduled repush because the behaviour in the other two cases
   * (error and exception) are covered by the corresponding manual repush test cases.
   */
  @Test
  public void testEmitRepushStoreCallCountScheduledSuccessMetric() throws Exception {
    Attributes expectedAttributes = Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), TEST_STORE_NAME)
        .put(
            REPUSH_STORE_TRIGGER_SOURCE.getDimensionNameInDefaultFormat(),
            RepushStoreTriggerSource.SCHEDULED.getDimensionValue())
        .put(
            VENICE_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(),
            VeniceResponseStatusCategory.SUCCESS.getDimensionValue())
        .build();

    // mock test store for scheduled compaction
    StoreInfo mockTestStoreInfo = mock(StoreInfo.class);
    when(mockTestStoreInfo.getName()).thenReturn(TEST_STORE_NAME);
    when(getInternalAdmin().getStoresForCompaction(anyString()))
        .thenReturn(Collections.singletonList(mockTestStoreInfo));

    // mock successful response
    when(getInternalAdmin().repushStore(any())).thenReturn(new RepushJobResponse(TEST_STORE_NAME, TEST_EXECUTION_ID));

    // start log compaction service to trigger scheduled repush
    logCompactionService.startInner();
    logCompactionService.stopInner();

    // test validation
    verify(getInternalAdmin()).repushStore(any());
    validateMetricEmission(
        ControllerMetricEntity.REPUSH_STORE_ENDPOINT_CALL_COUNT.getMetricName(),
        1,
        expectedAttributes);
  }

  /**
   * Util method to mock request for repushStore() endpoint
   */
  private Request mockRequestForRepushStore() {
    Request request = mock(Request.class);
    doReturn(TEST_CLUSTER_NAME).when(request).queryParams(eq(ControllerApiConstants.CLUSTER));
    doReturn(TEST_STORE_NAME).when(request).queryParams(eq(ControllerApiConstants.NAME));

    QueryParamsMap queryParamsMap = mock(QueryParamsMap.class);
    doReturn(queryParamsMap).when(request).queryMap();
    return request;
  }

  private void validateMetricEmission(String metricName, int expectedMetricValue, Attributes expectedAttributes) {
    Collection<MetricData> metricsData = inMemoryMetricReader.collectAllMetrics();
    Assert.assertFalse(metricsData.isEmpty());
    assertEquals(metricsData.size(), 1);

    LongPointData callCountData =
        OpenTelemetryDataPointTestUtils.getLongPointData(metricsData, metricName, TEST_METRIC_PREFIX);
    OpenTelemetryDataPointTestUtils.validateLongPointData(callCountData, expectedMetricValue, expectedAttributes);
  }
}

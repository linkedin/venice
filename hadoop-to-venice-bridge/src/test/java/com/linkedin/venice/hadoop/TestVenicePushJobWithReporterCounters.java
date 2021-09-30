package com.linkedin.venice.hadoop;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StorageEngineOverheadRatioResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.status.protocol.PushJobDetails;
import com.linkedin.venice.writer.VeniceWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.RunningJob;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class TestVenicePushJobWithReporterCounters {

  private static final int PARTITION_COUNT = 10;
  private static final String SCHEMA_STR = "{" +
      "  \"namespace\" : \"example.avro\",  " +
      "  \"type\": \"record\",   " +
      "  \"name\": \"User\",     " +
      "  \"fields\": [           " +
      "       { \"name\": \"id\", \"type\": \"string\" },  " +
      "       { \"name\": \"name\", \"type\": \"string\" },  " +
      "       { \"name\": \"age\", \"type\": \"int\" },  " +
      "       { \"name\": \"company\", \"type\": \"string\" }  " +
      "  ] " +
      " } ";
  private static final String SIMPLE_FILE_SCHEMA_STR = "{\n" +
          "    \"type\": \"record\",\n" +
          "    \"name\": \"Type1\",\n" +
          "    \"fields\": [\n" +
          "        {\n" +
          "            \"name\": \"something\",\n" +
          "            \"type\": \"string\"\n" +
          "        }\n" +
          "    ]\n" +
          "}";

  @Test (expectedExceptions = { VeniceException.class })
  public void testHandleQuotaExceeded() throws Exception {
    testHandleErrorsInCounter(
        Arrays.asList(
            new MockCounterInfo(MRJobCounterHelper.TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME, 1001), // Quota exceeded
            new MockCounterInfo(MRJobCounterHelper.WRITE_ACL_FAILURE_GROUP_COUNTER_NAME, 0),
            new MockCounterInfo(MRJobCounterHelper.DUP_KEY_WITH_DISTINCT_VALUE_GROUP_COUNTER_NAME, 0),
            new MockCounterInfo(MRJobCounterHelper.REDUCER_CLOSED_COUNT_GROUP_COUNTER_NAME, PARTITION_COUNT) // All reducers closed
        ),
        Arrays.asList(
            VenicePushJob.PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            VenicePushJob.PushJobCheckpoints.NEW_VERSION_CREATED,
            VenicePushJob.PushJobCheckpoints.QUOTA_EXCEEDED)
    );
  }

  @Test (expectedExceptions = { VeniceException.class })
  public void testHandleWriteAclFailed() throws Exception {
    testHandleErrorsInCounter(
        Arrays.asList(
            new MockCounterInfo(MRJobCounterHelper.TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME, 1),
            new MockCounterInfo(MRJobCounterHelper.WRITE_ACL_FAILURE_GROUP_COUNTER_NAME, 1), // Write ACL failed
            new MockCounterInfo(MRJobCounterHelper.DUP_KEY_WITH_DISTINCT_VALUE_GROUP_COUNTER_NAME, 0),
            new MockCounterInfo(MRJobCounterHelper.REDUCER_CLOSED_COUNT_GROUP_COUNTER_NAME, PARTITION_COUNT) // All reducers closed
        ),
        Arrays.asList(
            VenicePushJob.PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            VenicePushJob.PushJobCheckpoints.NEW_VERSION_CREATED,
            VenicePushJob.PushJobCheckpoints.WRITE_ACL_FAILED)
    );
  }

  @Test (expectedExceptions = { VeniceException.class })
  public void testHandleDuplicatedKeyWithDistinctValue() throws Exception {
    testHandleErrorsInCounter(
        Arrays.asList(
            new MockCounterInfo(MRJobCounterHelper.TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME, 1),
            new MockCounterInfo(MRJobCounterHelper.WRITE_ACL_FAILURE_GROUP_COUNTER_NAME, 0),
            new MockCounterInfo(MRJobCounterHelper.DUP_KEY_WITH_DISTINCT_VALUE_GROUP_COUNTER_NAME, 1), // Duplicated key with distinct value
            new MockCounterInfo(MRJobCounterHelper.REDUCER_CLOSED_COUNT_GROUP_COUNTER_NAME, PARTITION_COUNT) // All reducers closed
        ),
        Arrays.asList(
            VenicePushJob.PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            VenicePushJob.PushJobCheckpoints.NEW_VERSION_CREATED,
            VenicePushJob.PushJobCheckpoints.DUP_KEY_WITH_DIFF_VALUE)
    );
  }

  @Test (expectedExceptions = { VeniceException.class })
  public void testHandleZeroClosedReducersFailure() throws Exception {
    testHandleErrorsInCounter(
        Arrays.asList(
            new MockCounterInfo(MRJobCounterHelper.MAPPER_SPRAY_ALL_PARTITIONS_TRIGGERED_COUNT_NAME, 1), // Spray all partitions gets triggered
            new MockCounterInfo(MRJobCounterHelper.TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME, 1), // Quota not exceeded
            new MockCounterInfo(MRJobCounterHelper.WRITE_ACL_FAILURE_GROUP_COUNTER_NAME, 0), // No authorization error
            new MockCounterInfo(MRJobCounterHelper.DUP_KEY_WITH_DISTINCT_VALUE_GROUP_COUNTER_NAME, 0), // No duplicated key with distinct value
            new MockCounterInfo(MRJobCounterHelper.REDUCER_CLOSED_COUNT_GROUP_COUNTER_NAME, 0) // No reducers at all closed
        ),
        Arrays.asList(
            VenicePushJob.PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            VenicePushJob.PushJobCheckpoints.NEW_VERSION_CREATED,
            VenicePushJob.PushJobCheckpoints.DUP_KEY_WITH_DIFF_VALUE),
        10L // Non-empty input data file
    );
  }

  @Test (expectedExceptions = { VeniceException.class }, expectedExceptionsMessageRegExp = "MR job counter is not reliable.*")
  public void testUnreliableMapReduceCounter() throws Exception {
    testHandleErrorsInCounter(
        Arrays.asList(
            new MockCounterInfo(MRJobCounterHelper.MAPPER_SPRAY_ALL_PARTITIONS_TRIGGERED_COUNT_NAME, 1), // Spray all partitions gets triggered
            new MockCounterInfo(MRJobCounterHelper.TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME, 0), // Quota not exceeded
            new MockCounterInfo(MRJobCounterHelper.WRITE_ACL_FAILURE_GROUP_COUNTER_NAME, 0), // No authorization error
            new MockCounterInfo(MRJobCounterHelper.DUP_KEY_WITH_DISTINCT_VALUE_GROUP_COUNTER_NAME, 0), // No duplicated key with distinct value
            new MockCounterInfo(MRJobCounterHelper.REDUCER_CLOSED_COUNT_GROUP_COUNTER_NAME, 0) // No reducers at all closed
        ),
        Collections.emptyList(),
        10L, // Non-empty input data file
           true
    );
  }

  @Test
  public void testHandleZeroClosedReducersWithNoRecordInputDataFile() throws Exception {
    testHandleErrorsInCounter(
        Arrays.asList(
            new MockCounterInfo(MRJobCounterHelper.TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME, 0), // No quota not exceeded
            new MockCounterInfo(MRJobCounterHelper.WRITE_ACL_FAILURE_GROUP_COUNTER_NAME, 0), // No authorization error
            new MockCounterInfo(MRJobCounterHelper.DUP_KEY_WITH_DISTINCT_VALUE_GROUP_COUNTER_NAME, 0), // No duplicated key with distinct value
            new MockCounterInfo(MRJobCounterHelper.REDUCER_CLOSED_COUNT_GROUP_COUNTER_NAME, 0) // No reducers at all closed
        ),
        Arrays.asList(
            VenicePushJob.PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            VenicePushJob.PushJobCheckpoints.NEW_VERSION_CREATED,
            VenicePushJob.PushJobCheckpoints.MAP_REDUCE_JOB_COMPLETED,
            VenicePushJob.PushJobCheckpoints.JOB_STATUS_POLLING_COMPLETED // Expect the job to finish successfully
        ),
        10L,
           false // Input data file has no record
    );
  }

  @Test (expectedExceptions = { IllegalArgumentException.class })
  public void testInitInputDataInfoWithIllegalArguments() {
    VenicePushJob.SchemaInfo schemaInfo = new VenicePushJob.SchemaInfo();
    // Input file size cannot be zero.
    new InputDataInfoProvider.InputDataInfo(schemaInfo, 0, false);
  }

  @Test (expectedExceptions = { VeniceException.class })
  public void testHandleInsufficientClosedReducersFailure() throws Exception { // Successful workflow
    testHandleErrorsInCounter(
        Arrays.asList(
            new MockCounterInfo(MRJobCounterHelper.MAPPER_SPRAY_ALL_PARTITIONS_TRIGGERED_COUNT_NAME, 1), // Spray all partitions gets triggered
            new MockCounterInfo(MRJobCounterHelper.TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME, 1), // Quota not exceeded
            new MockCounterInfo(MRJobCounterHelper.WRITE_ACL_FAILURE_GROUP_COUNTER_NAME, 0), // No authorization error
            new MockCounterInfo(MRJobCounterHelper.DUP_KEY_WITH_DISTINCT_VALUE_GROUP_COUNTER_NAME, 0), // No duplicated key with distinct value
            new MockCounterInfo(MRJobCounterHelper.REDUCER_CLOSED_COUNT_GROUP_COUNTER_NAME, PARTITION_COUNT - 1) // Some but not all reducers closed

        ),
        Arrays.asList(
            VenicePushJob.PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            VenicePushJob.PushJobCheckpoints.NEW_VERSION_CREATED,
            VenicePushJob.PushJobCheckpoints.MAP_REDUCE_JOB_COMPLETED,
            VenicePushJob.PushJobCheckpoints.JOB_STATUS_POLLING_COMPLETED
        )
    );
  }

  @Test
  public void testCounterValidationWhenSprayAllPartitionsNotTriggeredButWithMismatchedReducerCount() throws Exception { // Successful workflow
    testHandleErrorsInCounter(
        Arrays.asList(
            new MockCounterInfo(MRJobCounterHelper.MAPPER_SPRAY_ALL_PARTITIONS_TRIGGERED_COUNT_NAME, 0), // Spray all partitions isn't triggered
            new MockCounterInfo(MRJobCounterHelper.TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME, 1), // Quota not exceeded
            new MockCounterInfo(MRJobCounterHelper.WRITE_ACL_FAILURE_GROUP_COUNTER_NAME, 0), // No authorization error
            new MockCounterInfo(MRJobCounterHelper.DUP_KEY_WITH_DISTINCT_VALUE_GROUP_COUNTER_NAME, 0), // No duplicated key with distinct value
            new MockCounterInfo(MRJobCounterHelper.REDUCER_CLOSED_COUNT_GROUP_COUNTER_NAME, PARTITION_COUNT - 1) // Some but not all reducers closed

        ),
        Arrays.asList(
            VenicePushJob.PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            VenicePushJob.PushJobCheckpoints.NEW_VERSION_CREATED,
            VenicePushJob.PushJobCheckpoints.MAP_REDUCE_JOB_COMPLETED,
            VenicePushJob.PushJobCheckpoints.JOB_STATUS_POLLING_COMPLETED
        )
    );
  }

  @Test
  public void testHandleNoErrorInCounters() throws Exception { // Successful workflow
    testHandleErrorsInCounter(
        Arrays.asList(
            new MockCounterInfo(MRJobCounterHelper.TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME, 1), // Quota not exceeded
            new MockCounterInfo(MRJobCounterHelper.WRITE_ACL_FAILURE_GROUP_COUNTER_NAME, 0), // No authorization error
            new MockCounterInfo(MRJobCounterHelper.DUP_KEY_WITH_DISTINCT_VALUE_GROUP_COUNTER_NAME, 0), // No duplicated key with distinct value
            new MockCounterInfo(MRJobCounterHelper.REDUCER_CLOSED_COUNT_GROUP_COUNTER_NAME, PARTITION_COUNT) // All reducers closed
        ),
        Arrays.asList(
            VenicePushJob.PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            VenicePushJob.PushJobCheckpoints.NEW_VERSION_CREATED,
            VenicePushJob.PushJobCheckpoints.MAP_REDUCE_JOB_COMPLETED,
            VenicePushJob.PushJobCheckpoints.JOB_STATUS_POLLING_COMPLETED
        )
    );
  }

  private void testHandleErrorsInCounter(
      List<MockCounterInfo> mockCounterInfos,
      List<VenicePushJob.PushJobCheckpoints> expectedReportedCheckpoints
  ) throws Exception {
    testHandleErrorsInCounter(mockCounterInfos, expectedReportedCheckpoints, 10L);
  }

  private void testHandleErrorsInCounter(
      List<MockCounterInfo> mockCounterInfos,
      List<VenicePushJob.PushJobCheckpoints> expectedReportedCheckpoints,
      long inputFileDataSizeInBytes
  ) throws Exception {
    testHandleErrorsInCounter(mockCounterInfos, expectedReportedCheckpoints, inputFileDataSizeInBytes, inputFileDataSizeInBytes > 0);
  }

  private void testHandleErrorsInCounter(
      List<MockCounterInfo> mockCounterInfos,
      List<VenicePushJob.PushJobCheckpoints> expectedReportedCheckpoints,
      long inputFileDataSizeInBytes,
      boolean inputFileHasRecords
  ) throws Exception {
    VenicePushJob venicePushJob = new VenicePushJob(
            "job-id",
            getH2VProps(),
            createControllerClientMock(),
            createClusterDiscoverControllerClient()
    );
    venicePushJob.setSystemKMEStoreControllerClient(createControllerClientMock());

    venicePushJob.setJobClientWrapper(createJobClientWrapperMock(mockCounterInfos));
    venicePushJob.setClusterDiscoveryControllerClient(createClusterDiscoveryControllerClientMock());
    venicePushJob.setInputDataInfoProvider(getInputDataInfoProviderMock(inputFileDataSizeInBytes, inputFileHasRecords));
    venicePushJob.setVeniceWriter(createVeniceWriterMock());

    SentPushJobDetailsTrackerImpl pushJobDetailsTracker = new SentPushJobDetailsTrackerImpl();
    venicePushJob.setSentPushJobDetailsTracker(pushJobDetailsTracker);
    venicePushJob.run();
    List<Integer> actualReportedCheckpointValues =
        new ArrayList<>(pushJobDetailsTracker.getRecordedPushJobDetails().size());

    for (PushJobDetails pushJobDetails : pushJobDetailsTracker.getRecordedPushJobDetails()) {
      actualReportedCheckpointValues.add(pushJobDetails.pushJobLatestCheckpoint);
    }
    List<Integer> expectedCheckpointValues =
        expectedReportedCheckpoints.stream().map(VenicePushJob.PushJobCheckpoints::getValue).collect(Collectors.toList());

    Assert.assertEquals(actualReportedCheckpointValues, expectedCheckpointValues);
  }

  private Properties getH2VProps() {
    Properties props = new Properties();
    props.put(VenicePushJob.VENICE_URL_PROP, "venice-urls");
    props.put(VenicePushJob.VENICE_STORE_NAME_PROP, "store-name");
    props.put(VenicePushJob.INPUT_PATH_PROP, "input-path");
    props.put(VenicePushJob.KEY_FIELD_PROP, "id");
    props.put(VenicePushJob.VALUE_FIELD_PROP, "name");
    // No need for a big close timeout in tests. This is just to speed up discovery of certain regressions.
    props.put(VeniceWriter.CLOSE_TIMEOUT_MS, 500);
    props.put(VenicePushJob.POLL_JOB_STATUS_INTERVAL_MS, 1000);
    props.setProperty(VenicePushJob.SSL_KEY_STORE_PROPERTY_NAME, "test");
    props.setProperty(VenicePushJob.SSL_TRUST_STORE_PROPERTY_NAME,"test");
    props.setProperty(VenicePushJob.SSL_KEY_STORE_PASSWORD_PROPERTY_NAME,"test");
    props.setProperty(VenicePushJob.SSL_KEY_PASSWORD_PROPERTY_NAME,"test");
    props.setProperty(VenicePushJob.PUSH_JOB_STATUS_UPLOAD_ENABLE, "true");
    return props;
  }

  private InputDataInfoProvider getInputDataInfoProviderMock(
      long inputFileDataSizeInBytes,
      boolean inputFileHasRecords
  ) throws Exception {
    InputDataInfoProvider inputDataInfoProvider = mock(InputDataInfoProvider.class);
    VenicePushJob.SchemaInfo schemaInfo = new VenicePushJob.SchemaInfo();
    schemaInfo.keySchemaString = SCHEMA_STR;
    schemaInfo.valueSchemaString = SCHEMA_STR;
    schemaInfo.keyField = "key-field";
    schemaInfo.valueField = "value-field";
    schemaInfo.fileSchemaString = SIMPLE_FILE_SCHEMA_STR;
    InputDataInfoProvider.InputDataInfo inputDataInfo =
        new InputDataInfoProvider.InputDataInfo(schemaInfo, inputFileDataSizeInBytes, inputFileHasRecords);
    when(inputDataInfoProvider.validateInputAndGetInfo(anyString())).thenReturn(inputDataInfo);
    return inputDataInfoProvider;
  }

  private ControllerClient createClusterDiscoveryControllerClientMock() {
    ControllerClient controllerClient = mock(ControllerClient.class);
    D2ServiceDiscoveryResponse controllerResponse = mock(D2ServiceDiscoveryResponse.class);
    when(controllerResponse.getCluster()).thenReturn("mock-cluster-name");
    when(controllerClient.discoverCluster(anyString())).thenReturn(controllerResponse);
    return controllerClient;
  }

  private JobStatusQueryResponse createJobStatusQueryResponseMock() {
    JobStatusQueryResponse jobStatusQueryResponse = mock(JobStatusQueryResponse.class);
    when(jobStatusQueryResponse.isError()).thenReturn(false);
    when(jobStatusQueryResponse.getExtraInfo()).thenReturn(Collections.emptyMap());
    when(jobStatusQueryResponse.getOptionalStatusDetails()).thenReturn(Optional.empty());
    when(jobStatusQueryResponse.getOptionalExtraDetails()).thenReturn(Optional.empty());
    when(jobStatusQueryResponse.getStatus()).thenReturn(ExecutionStatus.COMPLETED.toString());
    return jobStatusQueryResponse;
  }

  private ControllerClient createControllerClientMock() {
    ControllerClient controllerClient = mock(ControllerClient.class);
    StoreResponse storeResponse = mock(StoreResponse.class);
    StoreInfo storeInfo = mock(StoreInfo.class);

    when(controllerClient.getValueSchema(anyString(), anyInt())).thenReturn(mock(SchemaResponse.class));

    StorageEngineOverheadRatioResponse storageEngineOverheadRatioResponse = mock(StorageEngineOverheadRatioResponse.class);
    when(storageEngineOverheadRatioResponse.isError()).thenReturn(false);
    when(storageEngineOverheadRatioResponse.getStorageEngineOverheadRatio()).thenReturn(1.0);

    when(storeInfo.getStorageQuotaInByte()).thenReturn(1000L);
    when(storeInfo.isSchemaAutoRegisterFromPushJobEnabled()).thenReturn(false);
    when(storeResponse.getStore()).thenReturn(storeInfo);

    SchemaResponse keySchemaResponse = mock(SchemaResponse.class);
    when(keySchemaResponse.isError()).thenReturn(false);
    when(keySchemaResponse.getSchemaStr()).thenReturn(SCHEMA_STR);

    SchemaResponse valueSchemaResponse = mock(SchemaResponse.class);
    when(valueSchemaResponse.isError()).thenReturn(false);
    when(valueSchemaResponse.getId()).thenReturn(12345);
    VersionCreationResponse versionCreationResponse = createVersionCreationResponse();

    when(controllerClient.getStore(anyString())).thenReturn(storeResponse);
    when(controllerClient.getStorageEngineOverheadRatio(anyString())).thenReturn(storageEngineOverheadRatioResponse);
    when(controllerClient.getKeySchema(anyString())).thenReturn(keySchemaResponse);
    when(controllerClient.getValueSchemaID(anyString(), anyString())).thenReturn(valueSchemaResponse);
    when(controllerClient.requestTopicForWrites(anyString(), anyLong(), any(),
        anyString(), anyBoolean(), anyBoolean(), anyBoolean(), any(), any(), any(), anyBoolean(), anyLong())).thenReturn(versionCreationResponse);
    JobStatusQueryResponse jobStatusQueryResponse = createJobStatusQueryResponseMock();
    when(controllerClient.queryOverallJobStatus(anyString(), any())).thenReturn(jobStatusQueryResponse);

    ControllerResponse controllerResponse = mock(ControllerResponse.class);
    when(controllerResponse.isError()).thenReturn(false);
    when(controllerClient.sendPushJobDetails(anyString(), anyInt(), any(byte[].class))).thenReturn(controllerResponse);
    return controllerClient;
  }

  private ControllerClient createClusterDiscoverControllerClient() {
    ControllerClient controllerClient = mock(ControllerClient.class);
    D2ServiceDiscoveryResponse clusterDiscoveryResponse = mock(D2ServiceDiscoveryResponse.class);
    when(clusterDiscoveryResponse.isError()).thenReturn(false);
    when(clusterDiscoveryResponse.getCluster()).thenReturn("some-cluster");
    when(controllerClient.discoverCluster(anyString())).thenReturn(clusterDiscoveryResponse);
    return controllerClient;
  }

  private VersionCreationResponse createVersionCreationResponse() {
    VersionCreationResponse versionCreationResponse = mock(VersionCreationResponse.class);
    when(versionCreationResponse.isError()).thenReturn(false);
    when(versionCreationResponse.getKafkaTopic()).thenReturn("kafka-topic");
    when(versionCreationResponse.getVersion()).thenReturn(1);
    when(versionCreationResponse.getKafkaBootstrapServers()).thenReturn("kafka-bootstrap-server");
    when(versionCreationResponse.getPartitions()).thenReturn(PARTITION_COUNT);
    when(versionCreationResponse.isEnableSSL()).thenReturn(false);
    when(versionCreationResponse.getCompressionStrategy()).thenReturn(CompressionStrategy.NO_OP);
    when(versionCreationResponse.isDaVinciPushStatusStoreEnabled()).thenReturn(false);
    when(versionCreationResponse.getPartitionerClass()).thenReturn("PartitionerClass");
    when(versionCreationResponse.getPartitionerParams()).thenReturn(Collections.emptyMap());
    return versionCreationResponse;
  }

  private JobClientWrapper createJobClientWrapperMock(List<MockCounterInfo> mockCounterInfos) throws IOException {
    RunningJob runningJob = mock(RunningJob.class);
    Counters counters = mock(Counters.class);

    Map<String, Counters.Group> groupMap = new HashMap<>();
    for (MockCounterInfo mockCounterInfo : mockCounterInfos) {
      Counters.Group group = groupMap.computeIfAbsent(mockCounterInfo.getGroupName(), k -> mock(Counters.Group.class));
      when(group.getCounter(mockCounterInfo.getCounterName())).thenReturn(mockCounterInfo.getCounterValue());
      when(counters.getGroup(mockCounterInfo.getGroupName())).thenReturn(group);
    }
    when(runningJob.getCounters()).thenReturn(counters);
    JobClientWrapper jobClientWrapper = mock(JobClientWrapper.class);
    when(jobClientWrapper.runJobWithConfig(any())).thenReturn(runningJob);
    return jobClientWrapper;
  }

  private VeniceWriter<KafkaKey, byte[], byte[]> createVeniceWriterMock() {
    return mock(VeniceWriter.class);
  }

  private static class MockCounterInfo {
    private final MRJobCounterHelper.GroupAndCounterNames groupAndCounterNames;
    private final long counterValue;

    MockCounterInfo(MRJobCounterHelper.GroupAndCounterNames groupAndCounterNames, long counterValue) {
      this.groupAndCounterNames = groupAndCounterNames;
      this.counterValue = counterValue;
    }

    String getGroupName() {
      return groupAndCounterNames.getGroupName();
    }
    String getCounterName() {
      return groupAndCounterNames.getCounterName();
    }
    long getCounterValue() {
      return counterValue;
    }
  }
}

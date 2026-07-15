package com.linkedin.venice.hadoop;

import static com.linkedin.venice.vpj.VenicePushJobConstants.DATA_WRITER_COMPUTE_JOB_CLASS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PUSH_JOB_TIMEOUT_OVERRIDE_MS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALUE_FIELD_PROP;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.venice.PushJobCheckpoints;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.etl.ETLValueSchemaTransformation;
import com.linkedin.venice.exceptions.ConcurrentBatchPushException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceStoreAclException;
import com.linkedin.venice.jobs.DataWriterComputeJob;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * This class contains only unit tests for VenicePushJob class.
 *
 * For integration tests please refer to TestVenicePushJob
 *
 * todo: Remove dependency on utils from 'venice-test-common' module
 */

public class VenicePushJobLifecycleTest extends VenicePushJobTestBase {
  @Test
  public void testCheckLastModifiedTimestamp() throws Exception {
    File inputDir = TestWriteUtils.getTempDataDirectory();
    TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);
    String inputUri = "file://" + inputDir.getAbsolutePath();

    PushJobSetting pushJobSetting = new PushJobSetting();
    pushJobSetting.inputURI = inputUri;
    pushJobSetting.etlValueSchemaTransformation = ETLValueSchemaTransformation.NONE;
    pushJobSetting.isZstdDictCreationRequired = false;
    VeniceProperties props = VeniceProperties.empty();

    try (DefaultInputDataInfoProvider provider = new DefaultInputDataInfoProvider(pushJobSetting, props);
        VenicePushJob mockJob = getSpyVenicePushJob(new Properties(), null)) {
      InputDataInfoProvider.InputDataInfo inputDataInfo = provider.validateInputAndGetInfo(inputUri);

      when(mockJob.getInputDataInfo()).thenReturn(inputDataInfo);
      when(mockJob.getInputDataInfo()).thenReturn(inputDataInfo);
      when(mockJob.getPushJobSetting()).thenReturn(pushJobSetting);
      when(mockJob.getPushJobSetting()).thenReturn(pushJobSetting);
      when(mockJob.getInputDataInfoProvider()).thenReturn(provider);
      when(mockJob.getInputDataInfoProvider()).thenReturn(provider);

      // No modifications to input and no interactions expected with mockJob
      mockJob.checkLastModificationTimeAndLog();
      verify(mockJob, never()).updatePushJobDetailsWithCheckpoint(any(PushJobCheckpoints.class));

      // Write a new file to the input directory
      TestWriteUtils.writeSimpleAvroFileWithStringToStringWithExtraSchema(inputDir);
      mockJob.checkLastModificationTimeAndLog();
      verify(mockJob, times(1)).updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.DATASET_CHANGED);
    }
  }

  @Test
  public void testHandleVersionCreationACLError() {
    VenicePushJob mockJob = getSpyVenicePushJob(new Properties(), null);
    Throwable error = new VeniceStoreAclException("ACL error");
    VersionCreationResponse response = new VersionCreationResponse();
    response.setError(error);
    mockJob.handleVersionCreationError(response);
    verify(mockJob).updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.WRITE_ACL_FAILED);
  }

  @Test
  public void testHandleVersionCreationConcurrentPushError() {
    VenicePushJob mockJob = getSpyVenicePushJob(new Properties(), null);
    Throwable error = new ConcurrentBatchPushException("Another push is in progress");
    VersionCreationResponse response = new VersionCreationResponse();
    response.setError(error);
    mockJob.handleVersionCreationError(response);
    verify(mockJob).updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.CONCURRENT_BATCH_PUSH);
  }

  /**
   * Test that VenicePushJob.cancel() is called after bootstrapToOnlineTimeoutInHours is reached.
   * UNKNOWN status is returned for pollStatusUntilComplete() to stall the job until cancel() can be called.
   */
  @Test(dataProvider = "DataWriterJobClasses")
  public void testPushJobTimeout(Class<? extends DataWriterComputeJob> dataWriterJobClass) throws Exception {
    Properties props = getVpjRequiredProperties();
    props.put(KEY_FIELD_PROP, "id");
    props.put(VALUE_FIELD_PROP, "name");
    props.put(DATA_WRITER_COMPUTE_JOB_CLASS, dataWriterJobClass.getCanonicalName());
    props.put(PUSH_JOB_TIMEOUT_OVERRIDE_MS, 2);
    ControllerClient client = getClient();
    JobStatusQueryResponse response = mock(JobStatusQueryResponse.class);
    doReturn("UNKNOWN").when(response).getStatus();
    doReturn(response).when(client).queryOverallJobStatus(anyString(), any(), any(), anyBoolean());
    doReturn(response).when(client).killOfflinePushJob(anyString());

    try (VenicePushJob pushJob = getSpyVenicePushJob(props, client)) {
      PushJobSetting pushJobSetting = pushJob.getPushJobSetting();
      pushJobSetting.jobStatusInUnknownStateTimeoutMs = 100; // give some time for the timeout to run on the executor
      StoreInfo storeInfo = new StoreInfo();
      storeInfo.setBootstrapToOnlineTimeoutInHours(0);
      pushJobSetting.storeResponse = new StoreResponse();
      pushJobSetting.storeResponse.setStore(storeInfo);
      skipVPJValidation(pushJob);
      try {
        DataWriterComputeJob dataWriterJob = spy(pushJob.getDataWriterComputeJob());
        doNothing().when(dataWriterJob).configure(any(), any()); // the spark job takes a long time to configure
        pushJob.setDataWriterComputeJob(dataWriterJob);
        pushJob.run();
        fail("Test should fail because pollStatusUntilComplete() never saw COMPLETE status, but doesn't.");
      } catch (VeniceException e) {
        Assert.assertTrue(e.getMessage().contains("push job is still in unknown state."), e.getMessage());
      }
      verify(pushJob, times(1)).cancel();
      verify(pushJob.getDataWriterComputeJob(), times(1)).kill();
    }
  }

  /**
   * Ensures that the data writer job is killed if the job times out. Uses an Answer to stall the data writer job
   * while it's running in order for it to get killed properly.
   */
  @Test(dataProvider = "DataWriterJobClasses")
  public void testDataWriterComputeJobTimeout(Class<? extends DataWriterComputeJob> dataWriterJobClass)
      throws Exception {
    Properties props = getVpjRequiredProperties();
    props.put(KEY_FIELD_PROP, "id");
    props.put(VALUE_FIELD_PROP, "name");
    props.put(DATA_WRITER_COMPUTE_JOB_CLASS, dataWriterJobClass.getCanonicalName());
    props.put(PUSH_JOB_TIMEOUT_OVERRIDE_MS, 5L);
    ControllerClient client = getClient();
    JobStatusQueryResponse response = mock(JobStatusQueryResponse.class);
    doReturn("SUCCESS").when(response).getStatus();
    doReturn(response).when(client).queryOverallJobStatus(anyString(), any(), any(), anyBoolean());
    doReturn(response).when(client).killOfflinePushJob(anyString());

    try (VenicePushJob pushJob = getSpyVenicePushJob(props, client)) {
      StoreInfo storeInfo = new StoreInfo();
      storeInfo.setBootstrapToOnlineTimeoutInHours(1);
      PushJobSetting pushJobSetting = pushJob.getPushJobSetting();
      pushJobSetting.storeResponse = new StoreResponse();
      pushJobSetting.storeResponse.setStore(storeInfo);

      CountDownLatch runningJobLatch = new CountDownLatch(1);
      CountDownLatch killedJobLatch = new CountDownLatch(1);

      /*
       * 1. Data writer job starts and status is set to RUNNING.
       * 2. Timeout thread kills the data writer job and status is set to KILLED.
       * The latch is used to stall the runComputeJob() method until the data writer job is killed.
       */
      Answer<Void> stallDataWriterJob = invocation -> {
        // At this point, the data writer job status is already set to RUNNING.
        runningJobLatch.countDown(); // frees VenicePushJob.killJob()
        if (!killedJobLatch.await(10, TimeUnit.SECONDS)) { // waits for this data writer job to be killed
          fail("Timed out waiting for the data writer job to be killed.");
        }
        throw new VeniceException("No data found at source path");
      };

      Answer<Void> killDataWriterJob = invocation -> {
        if (!runningJobLatch.await(10, TimeUnit.SECONDS)) { // waits for job status to be set to RUNNING
          fail("Timed out waiting for the data writer job status to be set to RUNNING");
        }
        pushJob.killDataWriterJob(); // sets job status to KILLED
        killedJobLatch.countDown(); // frees DataWriterComputeJob.runComputeJob()
        return null;
      };

      DataWriterComputeJob dataWriterJob = spy(pushJob.getDataWriterComputeJob());
      try {
        skipVPJValidation(pushJob);
        doCallRealMethod().when(pushJob).runJobAndUpdateStatus();
        pushJob.setDataWriterComputeJob(dataWriterJob);
        doNothing().when(dataWriterJob).validateJob();
        doNothing().when(dataWriterJob).configure(any(), any()); // the spark job takes a long time to configure
        doAnswer(stallDataWriterJob).when(dataWriterJob).runComputeJob();
        doAnswer(killDataWriterJob).when(pushJob).killJob(any(), any());
        pushJob.run(); // data writer job will run in this main test thread
      } catch (VeniceException e) {
        // Expected, because the data writer job is not configured to run successfully in this unit test environment
      }
      assertEquals(runningJobLatch.getCount(), 0); // killDataWriterJob() does not occur in the main test thread
      assertEquals(killedJobLatch.getCount(), 0);
      verify(pushJob, times(1)).cancel();
      verify(dataWriterJob, times(1)).kill();
      assertEquals(pushJob.getDataWriterComputeJob().getStatus(), DataWriterComputeJob.Status.KILLED);
    }
  }

  /**
   * Test that VPJ detects a killed push during the data writing phase and kills the data writer job.
   * This simulates the scenario where a repush is superseded by a user push, and the controller kills
   * the repush version while data is still being written.
   */
  @Test(dataProvider = "DataWriterJobClasses")
  public void testPushJobKilledDuringDataWriting(Class<? extends DataWriterComputeJob> dataWriterJobClass)
      throws Exception {
    Properties props = getVpjRequiredProperties();
    props.put(KEY_FIELD_PROP, "id");
    props.put(VALUE_FIELD_PROP, "name");
    props.put(DATA_WRITER_COMPUTE_JOB_CLASS, dataWriterJobClass.getCanonicalName());
    ControllerClient client = getClient();

    // Simulate controller returning ERROR status (push was killed)
    JobStatusQueryResponse killResponse = mock(JobStatusQueryResponse.class);
    doReturn(ExecutionStatus.ERROR.toString()).when(killResponse).getStatus();
    doReturn(false).when(killResponse).isError();
    doReturn(killResponse).when(client).queryOverallJobStatus(anyString(), any(), any(), anyBoolean());

    try (VenicePushJob pushJob = getSpyVenicePushJob(props, client)) {
      PushJobSetting pushJobSetting = pushJob.getPushJobSetting();
      pushJobSetting.pollJobStatusIntervalMs = 10; // Poll quickly for the test

      CountDownLatch dataWriterRunningLatch = new CountDownLatch(1);
      CountDownLatch dataWriterKilledLatch = new CountDownLatch(1);

      // Stall the data writer job until it gets killed by the kill-check monitor
      doCallRealMethod().when(pushJob).runJobWithKillDetection();
      doCallRealMethod().when(pushJob).runJobAndUpdateStatus();
      doCallRealMethod().when(pushJob).startPushJobKillCheckMonitor();
      doCallRealMethod().when(pushJob).stopPushJobKillCheckMonitor();
      doCallRealMethod().when(pushJob).killDataWriterJob();

      DataWriterComputeJob dataWriterJob = spy(pushJob.getDataWriterComputeJob());
      pushJob.setDataWriterComputeJob(dataWriterJob);
      doNothing().when(dataWriterJob).configure(any(), any());
      doNothing().when(dataWriterJob).validateJob();

      Answer<Void> stallDataWriterJob = invocation -> {
        dataWriterRunningLatch.countDown();
        if (!dataWriterKilledLatch.await(10, TimeUnit.SECONDS)) {
          fail("Timed out waiting for the data writer job to be killed by kill-check monitor");
        }
        throw new VeniceException("Data writer job was killed");
      };
      doAnswer(stallDataWriterJob).when(dataWriterJob).runComputeJob();

      // When dataWriterJob.kill() is called, release the stalled data writer
      doAnswer(invocation -> {
        invocation.callRealMethod();
        dataWriterKilledLatch.countDown();
        return null;
      }).when(dataWriterJob).kill();

      // Stub only the validation methods from skipVPJValidation that this test needs bypassed.
      // We intentionally avoid skipVPJValidation() because it also stubs runJobAndUpdateStatus(),
      // which we need to run for kill-detection to work.
      doAnswer(invocation -> {
        VeniceProperties properties = pushJob.getJobProperties();
        PushJobSetting pjs = pushJob.getPushJobSetting();
        if (!pjs.isSourceKafka) {
          Schema schema = AvroSchemaParseUtils.parseSchemaFromJSONLooseValidation(SIMPLE_FILE_SCHEMA_STR);
          pjs.keyField = properties.getString(KEY_FIELD_PROP, DEFAULT_KEY_FIELD_PROP);
          pjs.valueField = properties.getString(VALUE_FIELD_PROP, DEFAULT_VALUE_FIELD_PROP);
          pjs.inputDataSchema = schema;
          pjs.valueSchema = schema.getField(pjs.valueField).schema();
          pjs.inputDataSchemaString = SIMPLE_FILE_SCHEMA_STR;
          pjs.keySchema = pjs.inputDataSchema.getField(pjs.keyField).schema();
          pjs.keySchemaString = pjs.keySchema.toString();
          pjs.valueSchemaString = pjs.valueSchema.toString();
        }
        return getMockInputDataInfoProvider();
      }).when(pushJob).getInputDataInfoProvider();
      doNothing().when(pushJob).validateKeySchema(any());
      doNothing().when(pushJob).validateAndRetrieveValueSchemas(any(), any(), anyBoolean());

      try {
        pushJob.run();
        fail("Expected VeniceException due to push job being killed during data writing");
      } catch (VeniceException e) {
        assertTrue(
            e.getMessage().contains("killed by the controller during the data writing phase")
                || e.getMessage().contains("Data writer job was killed"),
            "Unexpected error message: " + e.getMessage());
      }

      assertEquals(dataWriterRunningLatch.getCount(), 0, "Data writer job should have started");
      assertEquals(dataWriterKilledLatch.getCount(), 0, "Data writer job should have been killed");
      verify(dataWriterJob, times(1)).kill();
      verify(client, atLeastOnce()).queryOverallJobStatus(anyString(), any(), any(), anyBoolean());
    }
  }
}

package com.linkedin.venice.hadoop.mapreduce.datawriter.jobs;

import static com.linkedin.venice.vpj.VenicePushJobConstants.WRITER_RMD_SCHEMA_STRING_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.WRITER_VALUE_SCHEMA_STRING_PROP;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.etl.ETLValueSchemaTransformation;
import com.linkedin.venice.hadoop.PushJobSetting;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.utils.TestWriteUtils;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Cluster;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestDataWriterMRJob {
  private FileSystem fs;
  private Path stagingAreaDir;

  @BeforeMethod
  public void setUp() throws IOException {
    stagingAreaDir = new Path(System.getProperty("java.io.tmpdir"), "staging-area-" + System.nanoTime());
    fs = stagingAreaDir.getFileSystem(new Configuration());
  }

  @AfterMethod
  public void tearDown() throws IOException {
    fs.delete(stagingAreaDir, true);
  }

  private DataWriterMRJob createMRJobWithMockCluster(RunningJob runningJob) {
    Cluster mockCluster = mock(Cluster.class);
    try {
      doReturn(stagingAreaDir).when(mockCluster).getStagingAreaDir();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    DataWriterMRJob mrJob = new DataWriterMRJob() {
      @Override
      Cluster createCluster(JobConf conf) {
        return mockCluster;
      }
    };
    mrJob.setRunningJob(runningJob);
    mrJob.setJobConf(new JobConf());
    return mrJob;
  }

  @Test
  public void testCloseCleansStagingDirOnCompletedJob() throws IOException {
    JobID jobId = new JobID("test", 1);
    Path jobStagingDir = new Path(stagingAreaDir, jobId.toString());
    fs.mkdirs(jobStagingDir);
    assertTrue(fs.exists(jobStagingDir));

    RunningJob runningJob = mock(RunningJob.class);
    doReturn(true).when(runningJob).isComplete();
    doReturn(jobId).when(runningJob).getID();

    DataWriterMRJob mrJob = createMRJobWithMockCluster(runningJob);
    mrJob.close();

    assertFalse(fs.exists(jobStagingDir));
  }

  @Test
  public void testCloseCleansStagingDirOnIncompleteJob() throws IOException {
    JobID jobId = new JobID("test", 2);
    Path jobStagingDir = new Path(stagingAreaDir, jobId.toString());
    fs.mkdirs(jobStagingDir);
    assertTrue(fs.exists(jobStagingDir));

    RunningJob runningJob = mock(RunningJob.class);
    doReturn(false).when(runningJob).isComplete();
    doReturn(jobId).when(runningJob).getID();

    DataWriterMRJob mrJob = createMRJobWithMockCluster(runningJob);
    mrJob.close();

    verify(runningJob).killJob();
    assertFalse(fs.exists(jobStagingDir));
  }

  @Test
  public void testCloseSkipsCleanupWhenRunningJobIsNull() throws IOException {
    // Should not throw when runningJob is null
    DataWriterMRJob mrJob = new DataWriterMRJob();
    mrJob.close();
  }

  @Test
  public void testCloseStagingCleanupStillRunsWhenKillJobThrows() throws IOException {
    JobID jobId = new JobID("test", 3);
    Path jobStagingDir = new Path(stagingAreaDir, jobId.toString());
    fs.mkdirs(jobStagingDir);
    assertTrue(fs.exists(jobStagingDir));

    RunningJob runningJob = mock(RunningJob.class);
    doReturn(false).when(runningJob).isComplete();
    doReturn(jobId).when(runningJob).getID();
    org.mockito.Mockito.doThrow(new IOException("kill failed")).when(runningJob).killJob();

    DataWriterMRJob mrJob = createMRJobWithMockCluster(runningJob);

    try {
      mrJob.close();
      Assert.fail("Expected IOException from killJob");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("kill failed"));
    }

    // Staging dir should still be cleaned up despite the killJob exception
    assertFalse(fs.exists(jobStagingDir));
  }

  @Test
  public void testCloseHandlesNonExistentStagingDir() throws IOException {
    JobID jobId = new JobID("test", 4);
    Path jobStagingDir = new Path(stagingAreaDir, jobId.toString());
    // Don't create the directory — it doesn't exist
    assertFalse(fs.exists(jobStagingDir));

    RunningJob runningJob = mock(RunningJob.class);
    doReturn(true).when(runningJob).isComplete();
    doReturn(jobId).when(runningJob).getID();

    DataWriterMRJob mrJob = createMRJobWithMockCluster(runningJob);
    // Should not throw when staging dir doesn't exist
    mrJob.close();
  }

  @Test
  public void testSetupInputFormatConfPlumbsWriterSchemasWhenProjecting() {
    Schema writerValueSchema = TestWriteUtils.NAME_RECORD_V1_SCHEMA;
    Schema writerRmdSchema = RmdSchemaGenerator.generateMetadataSchema(writerValueSchema, 1);

    PushJobSetting setting = avroProjectionPushJobSetting();
    setting.projectInputToWriterSchema = true;
    setting.writerValueSchemaString = writerValueSchema.toString();
    setting.replicationMetadataSchemaString = writerRmdSchema.toString();

    JobConf jobConf = new JobConf();
    new DataWriterMRJob().setupInputFormatConf(jobConf, setting);

    Assert.assertEquals(jobConf.get(WRITER_VALUE_SCHEMA_STRING_PROP), writerValueSchema.toString());
    Assert.assertEquals(jobConf.get(WRITER_RMD_SCHEMA_STRING_PROP), writerRmdSchema.toString());
  }

  @Test
  public void testSetupInputFormatConfOmitsWriterSchemasWhenNotProjecting() {
    PushJobSetting setting = avroProjectionPushJobSetting();
    setting.projectInputToWriterSchema = false;

    JobConf jobConf = new JobConf();
    new DataWriterMRJob().setupInputFormatConf(jobConf, setting);

    assertNull(jobConf.get(WRITER_VALUE_SCHEMA_STRING_PROP));
    assertNull(jobConf.get(WRITER_RMD_SCHEMA_STRING_PROP));
  }

  private PushJobSetting avroProjectionPushJobSetting() {
    PushJobSetting setting = new PushJobSetting();
    setting.isSourceKafka = false;
    setting.isAvro = true;
    setting.inputURI = System.getProperty("java.io.tmpdir");
    setting.keyField = "key";
    setting.valueField = "value";
    setting.rmdField = "rmd";
    setting.etlValueSchemaTransformation = ETLValueSchemaTransformation.NONE;
    setting.inputDataSchemaString = TestWriteUtils.STRING_TO_NAME_RECORD_V2_SCHEMA.toString();
    return setting;
  }
}

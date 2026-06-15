package com.linkedin.venice.vpj;

import static com.linkedin.venice.vpj.ExternalStorageWriteUtils.getDualWriteTargetRegions;
import static com.linkedin.venice.vpj.ExternalStorageWriteUtils.isDualWriteToExternalStorageFromVpjEnabled;
import static com.linkedin.venice.vpj.ExternalStorageWriteUtils.loadAndConfigure;
import static com.linkedin.venice.vpj.ExternalStorageWriteUtils.loadExternalStorageWriter;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PUSH_JOB_DUAL_WRITE_TARGET_REGIONS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PUSH_JOB_EXTERNAL_STORAGE_WRITER_CLASS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.task.datawriter.ExternalStorageRecord;
import com.linkedin.venice.hadoop.task.datawriter.ExternalStorageWriter;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;


public class ExternalStorageWriteUtilsTest {
  private static final String IMPL_CLASS = "com.example.LiveExternalWriter";

  private static VeniceProperties propsWithClass(String value) {
    return propsWith(value, "dc-0");
  }

  private static VeniceProperties propsWith(String writerClass, String dualWriteRegions) {
    Properties props = new Properties();
    if (writerClass != null) {
      props.setProperty(PUSH_JOB_EXTERNAL_STORAGE_WRITER_CLASS, writerClass);
    }
    if (dualWriteRegions != null) {
      props.setProperty(PUSH_JOB_DUAL_WRITE_TARGET_REGIONS, dualWriteRegions);
    }
    return new VeniceProperties(props);
  }

  @AfterMethod(alwaysRun = true)
  public void resetTestWriterState() {
    RecordingTestWriter.resetAll();
  }

  // --- isDualWriteToExternalStorageFromVpjEnabled -----------------------------------------------

  @Test
  public void bothHalvesSetReturnsTrue() {
    assertTrue(isDualWriteToExternalStorageFromVpjEnabled(propsWith(IMPL_CLASS, "dc-0")));
  }

  @Test
  public void writerClassMissingReturnsFalse() {
    assertFalse(isDualWriteToExternalStorageFromVpjEnabled(propsWith(null, "dc-0")));
  }

  @Test
  public void writerClassEmptyReturnsFalse() {
    assertFalse(isDualWriteToExternalStorageFromVpjEnabled(propsWith("", "dc-0")));
  }

  @Test
  public void noDualWriteRegionsReturnsFalse() {
    assertFalse(isDualWriteToExternalStorageFromVpjEnabled(propsWith(IMPL_CLASS, null)));
  }

  @Test
  public void emptyDualWriteRegionsReturnsFalse() {
    assertFalse(isDualWriteToExternalStorageFromVpjEnabled(propsWith(IMPL_CLASS, "")));
  }

  @Test
  public void nullPropsReturnsFalse() {
    assertFalse(isDualWriteToExternalStorageFromVpjEnabled(null));
  }

  // --- getDualWriteTargetRegions ----------------------------------------------------------------

  @Test
  public void parsesCommaSeparatedRegionsTrimmingBlanks() {
    assertEquals(
        getDualWriteTargetRegions(propsWith(IMPL_CLASS, "dc-0, dc-1 ,,dc-2")),
        Arrays.asList("dc-0", "dc-1", "dc-2"));
  }

  @Test
  public void parsesSingleRegion() {
    assertEquals(getDualWriteTargetRegions(propsWith(IMPL_CLASS, "dc-0")), Arrays.asList("dc-0"));
  }

  @Test
  public void absentRegionsYieldsEmptyList() {
    assertTrue(getDualWriteTargetRegions(propsWith(IMPL_CLASS, null)).isEmpty());
    assertTrue(getDualWriteTargetRegions(null).isEmpty());
  }

  @Test
  public void dedupsRepeatedRegionsPreservingFirstOccurrenceOrder() {
    assertEquals(
        getDualWriteTargetRegions(propsWith(IMPL_CLASS, "dc-1,dc-0,dc-1,dc-0")),
        Arrays.asList("dc-1", "dc-0"));
  }

  // --- loadExternalStorageWriter ----------------------------------------------------------------

  @Test
  public void loadExternalStorageWriterReturnsInstanceOfConfiguredImpl() {
    ExternalStorageWriter writer = loadExternalStorageWriter(RecordingTestWriter.class.getName());
    assertNotNull(writer);
    assertTrue(writer instanceof RecordingTestWriter);
  }

  @Test
  public void loadExternalStorageWriterFailsClearlyWhenClassMissing() {
    VeniceException e =
        expectThrows(VeniceException.class, () -> loadExternalStorageWriter("com.example.does.not.Exist"));
    assertTrue(
        e.getMessage().contains("Failed to load") && e.getMessage().contains("com.example.does.not.Exist"),
        "Expected message to identify the missing class; got: " + e.getMessage());
  }

  @Test
  public void loadExternalStorageWriterFailsClearlyWhenClassDoesNotImplementSpi() {
    // String.class is loadable but does not implement ExternalStorageWriter.
    VeniceException e = expectThrows(VeniceException.class, () -> loadExternalStorageWriter(String.class.getName()));
    assertTrue(
        e.getMessage().contains("does not implement") && e.getMessage().contains("ExternalStorageWriter"),
        "Expected message to call out the missing interface; got: " + e.getMessage());
  }

  // --- loadAndConfigure -------------------------------------------------------------------------

  @Test
  public void loadAndConfigureInvokesConfigureOnTheInstance() {
    ExternalStorageWriter writer =
        loadAndConfigure(RecordingTestWriter.class.getName(), propsWithClass(IMPL_CLASS), "store_v1", 7, "dc-0");
    assertTrue(writer instanceof RecordingTestWriter);
    RecordingTestWriter recorded = RecordingTestWriter.lastInstance();
    assertNotNull(recorded, "constructor should have published the instance");
    assertTrue(recorded.wasConfigured(), "configure() should have been invoked");
    assertFalse(recorded.wasClosed(), "close() should NOT have been called on the happy path");
  }

  @Test
  public void loadAndConfigureClosesWriterWhenConfigureThrows() {
    RecordingTestWriter.setThrowOnConfigure(true);
    RuntimeException raised = expectThrows(
        RuntimeException.class,
        () -> loadAndConfigure(RecordingTestWriter.class.getName(), propsWithClass(IMPL_CLASS), "store_v1", 7, "dc-0"));
    assertTrue(
        raised.getMessage().contains("simulated configure failure"),
        "Caller should see the original configure() exception; got: " + raised.getMessage());
    RecordingTestWriter recorded = RecordingTestWriter.lastInstance();
    assertNotNull(recorded, "constructor should have published the instance");
    assertTrue(
        recorded.wasClosed(),
        "Writer must be closed when configure() throws, to release any resources allocated by the no-arg ctor "
            + "or by partial configure() work");
  }

  /**
   * Public no-arg test impl loaded reflectively by the tests above. Per-test state lives on the instance
   * (not on statics) so SpotBugs' {@code ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD} stays quiet; the
   * constructor publishes the freshly-built instance to a static {@link AtomicReference} so tests can
   * reach it without needing the helper to return the writer.
   */
  public static class RecordingTestWriter implements ExternalStorageWriter {
    private static final AtomicReference<RecordingTestWriter> LAST_INSTANCE = new AtomicReference<>();
    private static final AtomicBoolean THROW_ON_CONFIGURE = new AtomicBoolean(false);

    private boolean configured = false;
    private boolean closed = false;

    public RecordingTestWriter() {
      LAST_INSTANCE.set(this);
    }

    static void setThrowOnConfigure(boolean value) {
      THROW_ON_CONFIGURE.set(value);
    }

    static RecordingTestWriter lastInstance() {
      return LAST_INSTANCE.get();
    }

    static void resetAll() {
      THROW_ON_CONFIGURE.set(false);
      LAST_INSTANCE.set(null);
    }

    boolean wasConfigured() {
      return configured;
    }

    boolean wasClosed() {
      return closed;
    }

    @Override
    public void configure(VeniceProperties jobProps, String topicName, int partitionId) {
      if (THROW_ON_CONFIGURE.get()) {
        throw new RuntimeException("simulated configure failure for " + topicName + "/" + partitionId);
      }
      configured = true;
    }

    @Override
    public void batchPut(List<ExternalStorageRecord> records) {
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() {
      closed = true;
    }
  }
}

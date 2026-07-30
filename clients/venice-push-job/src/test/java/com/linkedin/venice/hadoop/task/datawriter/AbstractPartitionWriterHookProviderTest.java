package com.linkedin.venice.hadoop.task.datawriter;

import static com.linkedin.venice.vpj.VenicePushJobConstants.PARTITION_COUNT;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PUSH_JOB_WRITER_HOOK_PROP_PREFIX;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PUSH_JOB_WRITER_HOOK_PROVIDER_CLASS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TELEMETRY_MESSAGE_INTERVAL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TOPIC_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALUE_SCHEMA_ID_PROP;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.engine.EngineTaskConfigProvider;
import com.linkedin.venice.meta.MaterializedViewParameters;
import com.linkedin.venice.meta.ViewConfig;
import com.linkedin.venice.meta.ViewConfigImpl;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.views.MaterializedView;
import com.linkedin.venice.views.ViewUtils;
import com.linkedin.venice.writer.ComplexVeniceWriter;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterHook;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class AbstractPartitionWriterHookProviderTest {
  private static final String TOPIC_NAME = "testStore_v1";
  private static final String JOB_NAME = "test-job";
  private static final int TASK_ID = 3;
  private static final int PARTITIONS = 8;

  @BeforeMethod
  public void resetProviders() {
    RecordingProvider.reset();
    NullHookProvider.CLOSE_COUNT.set(0);
  }

  @Test
  public void testUnconfiguredWriterHasNoHook() throws IOException {
    TestablePartitionWriter partitionWriter = configureWriter(createBaseProperties());
    try {
      VeniceWriterOptions options = createAndCaptureMainWriterOptions(partitionWriter);
      assertNull(options.getWriterHook());
    } finally {
      partitionWriter.close();
    }
  }

  @Test
  public void testConfiguredProviderInjectsHookAndReceivesContext() throws IOException {
    Properties properties = createBaseProperties();
    properties.setProperty(PUSH_JOB_WRITER_HOOK_PROVIDER_CLASS, " " + RecordingProvider.class.getName() + " ");
    properties.setProperty(PUSH_JOB_WRITER_HOOK_PROP_PREFIX + "test.setting", "test-value");

    TestablePartitionWriter partitionWriter = configureWriter(properties);
    try {
      VeniceWriterOptions options = createAndCaptureMainWriterOptions(partitionWriter);
      assertSame(options.getWriterHook(), RecordingProvider.HOOK);
      assertEquals(RecordingProvider.CREATE_COUNT.get(), 1);
      assertEquals(RecordingProvider.CONTEXT.get().getStoreName(), "testStore");
      assertEquals(RecordingProvider.CONTEXT.get().getTopicName(), TOPIC_NAME);
      assertEquals(RecordingProvider.CONTEXT.get().getJobName(), JOB_NAME);
      assertEquals(RecordingProvider.CONTEXT.get().getTaskId(), TASK_ID);
      assertEquals(RecordingProvider.CONTEXT.get().getPartitionCount(), PARTITIONS);
      assertEquals(
          RecordingProvider.CONTEXT.get()
              .getJobProperties()
              .getString(PUSH_JOB_WRITER_HOOK_PROP_PREFIX + "test.setting"),
          "test-value");
    } finally {
      partitionWriter.close();
    }
    assertEquals(RecordingProvider.CLOSE_COUNT.get(), 1);
  }

  @Test
  public void testHookIsNotAttachedToMaterializedViewChildWriter() throws IOException {
    Properties properties = createBaseProperties();
    properties.setProperty(PUSH_JOB_WRITER_HOOK_PROVIDER_CLASS, RecordingProvider.class.getName());
    MaterializedViewParameters.Builder viewParametersBuilder = new MaterializedViewParameters.Builder("testView");
    viewParametersBuilder.setPartitionCount(4);
    viewParametersBuilder.setPartitioner(DefaultVenicePartitioner.class.getName());
    ViewConfig viewConfig = new ViewConfigImpl(MaterializedView.class.getName(), viewParametersBuilder.build());
    properties.setProperty(
        ConfigKeys.PUSH_JOB_VIEW_CONFIGS,
        ViewUtils.flatViewConfigMapString(Collections.singletonMap("testView", viewConfig)));

    TestablePartitionWriter partitionWriter = configureWriter(properties);
    VeniceWriterFactory writerFactory = mock(VeniceWriterFactory.class);
    when(writerFactory.createVeniceWriter(any())).thenReturn(mock(VeniceWriter.class));
    when(writerFactory.createComplexVeniceWriter(any())).thenReturn(mock(ComplexVeniceWriter.class));
    partitionWriter.setVeniceWriterFactory(writerFactory);
    try {
      partitionWriter.createBasicVeniceWriter();

      ArgumentCaptor<VeniceWriterOptions> mainOptions = ArgumentCaptor.forClass(VeniceWriterOptions.class);
      ArgumentCaptor<VeniceWriterOptions> childOptions = ArgumentCaptor.forClass(VeniceWriterOptions.class);
      verify(writerFactory).createVeniceWriter(mainOptions.capture());
      verify(writerFactory).createComplexVeniceWriter(childOptions.capture());
      assertSame(mainOptions.getValue().getWriterHook(), RecordingProvider.HOOK);
      assertNull(childOptions.getValue().getWriterHook());
    } finally {
      partitionWriter.close();
    }
  }

  @Test
  public void testProviderReturningNullHookFailsAndIsClosed() {
    Properties properties = createBaseProperties();
    properties.setProperty(PUSH_JOB_WRITER_HOOK_PROVIDER_CLASS, NullHookProvider.class.getName());

    VeniceException exception = Assert.expectThrows(VeniceException.class, () -> configureWriter(properties));
    assertTrue(exception.getMessage().contains("returned a null hook"));
    assertEquals(NullHookProvider.CLOSE_COUNT.get(), 1);
  }

  @Test
  public void testMissingJobNameFailsBeforeProviderInitialization() {
    Properties properties = createBaseProperties();
    properties.setProperty(PUSH_JOB_WRITER_HOOK_PROVIDER_CLASS, RecordingProvider.class.getName());

    VeniceException exception = Assert.expectThrows(VeniceException.class, () -> configureWriter(properties, null));
    assertTrue(exception.getMessage().contains("Compute job name is required"));
    assertEquals(RecordingProvider.CREATE_COUNT.get(), 0);
    assertEquals(RecordingProvider.CLOSE_COUNT.get(), 0);
  }

  @Test
  public void testProviderContextRejectsNullJobName() {
    NullPointerException exception = Assert.expectThrows(
        NullPointerException.class,
        () -> new VeniceWriterHookProvider.Context(
            new VeniceProperties(createBaseProperties()),
            "testStore",
            TOPIC_NAME,
            null,
            TASK_ID,
            PARTITIONS));
    assertEquals(exception.getMessage(), "jobName");
  }

  @Test
  public void testConfiguredClassMustImplementProvider() {
    Properties properties = createBaseProperties();
    properties.setProperty(PUSH_JOB_WRITER_HOOK_PROVIDER_CLASS, String.class.getName());

    VeniceException exception = Assert.expectThrows(VeniceException.class, () -> configureWriter(properties));
    assertTrue(exception.getMessage().contains("does not implement " + VeniceWriterHookProvider.class.getName()));
  }

  @Test
  public void testProviderCloseFailurePropagates() {
    Properties properties = createBaseProperties();
    properties.setProperty(PUSH_JOB_WRITER_HOOK_PROVIDER_CLASS, ThrowingCloseProvider.class.getName());

    TestablePartitionWriter partitionWriter = configureWriter(properties);
    IOException exception = Assert.expectThrows(IOException.class, partitionWriter::close);
    assertEquals(exception.getMessage(), "provider close failed");
  }

  private TestablePartitionWriter configureWriter(Properties properties) {
    return configureWriter(properties, JOB_NAME);
  }

  private TestablePartitionWriter configureWriter(Properties properties, String jobName) {
    EngineTaskConfigProvider taskConfigProvider = mock(EngineTaskConfigProvider.class);
    when(taskConfigProvider.getJobProps()).thenReturn(properties);
    when(taskConfigProvider.getJobName()).thenReturn(jobName);
    when(taskConfigProvider.getTaskId()).thenReturn(TASK_ID);
    TestablePartitionWriter partitionWriter = new TestablePartitionWriter();
    partitionWriter.configure(taskConfigProvider);
    return partitionWriter;
  }

  private VeniceWriterOptions createAndCaptureMainWriterOptions(TestablePartitionWriter partitionWriter) {
    VeniceWriterFactory writerFactory = mock(VeniceWriterFactory.class);
    when(writerFactory.createVeniceWriter(any())).thenReturn(mock(VeniceWriter.class));
    partitionWriter.setVeniceWriterFactory(writerFactory);
    partitionWriter.createBasicVeniceWriter();

    ArgumentCaptor<VeniceWriterOptions> options = ArgumentCaptor.forClass(VeniceWriterOptions.class);
    verify(writerFactory).createVeniceWriter(options.capture());
    return options.getValue();
  }

  private Properties createBaseProperties() {
    Properties properties = new Properties();
    properties.setProperty(PARTITION_COUNT, Integer.toString(PARTITIONS));
    properties.setProperty(TOPIC_PROP, TOPIC_NAME);
    properties.setProperty(VALUE_SCHEMA_ID_PROP, "1");
    properties.setProperty(TELEMETRY_MESSAGE_INTERVAL, "10000");
    properties.setProperty(ConfigKeys.PARTITIONER_CLASS, DefaultVenicePartitioner.class.getName());
    return properties;
  }

  private static class TestablePartitionWriter extends AbstractPartitionWriter {
  }

  public static class RecordingProvider implements VeniceWriterHookProvider {
    private static final VeniceWriterHook HOOK = (operationType, keySizeBytes, valueSizeBytes) -> {};
    private static final AtomicReference<Context> CONTEXT = new AtomicReference<>();
    private static final AtomicInteger CREATE_COUNT = new AtomicInteger();
    private static final AtomicInteger CLOSE_COUNT = new AtomicInteger();

    private static void reset() {
      CONTEXT.set(null);
      CREATE_COUNT.set(0);
      CLOSE_COUNT.set(0);
    }

    @Override
    public VeniceWriterHook createWriterHook(Context context) {
      CONTEXT.set(context);
      CREATE_COUNT.incrementAndGet();
      return HOOK;
    }

    @Override
    public void close() {
      CLOSE_COUNT.incrementAndGet();
    }
  }

  public static class NullHookProvider implements VeniceWriterHookProvider {
    private static final AtomicInteger CLOSE_COUNT = new AtomicInteger();

    @Override
    public VeniceWriterHook createWriterHook(Context context) {
      return null;
    }

    @Override
    public void close() {
      CLOSE_COUNT.incrementAndGet();
    }
  }

  public static class ThrowingCloseProvider implements VeniceWriterHookProvider {
    @Override
    public VeniceWriterHook createWriterHook(Context context) {
      return RecordingProvider.HOOK;
    }

    @Override
    public void close() throws IOException {
      throw new IOException("provider close failed");
    }
  }
}

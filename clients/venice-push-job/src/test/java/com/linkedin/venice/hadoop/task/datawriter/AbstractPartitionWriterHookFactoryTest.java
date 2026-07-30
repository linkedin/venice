package com.linkedin.venice.hadoop.task.datawriter;

import static com.linkedin.venice.vpj.VenicePushJobConstants.PARTITION_COUNT;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PUSH_JOB_WRITER_HOOK_FACTORY_CLASS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PUSH_JOB_WRITER_HOOK_PROP_PREFIX;
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


public class AbstractPartitionWriterHookFactoryTest {
  private static final String TOPIC_NAME = "testStore_v1";
  private static final String JOB_NAME = "test-job";
  private static final int TASK_ID = 3;
  private static final int PARTITIONS = 8;

  @BeforeMethod
  public void resetFactories() {
    RecordingFactory.reset();
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
  public void testWhitespaceOnlyFactoryConfigurationHasNoHook() throws IOException {
    Properties properties = createBaseProperties();
    properties.setProperty(PUSH_JOB_WRITER_HOOK_FACTORY_CLASS, " \t ");

    TestablePartitionWriter partitionWriter = configureWriter(properties);
    try {
      VeniceWriterOptions options = createAndCaptureMainWriterOptions(partitionWriter);
      assertNull(options.getWriterHook());
      assertEquals(RecordingFactory.CREATE_COUNT.get(), 0);
    } finally {
      partitionWriter.close();
    }
  }

  @Test
  public void testConfiguredFactoryInjectsHookAndReceivesContext() throws IOException {
    Properties properties = createBaseProperties();
    properties.setProperty(PUSH_JOB_WRITER_HOOK_FACTORY_CLASS, " " + RecordingFactory.class.getName() + " ");
    properties.setProperty(PUSH_JOB_WRITER_HOOK_PROP_PREFIX + "test.setting", "test-value");

    TestablePartitionWriter partitionWriter = configureWriter(properties);
    try {
      VeniceWriterOptions options = createAndCaptureMainWriterOptions(partitionWriter);
      assertSame(options.getWriterHook(), RecordingFactory.HOOK);
      assertEquals(RecordingFactory.CREATE_COUNT.get(), 1);
      assertEquals(RecordingFactory.CONTEXT.get().getStoreName(), "testStore");
      assertEquals(RecordingFactory.CONTEXT.get().getTopicName(), TOPIC_NAME);
      assertEquals(RecordingFactory.CONTEXT.get().getJobName(), JOB_NAME);
      assertEquals(RecordingFactory.CONTEXT.get().getTaskId(), TASK_ID);
      assertEquals(RecordingFactory.CONTEXT.get().getPartitionCount(), PARTITIONS);
      assertEquals(
          RecordingFactory.CONTEXT.get()
              .getTaskProperties()
              .getString(PUSH_JOB_WRITER_HOOK_PROP_PREFIX + "test.setting"),
          "test-value");
    } finally {
      partitionWriter.close();
    }
  }

  @Test
  public void testHookIsNotAttachedToMaterializedViewChildWriter() throws IOException {
    Properties properties = createBaseProperties();
    properties.setProperty(PUSH_JOB_WRITER_HOOK_FACTORY_CLASS, RecordingFactory.class.getName());
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
      assertSame(mainOptions.getValue().getWriterHook(), RecordingFactory.HOOK);
      assertNull(childOptions.getValue().getWriterHook());
    } finally {
      partitionWriter.close();
    }
  }

  @Test
  public void testFactoryReturningNullHookFails() {
    Properties properties = createBaseProperties();
    properties.setProperty(PUSH_JOB_WRITER_HOOK_FACTORY_CLASS, NullHookFactory.class.getName());

    VeniceException exception = Assert.expectThrows(VeniceException.class, () -> configureWriter(properties));
    assertTrue(exception.getMessage().contains("returned a null hook"));
  }

  @Test
  public void testMissingJobNameFailsBeforeFactoryInitialization() {
    Properties properties = createBaseProperties();
    properties.setProperty(PUSH_JOB_WRITER_HOOK_FACTORY_CLASS, RecordingFactory.class.getName());

    VeniceException exception = Assert.expectThrows(VeniceException.class, () -> configureWriter(properties, null));
    assertTrue(exception.getMessage().contains("Compute job name is required"));
    assertEquals(RecordingFactory.CREATE_COUNT.get(), 0);
  }

  @Test
  public void testFactoryContextRejectsNullJobName() {
    NullPointerException exception = Assert.expectThrows(
        NullPointerException.class,
        () -> new VeniceWriterHookFactory.Context(
            new VeniceProperties(createBaseProperties()),
            "testStore",
            TOPIC_NAME,
            null,
            TASK_ID,
            PARTITIONS));
    assertEquals(exception.getMessage(), "jobName");
  }

  @Test
  public void testInvalidFactoryClassNameFailsClearly() {
    Properties properties = createBaseProperties();
    properties.setProperty(PUSH_JOB_WRITER_HOOK_FACTORY_CLASS, "com.example.DoesNotExist");

    VeniceException exception = Assert.expectThrows(VeniceException.class, () -> configureWriter(properties));
    assertTrue(exception.getMessage().contains("Failed to load VeniceWriterHookFactory class"));
  }

  @Test
  public void testConfiguredClassMustImplementFactory() {
    Properties properties = createBaseProperties();
    properties.setProperty(PUSH_JOB_WRITER_HOOK_FACTORY_CLASS, String.class.getName());

    VeniceException exception = Assert.expectThrows(VeniceException.class, () -> configureWriter(properties));
    assertTrue(exception.getMessage().contains("does not implement " + VeniceWriterHookFactory.class.getName()));
  }

  @Test
  public void testFactoryMustHavePublicNoArgConstructor() {
    Properties properties = createBaseProperties();
    properties.setProperty(PUSH_JOB_WRITER_HOOK_FACTORY_CLASS, FactoryWithoutNoArgConstructor.class.getName());

    VeniceException exception = Assert.expectThrows(VeniceException.class, () -> configureWriter(properties));
    assertTrue(exception.getMessage().contains("Failed to instantiate VeniceWriterHookFactory"));
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

  public static class RecordingFactory implements VeniceWriterHookFactory {
    private static final VeniceWriterHook HOOK = (operationType, keySizeBytes, valueSizeBytes) -> {};
    private static final AtomicReference<Context> CONTEXT = new AtomicReference<>();
    private static final AtomicInteger CREATE_COUNT = new AtomicInteger();

    private static void reset() {
      CONTEXT.set(null);
      CREATE_COUNT.set(0);
    }

    @Override
    public VeniceWriterHook createWriterHook(Context context) {
      CONTEXT.set(context);
      CREATE_COUNT.incrementAndGet();
      return HOOK;
    }
  }

  public static class NullHookFactory implements VeniceWriterHookFactory {
    @Override
    public VeniceWriterHook createWriterHook(Context context) {
      return null;
    }
  }

  public static class FactoryWithoutNoArgConstructor implements VeniceWriterHookFactory {
    public FactoryWithoutNoArgConstructor(String ignored) {
    }

    @Override
    public VeniceWriterHook createWriterHook(Context context) {
      return RecordingFactory.HOOK;
    }
  }
}

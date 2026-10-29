package com.linkedin.venice.hadoop.task.datawriter;

import static com.linkedin.venice.vpj.VenicePushJobConstants.INCREMENTAL_PUSH;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PARTITION_COUNT;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PUSH_JOB_WRITER_HOOK_CLASS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PUSH_JOB_WRITER_HOOK_PROP_PREFIX;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TELEMETRY_MESSAGE_INTERVAL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TOPIC_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALUE_SCHEMA_ID_PROP;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
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
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class AbstractPartitionWriterHookTest {
  private static final String TOPIC_NAME = "testStore_v1";
  private static final int TASK_ID = 3;
  private static final int PARTITIONS = 8;

  @BeforeMethod
  public void resetHooks() {
    RecordingHook.reset();
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
  public void testWhitespaceOnlyHookConfigurationHasNoHook() throws IOException {
    Properties properties = createBaseProperties();
    properties.setProperty(PUSH_JOB_WRITER_HOOK_CLASS, " \t ");

    TestablePartitionWriter partitionWriter = configureWriter(properties);
    try {
      VeniceWriterOptions options = createAndCaptureMainWriterOptions(partitionWriter);
      assertNull(options.getWriterHook());
      assertEquals(RecordingHook.CREATE_COUNT.get(), 0);
    } finally {
      partitionWriter.close();
    }
  }

  @Test
  public void testConfiguredHookReceivesStoreAndTaskProperties() throws IOException {
    Properties properties = createBaseProperties();
    properties.setProperty(PUSH_JOB_WRITER_HOOK_CLASS, " " + RecordingHook.class.getName() + " ");
    properties.setProperty(PUSH_JOB_WRITER_HOOK_PROP_PREFIX + "test.setting", "test-value");

    TestablePartitionWriter partitionWriter = configureWriter(properties);
    try {
      VeniceWriterOptions options = createAndCaptureMainWriterOptions(partitionWriter);
      assertSame(options.getWriterHook(), RecordingHook.INSTANCE.get());
      assertEquals(RecordingHook.CREATE_COUNT.get(), 1);
      assertEquals(RecordingHook.STORE_NAME.get(), "testStore");
      assertEquals(
          RecordingHook.TASK_PROPERTIES.get().getString(PUSH_JOB_WRITER_HOOK_PROP_PREFIX + "test.setting"),
          "test-value");
    } finally {
      partitionWriter.close();
    }
  }

  @Test
  public void testEachPartitionWriterConstructsItsOwnHook() throws IOException {
    Properties properties = createBaseProperties();
    properties.setProperty(PUSH_JOB_WRITER_HOOK_CLASS, RecordingHook.class.getName());
    TestablePartitionWriter firstWriter = configureWriter(properties);
    try {
      VeniceWriterHook firstHook = createAndCaptureMainWriterOptions(firstWriter).getWriterHook();
      TestablePartitionWriter secondWriter = configureWriter(properties);
      try {
        VeniceWriterHook secondHook = createAndCaptureMainWriterOptions(secondWriter).getWriterHook();
        assertNotSame(firstHook, secondHook);
        assertEquals(RecordingHook.CREATE_COUNT.get(), 2);
      } finally {
        secondWriter.close();
      }
    } finally {
      firstWriter.close();
    }
  }

  @DataProvider
  public Object[][] incrementalPushTopics() {
    return new Object[][] { { "testStore_rt" }, { "testStore_rt_v2" }, { "testStore_rt_v2_sep" } };
  }

  @Test(dataProvider = "incrementalPushTopics")
  public void testIncrementalPushHookReceivesStoreName(String topic) throws IOException {
    Properties properties = createBaseProperties();
    properties.setProperty(TOPIC_PROP, topic);
    properties.setProperty(INCREMENTAL_PUSH, "true");
    properties.setProperty(PUSH_JOB_WRITER_HOOK_CLASS, RecordingHook.class.getName());

    TestablePartitionWriter partitionWriter = configureWriter(properties);
    try {
      assertSame(createAndCaptureMainWriterOptions(partitionWriter).getWriterHook(), RecordingHook.INSTANCE.get());
      assertEquals(RecordingHook.STORE_NAME.get(), "testStore");
      assertTrue(RecordingHook.TASK_PROPERTIES.get().getBoolean(INCREMENTAL_PUSH));
    } finally {
      partitionWriter.close();
    }
  }

  @Test
  public void testHookIsNotAttachedToMaterializedViewChildWriter() throws IOException {
    Properties properties = createBaseProperties();
    properties.setProperty(PUSH_JOB_WRITER_HOOK_CLASS, RecordingHook.class.getName());
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
      assertSame(mainOptions.getValue().getWriterHook(), RecordingHook.INSTANCE.get());
      assertNull(childOptions.getValue().getWriterHook());
    } finally {
      partitionWriter.close();
    }
  }

  @Test
  public void testInvalidHookClassNameFailsClearly() {
    Properties properties = createBaseProperties();
    properties.setProperty(PUSH_JOB_WRITER_HOOK_CLASS, "com.example.DoesNotExist");

    VeniceException exception = Assert.expectThrows(VeniceException.class, () -> configureWriter(properties));
    assertTrue(exception.getMessage().contains("Failed to load VeniceWriterHook class 'com.example.DoesNotExist'"));
  }

  @Test
  public void testHookClassInitializationFailureIncludesClassAndCause() {
    Properties properties = createBaseProperties();
    properties.setProperty(PUSH_JOB_WRITER_HOOK_CLASS, LinkageFailureHook.class.getName());

    VeniceException exception = Assert.expectThrows(VeniceException.class, () -> configureWriter(properties));
    assertTrue(exception.getMessage().contains("Failed to load VeniceWriterHook class"));
    assertTrue(exception.getMessage().contains(LinkageFailureHook.class.getName()));
    assertTrue(exception.getCause() instanceof NoClassDefFoundError);
  }

  @Test
  public void testConfiguredClassMustImplementHook() {
    Properties properties = createBaseProperties();
    properties.setProperty(PUSH_JOB_WRITER_HOOK_CLASS, String.class.getName());

    VeniceException exception = Assert.expectThrows(VeniceException.class, () -> configureWriter(properties));
    assertTrue(exception.getMessage().contains("does not implement " + VeniceWriterHook.class.getName()));
  }

  @Test
  public void testHookMustHavePublicContextConstructor() {
    Properties properties = createBaseProperties();
    properties.setProperty(PUSH_JOB_WRITER_HOOK_CLASS, HookWithoutContextConstructor.class.getName());

    VeniceException exception = Assert.expectThrows(VeniceException.class, () -> configureWriter(properties));
    assertTrue(exception.getMessage().contains("Failed to instantiate VeniceWriterHook"));
    assertTrue(exception.getMessage().contains("constructor (String, VeniceProperties)"));
  }

  @Test
  public void testHookConstructorFailureIncludesClassAndCause() {
    Properties properties = createBaseProperties();
    properties.setProperty(PUSH_JOB_WRITER_HOOK_CLASS, FailingHook.class.getName());

    VeniceException exception = Assert.expectThrows(VeniceException.class, () -> configureWriter(properties));
    assertTrue(exception.getMessage().contains(FailingHook.class.getName()));
    assertTrue(exception.getMessage().contains("Failed to instantiate VeniceWriterHook"));
    assertTrue(exception.getCause() instanceof IllegalStateException);
    assertEquals(exception.getCause().getMessage(), "hook initialization failed");
  }

  private TestablePartitionWriter configureWriter(Properties properties) {
    EngineTaskConfigProvider taskConfigProvider = mock(EngineTaskConfigProvider.class);
    when(taskConfigProvider.getJobProps()).thenReturn(properties);
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

  public static class RecordingHook implements VeniceWriterHook {
    private static final AtomicReference<RecordingHook> INSTANCE = new AtomicReference<>();
    private static final AtomicReference<String> STORE_NAME = new AtomicReference<>();
    private static final AtomicReference<VeniceProperties> TASK_PROPERTIES = new AtomicReference<>();
    private static final AtomicInteger CREATE_COUNT = new AtomicInteger();

    private static void reset() {
      INSTANCE.set(null);
      STORE_NAME.set(null);
      TASK_PROPERTIES.set(null);
      CREATE_COUNT.set(0);
    }

    public RecordingHook(String storeName, VeniceProperties taskProperties) {
      INSTANCE.set(this);
      STORE_NAME.set(storeName);
      TASK_PROPERTIES.set(taskProperties);
      CREATE_COUNT.incrementAndGet();
    }

    @Override
    public void onBeforeProduce(OperationType operationType, int keySizeBytes, int valueSizeBytes) {
    }
  }

  public static class HookWithoutContextConstructor implements VeniceWriterHook {
    public HookWithoutContextConstructor() {
    }

    @Override
    public void onBeforeProduce(OperationType operationType, int keySizeBytes, int valueSizeBytes) {
    }
  }

  public static class FailingHook implements VeniceWriterHook {
    public FailingHook(String storeName, VeniceProperties taskProperties) {
      throw new IllegalStateException("hook initialization failed");
    }

    @Override
    public void onBeforeProduce(OperationType operationType, int keySizeBytes, int valueSizeBytes) {
    }
  }

  public static class LinkageFailureHook implements VeniceWriterHook {
    static {
      failInitialization();
    }

    private static void failInitialization() {
      throw new NoClassDefFoundError("missing hook dependency");
    }

    public LinkageFailureHook(String storeName, VeniceProperties taskProperties) {
    }

    @Override
    public void onBeforeProduce(OperationType operationType, int keySizeBytes, int valueSizeBytes) {
    }
  }
}

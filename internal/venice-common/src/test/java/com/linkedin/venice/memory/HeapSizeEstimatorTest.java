package com.linkedin.venice.memory;

import static com.linkedin.venice.kafka.protocol.enums.MessageType.PUT;
import static com.linkedin.venice.memory.HeapSizeEstimator.getClassOverhead;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.fail;

import com.linkedin.davinci.kafka.consumer.StoreBufferService;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.LeaderMetadata;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.ImmutablePubSubMessage;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class HeapSizeEstimatorTest {
  private static final Logger LOGGER = LogManager.getLogger(HeapSizeEstimatorTest.class);
  private static final int[] RESULT_ROW_CELL_LENGTHS =
      new int[] { 6, SubSubClassWithThreePrimitiveBooleanFields.class.getSimpleName().length(), 9, 9 };
  /**
   * Some scenarios are tricky to compute dynamically without just copy-pasting the whole main code, so we just skip it
   * for now, though we could come back to it later...
   */
  private static final int SKIP_EXPECTED_FIELD_OVERHEAD = -1;
  private static final Runtime RUNTIME = Runtime.getRuntime();
  private static final int NUMBER_OF_ALLOCATIONS_WHEN_MEASURING = 200_000;
  private static final int JAVA_MAJOR_VERSION = Utils.getJavaMajorVersion();
  private static final int BOOLEAN_SIZE = 1;
  private static final int ALIGNMENT_SIZE;
  private static final int OBJECT_HEADER_SIZE;
  private static final int ARRAY_HEADER_SIZE;
  private static final int POINTER_SIZE;

  static {
    // This duplicates the main code, which is not ideal, but there isn't much choice if we want the test to run in
    // various JVM scenarios...
    boolean is64bitsJVM = HeapSizeEstimator.is64bitsJVM();
    int markWordSize = is64bitsJVM ? 8 : 4;
    boolean isCompressedOopsEnabled = HeapSizeEstimator.isUseCompressedOopsEnabled();
    boolean isCompressedKlassPointersEnabled = HeapSizeEstimator.isCompressedKlassPointersEnabled();
    int classPointerSize = isCompressedKlassPointersEnabled ? 4 : 8;

    ALIGNMENT_SIZE = is64bitsJVM ? 8 : 4;
    OBJECT_HEADER_SIZE = markWordSize + classPointerSize;
    ARRAY_HEADER_SIZE = roundUpToNearestAlignment(OBJECT_HEADER_SIZE + Integer.BYTES);
    POINTER_SIZE = isCompressedOopsEnabled ? 4 : 8;
  }

  private interface TestFunction {
    default void test(Class c) {
      test(c, SKIP_EXPECTED_FIELD_OVERHEAD, null);
    }

    default void test(Class c, Supplier<Object> constructor) {
      test(c, SKIP_EXPECTED_FIELD_OVERHEAD, constructor);
    }

    default void test(Class c, int expectedFieldOverhead) {
      test(c, expectedFieldOverhead, null);
    }

    void test(Class c, int expectedFieldOverhead, Supplier<Object> constructor);
  }

  private enum TestMethodology {
    THEORETICAL_EXPECTATION(
        HeapSizeEstimatorTest::theoreticalExpectation, new String[] { "status", "Class name", "predicted", "expected" }
    ),
    EMPIRICAL_MEASUREMENT(
        HeapSizeEstimatorTest::empiricalClassMeasurement,
        new String[] { "status", "class name", "predicted", "allocated" }
    );

    final TestFunction tf;
    final String[] resultsTableHeader;

    TestMethodology(TestFunction tf, String[] resultsTableHeader) {
      this.tf = tf;
      this.resultsTableHeader = resultsTableHeader;
    }
  }

  @DataProvider
  public Object[][] testMethodologies() {
    return new Object[][] { { TestMethodology.THEORETICAL_EXPECTATION }, { TestMethodology.EMPIRICAL_MEASUREMENT } };
  }

  @BeforeTest
  public void preTest() {
    System.gc();
  }

  @Test
  public void testParamValidation() {
    assertThrows(NullPointerException.class, () -> getClassOverhead(null, false));
  }

  @Test(dataProvider = "testMethodologies")
  public void testClassOverhead(TestMethodology tm) {
    printHeader(tm.resultsTableHeader);

    // Most basic case... just a plain Object.
    tm.tf.test(Object.class, 0);

    // Ensure that inheritance (in and of itself) adds no overhead.
    tm.tf.test(SubclassOfObjectWithNoFields.class, 0);

    // Ensure that one public primitive fields within a single class is accounted.
    tm.tf.test(ClassWithOnePublicPrimitiveBooleanField.class, BOOLEAN_SIZE);
    tm.tf.test(ClassWithOnePublicPrimitiveByteField.class, Byte.BYTES);
    tm.tf.test(ClassWithOnePublicPrimitiveCharField.class, Character.BYTES);
    tm.tf.test(ClassWithOnePublicPrimitiveShortField.class, Short.BYTES);
    tm.tf.test(ClassWithOnePublicPrimitiveIntField.class, Integer.BYTES);
    tm.tf.test(ClassWithOnePublicPrimitiveFloatField.class, Float.BYTES);
    tm.tf.test(ClassWithOnePublicPrimitiveLongField.class, Long.BYTES);
    tm.tf.test(ClassWithOnePublicPrimitiveDoubleField.class, Double.BYTES);

    // Ensure that two private primitive fields within a single class are accounted.
    tm.tf.test(ClassWithTwoPrimitiveBooleanFields.class, BOOLEAN_SIZE * 2);
    tm.tf.test(ClassWithTwoPrimitiveByteFields.class, Byte.BYTES * 2);
    tm.tf.test(ClassWithTwoPrimitiveCharFields.class, Character.BYTES * 2);
    tm.tf.test(ClassWithTwoPrimitiveShortFields.class, Short.BYTES * 2);
    tm.tf.test(ClassWithTwoPrimitiveIntFields.class, Integer.BYTES * 2);
    tm.tf.test(ClassWithTwoPrimitiveFloatFields.class, Float.BYTES * 2);
    tm.tf.test(ClassWithTwoPrimitiveLongFields.class, Long.BYTES * 2);
    tm.tf.test(ClassWithTwoPrimitiveDoubleFields.class, Double.BYTES * 2);

    // Ensure that a mix of public and private fields across the class hierarchy are accounted.
    if (JAVA_MAJOR_VERSION < 15) {
      // TODO: Plug in correct expected field size for these JVMs...
      tm.tf.test(SubClassWithTwoPrimitiveBooleanFields.class);
      tm.tf.test(SubClassWithTwoPrimitiveByteFields.class);
      tm.tf.test(SubClassWithTwoPrimitiveCharFields.class);
      tm.tf.test(SubClassWithTwoPrimitiveShortFields.class);

      tm.tf.test(SubSubClassWithThreePrimitiveBooleanFields.class);
      tm.tf.test(SubSubClassWithThreePrimitiveByteFields.class);
      tm.tf.test(SubSubClassWithThreePrimitiveCharFields.class);
      tm.tf.test(SubSubClassWithThreePrimitiveShortFields.class);
    } else {
      tm.tf.test(SubClassWithTwoPrimitiveBooleanFields.class, BOOLEAN_SIZE * 2);
      tm.tf.test(SubClassWithTwoPrimitiveByteFields.class, Byte.BYTES * 2);
      tm.tf.test(SubClassWithTwoPrimitiveCharFields.class, Character.BYTES * 2);
      tm.tf.test(SubClassWithTwoPrimitiveShortFields.class, Short.BYTES * 2);

      tm.tf.test(SubSubClassWithThreePrimitiveBooleanFields.class, BOOLEAN_SIZE * 3);
      tm.tf.test(SubSubClassWithThreePrimitiveByteFields.class, Byte.BYTES * 3);
      tm.tf.test(SubSubClassWithThreePrimitiveCharFields.class, Character.BYTES * 3);
      tm.tf.test(SubSubClassWithThreePrimitiveShortFields.class, Short.BYTES * 3);
    }

    tm.tf.test(SubClassWithTwoPrimitiveIntFields.class, Integer.BYTES * 2);
    tm.tf.test(SubClassWithTwoPrimitiveFloatFields.class, Float.BYTES * 2);
    tm.tf.test(SubClassWithTwoPrimitiveLongFields.class, Long.BYTES * 2);
    tm.tf.test(SubClassWithTwoPrimitiveDoubleFields.class, Double.BYTES * 2);

    tm.tf.test(SubSubClassWithThreePrimitiveIntFields.class, Integer.BYTES * 3);
    tm.tf.test(SubSubClassWithThreePrimitiveFloatFields.class, Float.BYTES * 3);
    tm.tf.test(SubSubClassWithThreePrimitiveLongFields.class, Long.BYTES * 3);
    tm.tf.test(SubSubClassWithThreePrimitiveDoubleFields.class, Double.BYTES * 3);

    // Ensure that pointers are properly accounted.
    int classWithThreeObjectPointersFieldOverhead = (POINTER_SIZE + roundUpToNearestAlignment(OBJECT_HEADER_SIZE)) * 3;
    tm.tf.test(ClassWithThreeObjectPointers.class, classWithThreeObjectPointersFieldOverhead);

    // Ensure that arrays are properly accounted.
    int classWithArrayFieldOverhead = POINTER_SIZE + ARRAY_HEADER_SIZE;
    tm.tf.test(ClassWithArray.class, classWithArrayFieldOverhead);

    /**
     * Ensure that field packing and ordering is accounted.
     *
     * Note that we don't actually do anything special about packing and ordering, and things still work. The ordering
     * can have implications in terms of padding fields to protect against false sharing, but does not appear to have
     * a concrete effect on memory utilization (or at least, none of the current test cases expose such issue).
     */
    tm.tf.test(FieldPacking.class);
    tm.tf.test(FieldOrder.class);

    // Put it all together...
    tm.tf.test(
        ComplexClass.class,
        POINTER_SIZE * 10 + roundUpToNearestAlignment(OBJECT_HEADER_SIZE + BOOLEAN_SIZE * 2)
            + roundUpToNearestAlignment(OBJECT_HEADER_SIZE + Byte.BYTES * 2)
            + roundUpToNearestAlignment(OBJECT_HEADER_SIZE + Character.BYTES * 2)
            + roundUpToNearestAlignment(OBJECT_HEADER_SIZE + Short.BYTES * 2)
            + roundUpToNearestAlignment(OBJECT_HEADER_SIZE + Integer.BYTES * 2)
            + roundUpToNearestAlignment(OBJECT_HEADER_SIZE + Float.BYTES * 2)
            + roundUpToNearestAlignment(OBJECT_HEADER_SIZE + Long.BYTES * 2)
            + roundUpToNearestAlignment(OBJECT_HEADER_SIZE + Double.BYTES * 2)
            + roundUpToNearestAlignment(OBJECT_HEADER_SIZE + classWithThreeObjectPointersFieldOverhead)
            + roundUpToNearestAlignment(OBJECT_HEADER_SIZE + classWithArrayFieldOverhead));

    // Test the Venice main code classes we care about measuring.
    tm.tf.test(KafkaKey.class, () -> new KafkaKey(PUT, new byte[0]));

    printResultSeparatorLine();
  }

  private static void theoreticalExpectation(Class c, int expectedFieldOverhead, Supplier<Object> constructor) {
    int predictedClassOverhead = getClassOverhead(c, false);
    int expectedClassOverheadWithoutAlignment = OBJECT_HEADER_SIZE + expectedFieldOverhead;
    if (expectedFieldOverhead != SKIP_EXPECTED_FIELD_OVERHEAD) {
      int expectedClassOverhead = roundUpToNearestAlignment(expectedClassOverheadWithoutAlignment);
      boolean success = predictedClassOverhead == expectedClassOverhead;
      printResultRow(
          status(success, 1, 1),
          c.getSimpleName(),
          String.valueOf(predictedClassOverhead),
          String.valueOf(expectedClassOverhead));
      assertEquals(predictedClassOverhead, expectedClassOverhead);
    }
  }

  private static void empiricalClassMeasurement(Class c, int expectedFieldOverhead, Supplier<Object> constructor) {
    empiricalMeasurement(c, getClassOverhead(c, false), constructor);
  }

  private static void empiricalInstanceMeasurement(Class c, Supplier<?> constructor) {
    Object o = constructor.get();
    int expectedSize = MeasurableUtils.getObjectSize(o);
    empiricalMeasurement(c, expectedSize, constructor);
  }

  @Test
  public void testInstanceMeasurement() throws ClassNotFoundException {
    printHeader(TestMethodology.EMPIRICAL_MEASUREMENT.resultsTableHeader);

    // Try various array sizes to take alignment into account.
    List<Supplier<KafkaKey>> kafkaKeySuppliers = new ArrayList<>();
    kafkaKeySuppliers.add(() -> new KafkaKey(PUT, new byte[0]));
    kafkaKeySuppliers.add(() -> new KafkaKey(PUT, new byte[2]));
    kafkaKeySuppliers.add(() -> new KafkaKey(PUT, new byte[4]));
    kafkaKeySuppliers.add(() -> new KafkaKey(PUT, new byte[6]));
    kafkaKeySuppliers.add(() -> new KafkaKey(PUT, new byte[8]));
    kafkaKeySuppliers.add(() -> new KafkaKey(PUT, new byte[10]));

    for (Supplier kafkaKeySupplier: kafkaKeySuppliers) {
      empiricalInstanceMeasurement(KafkaKey.class, kafkaKeySupplier);
    }

    Supplier<ProducerMetadata> producerMetadataSupplier = () -> new ProducerMetadata(new GUID(), 0, 0, 0L, 0L);
    empiricalInstanceMeasurement(ProducerMetadata.class, producerMetadataSupplier);

    Supplier<Put> rtPutSupplier = () -> new Put(ByteBuffer.allocate(10), 1, -1, null);
    empiricalInstanceMeasurement(Put.class, rtPutSupplier);

    PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
    /** The {@link PubSubTopicPartition} is supposed to be a shared instance, but it cannot be null. */
    PubSubTopicPartition pubSubTopicPartition =
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("topic"), 0);

    VeniceWriter.DefaultLeaderMetadata defaultLeaderMetadata = new VeniceWriter.DefaultLeaderMetadata("blah");

    List<Supplier<KafkaMessageEnvelope>> kmeSuppliers = new ArrayList<>();
    kmeSuppliers.add(
        () -> new KafkaMessageEnvelope(
            // What a message in the RT topic might look like
            PUT.getValue(),
            producerMetadataSupplier.get(),
            rtPutSupplier.get(),
            // The (improperly-named) "leader" metadata footer is always populated, but in this path it points to a
            // static instance.
            defaultLeaderMetadata));
    kmeSuppliers.add(() -> {
      // What a VT message produced by a leader replica might look like
      byte[] rawKafkaValue = new byte[50];
      return new KafkaMessageEnvelope(
          PUT.getValue(),
          producerMetadataSupplier.get(),
          new Put(ByteBuffer.wrap(rawKafkaValue, 10, 10), 1, 1, ByteBuffer.wrap(rawKafkaValue, 25, 10)),
          new LeaderMetadata(
              null, // shared instance
              0L,
              0));
    });
    // TODO: Add updates, deletes...

    for (Supplier<KafkaMessageEnvelope> kmeSupplier: kmeSuppliers) {
      empiricalInstanceMeasurement(KafkaMessageEnvelope.class, kmeSupplier);
    }

    BiFunction<Supplier<KafkaKey>, Supplier<KafkaMessageEnvelope>, PubSubMessage> psmProvider =
        (kafkaKeySupplier, kmeSupplier) -> new ImmutablePubSubMessage<>(
            kafkaKeySupplier.get(),
            kmeSupplier.get(),
            pubSubTopicPartition,
            0,
            0,
            0);

    for (Supplier<KafkaKey> kafkaKeySupplier: kafkaKeySuppliers) {
      for (Supplier<KafkaMessageEnvelope> kmeSupplier: kmeSuppliers) {
        empiricalInstanceMeasurement(
            ImmutablePubSubMessage.class,
            () -> psmProvider.apply(kafkaKeySupplier, kmeSupplier));
      }
    }

    for (Supplier<KafkaKey> kafkaKeySupplier: kafkaKeySuppliers) {
      for (Supplier<KafkaMessageEnvelope> kmeSupplier: kmeSuppliers) {
        empiricalInstanceMeasurement(
            StoreBufferService.QueueNode.class,
            () -> new StoreBufferService.QueueNode(psmProvider.apply(kafkaKeySupplier, kmeSupplier), null, null, 0));
      }
    }

    printResultSeparatorLine();
  }

  private static void empiricalMeasurement(Class c, int predictedUsage, Supplier<?> constructor) {
    /**
     * The reason for having multiple attempts is that the allocation measurement method is not always reliable.
     * Presumably, this is because GC could kick in during the middle of the allocation loop. If the allocated memory
     * is negative then for sure it's not right. If the GC reduces memory allocated but not enough to make the
     * measurement go negative, then we cannot know if it's a measurement error, or a bug... In any case, we will do
     * a few attempts and assume that the measurement is good if it falls within the prescribed delta (even though
     * technically that could be a false negative).
     */
    int currentAttempt = 0;
    int totalAttempts = 3;
    while (currentAttempt++ < totalAttempts) {
      assertNotEquals(RUNTIME.maxMemory(), Long.MAX_VALUE);
      Object[] allocations = new Object[NUMBER_OF_ALLOCATIONS_WHEN_MEASURING];

      if (constructor == null) {
        Class<?>[] argTypes = new Class[0];
        Object[] args = new Object[0];
        Constructor<?> reflectiveConstructor;
        try {
          reflectiveConstructor = c.getConstructor(argTypes);
        } catch (NoSuchMethodException e) {
          fail("Could not get a no-arg constructor for " + c.getSimpleName(), e);
          throw new RuntimeException(e);
        }

        constructor = () -> {
          try {
            return reflectiveConstructor.newInstance(args);
          } catch (Exception e) {
            fail("Could not invoke the no-arg constructor for " + c.getSimpleName(), e);
          }
          return null; // Unreachable code, just to appease the compiler
        };
      }

      long memoryAllocatedBeforeInstantiations = getCurrentlyAllocatedMemory();

      for (int i = 0; i < NUMBER_OF_ALLOCATIONS_WHEN_MEASURING; i++) {
        allocations[i] = constructor.get();
      }

      long memoryAllocatedAfterInstantiations = getCurrentlyAllocatedMemory();
      long memoryAllocatedByInstantiations = memoryAllocatedAfterInstantiations - memoryAllocatedBeforeInstantiations;
      if (memoryAllocatedByInstantiations < 0) {
        String errorMessage = "Memory allocated is negative! memoryAllocatedBeforeInstantiations: "
            + memoryAllocatedBeforeInstantiations + "; memoryAllocatedAfterInstantiations: "
            + memoryAllocatedAfterInstantiations + "; memoryAllocatedByInstantiations: "
            + memoryAllocatedByInstantiations + "; " + currentAttempt + " attempts left.";
        if (currentAttempt < totalAttempts) {
          LOGGER.info(errorMessage);
          continue;
        } else {
          fail(errorMessage);
        }
      }

      double memoryAllocatedPerInstance =
          (double) memoryAllocatedByInstantiations / (double) NUMBER_OF_ALLOCATIONS_WHEN_MEASURING;

      for (int i = 0; i < NUMBER_OF_ALLOCATIONS_WHEN_MEASURING; i++) {
        assertNotNull(allocations[i]);
      }

      // Since the above method for measuring allocated memory is imperfect, we need to tolerate some delta.
      double allocatedToPredictedRatio = memoryAllocatedPerInstance / (double) predictedUsage;
      double delta = Math.abs(1 - allocatedToPredictedRatio);

      // For small objects, any delta of 1 byte or less will be tolerated
      double minimumAbsoluteDeltaInBytes = 1;
      double minimumAbsoluteDelta = minimumAbsoluteDeltaInBytes / memoryAllocatedPerInstance;

      // For larger objects, we'll tolerate up to 1% delta
      double minimumRelativeDelta = 0.01;

      // The larger of the two deltas is the one we use
      double maxAllowedDelta = Math.max(minimumAbsoluteDelta, minimumRelativeDelta);

      boolean success = delta < maxAllowedDelta;

      printResultRow(
          status(success, currentAttempt, totalAttempts),
          c.getSimpleName(),
          String.valueOf(predictedUsage),
          String.format("%.3f", memoryAllocatedPerInstance));

      // A best-effort attempt to minimize the chance of needing to GC in the middle of the next measurement run...
      allocations = null;
      System.gc();

      if (success) {
        break; // No more attempts needed if the allocation measurement and all assertions succeeded
      } else if (currentAttempt == totalAttempts) {
        fail(
            "Class " + c.getSimpleName() + " has a memoryAllocatedPerInstance (" + memoryAllocatedPerInstance
                + ") which is too far from the predictedUsage (" + predictedUsage + "); delta: "
                + String.format("%.3f", delta) + "; maxAllowedDelta: " + String.format("%.3f", maxAllowedDelta)
                + ". No more attempts left.");
      }
    }
  }

  /** Different algo that the main code because why not? It should be equivalent... */
  private static int roundUpToNearestAlignment(int size) {
    double numberOfAlignmentWindowsFittingWithinTheSize = (double) size / ALIGNMENT_SIZE;
    double roundedUp = Math.ceil(numberOfAlignmentWindowsFittingWithinTheSize);
    int finalSize = (int) roundedUp * ALIGNMENT_SIZE;
    return finalSize;
  }

  private static long getCurrentlyAllocatedMemory() {
    System.gc();
    return RUNTIME.maxMemory() - RUNTIME.freeMemory();
  }

  private static void printHeader(String... headerCells) {
    LOGGER.info("Java major version: " + JAVA_MAJOR_VERSION);
    LOGGER.info("Alignment size: " + ALIGNMENT_SIZE);
    LOGGER.info("Object header size: " + OBJECT_HEADER_SIZE);
    LOGGER.info("Array header size: " + ARRAY_HEADER_SIZE);
    LOGGER.info("Pointer size: " + POINTER_SIZE);

    LOGGER.info("");
    printResultSeparatorLine();
    printResultRow(headerCells);
    printResultSeparatorLine();
  }

  private static void printResultSeparatorLine() {
    StringBuilder sb = new StringBuilder();
    sb.append(" ");
    for (int i = 0; i < RESULT_ROW_CELL_LENGTHS.length; i++) {
      sb.append("+-");
      for (int j = 0; j < RESULT_ROW_CELL_LENGTHS[i] + 1; j++) {
        sb.append('-');
      }
    }
    sb.append("+");
    LOGGER.info(sb.toString());
  }

  private static String status(boolean success, int currentAttempt, int totalAttempts) {
    String symbol = new String(Character.toChars(success ? 0x2705 : 0x274C));
    if (totalAttempts == 1) {
      return symbol;
    }
    return symbol + " " + currentAttempt + "/" + totalAttempts;
  }

  private static void printResultRow(String... cells) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < cells.length; i++) {
      sb.append(" | ");
      String cell = cells[i];
      sb.append(cell);

      int remainder = RESULT_ROW_CELL_LENGTHS[i] - cell.length();
      for (Character character: cell.toCharArray()) {
        if (Character.UnicodeBlock.of(character) != Character.UnicodeBlock.BASIC_LATIN) {
          // Emoticons take two characters' width
          remainder--;
        }
      }

      for (int j = 0; j < remainder; j++) {
        sb.append(' ');
      }
    }
    sb.append(" |");
    LOGGER.info(sb.toString());
  }

  private static class SubclassOfObjectWithNoFields {
    public SubclassOfObjectWithNoFields() {
    }
  }

  private static class ClassWithOnePublicPrimitiveBooleanField {
    public boolean publicField;

    public ClassWithOnePublicPrimitiveBooleanField() {
    }
  }

  private static class ClassWithOnePublicPrimitiveByteField {
    public byte publicField;

    public ClassWithOnePublicPrimitiveByteField() {
    }
  }

  private static class ClassWithOnePublicPrimitiveCharField {
    public char publicField;

    public ClassWithOnePublicPrimitiveCharField() {
    }
  }

  private static class ClassWithOnePublicPrimitiveShortField {
    public short publicField;

    public ClassWithOnePublicPrimitiveShortField() {
    }
  }

  private static class ClassWithOnePublicPrimitiveIntField {
    public int publicField;

    public ClassWithOnePublicPrimitiveIntField() {
    }
  }

  private static class ClassWithOnePublicPrimitiveFloatField {
    public float publicField;

    public ClassWithOnePublicPrimitiveFloatField() {
    }
  }

  private static class ClassWithOnePublicPrimitiveLongField {
    public long publicField;

    public ClassWithOnePublicPrimitiveLongField() {
    }
  }

  private static class ClassWithOnePublicPrimitiveDoubleField {
    public double publicField;

    public ClassWithOnePublicPrimitiveDoubleField() {
    }
  }

  private static class ClassWithTwoPrimitiveBooleanFields {
    private boolean field1, field2;

    public ClassWithTwoPrimitiveBooleanFields() {
    }
  }

  private static class ClassWithTwoPrimitiveByteFields {
    private byte field1, field2;

    public ClassWithTwoPrimitiveByteFields() {
    }
  }

  private static class ClassWithTwoPrimitiveCharFields {
    private char field1, field2;

    public ClassWithTwoPrimitiveCharFields() {
    }
  }

  private static class ClassWithTwoPrimitiveShortFields {
    private short field1, field2;

    public ClassWithTwoPrimitiveShortFields() {
    }
  }

  private static class ClassWithTwoPrimitiveIntFields {
    private int field1, field2;

    public ClassWithTwoPrimitiveIntFields() {
    }
  }

  private static class ClassWithTwoPrimitiveFloatFields {
    private float field1, field2;

    public ClassWithTwoPrimitiveFloatFields() {
    }
  }

  private static class ClassWithTwoPrimitiveLongFields {
    private long field1, field2;

    public ClassWithTwoPrimitiveLongFields() {
    }
  }

  private static class ClassWithTwoPrimitiveDoubleFields {
    private double field1, field2;

    public ClassWithTwoPrimitiveDoubleFields() {
    }
  }

  private static class SubClassWithTwoPrimitiveBooleanFields extends ClassWithOnePublicPrimitiveBooleanField {
    private boolean privateField;

    public SubClassWithTwoPrimitiveBooleanFields() {
    }
  }

  private static class SubClassWithTwoPrimitiveByteFields extends ClassWithOnePublicPrimitiveByteField {
    private byte privateField;

    public SubClassWithTwoPrimitiveByteFields() {
    }
  }

  private static class SubClassWithTwoPrimitiveCharFields extends ClassWithOnePublicPrimitiveCharField {
    private char privateField;

    public SubClassWithTwoPrimitiveCharFields() {
    }
  }

  private static class SubClassWithTwoPrimitiveShortFields extends ClassWithOnePublicPrimitiveShortField {
    private short privateField;

    public SubClassWithTwoPrimitiveShortFields() {
    }
  }

  private static class SubClassWithTwoPrimitiveIntFields extends ClassWithOnePublicPrimitiveIntField {
    private int privateField;

    public SubClassWithTwoPrimitiveIntFields() {
    }
  }

  private static class SubClassWithTwoPrimitiveFloatFields extends ClassWithOnePublicPrimitiveFloatField {
    private float privateField;

    public SubClassWithTwoPrimitiveFloatFields() {
    }
  }

  private static class SubClassWithTwoPrimitiveLongFields extends ClassWithOnePublicPrimitiveLongField {
    private long privateField;

    public SubClassWithTwoPrimitiveLongFields() {
    }
  }

  private static class SubClassWithTwoPrimitiveDoubleFields extends ClassWithOnePublicPrimitiveDoubleField {
    private double privateField;

    public SubClassWithTwoPrimitiveDoubleFields() {
    }
  }

  private static class SubSubClassWithThreePrimitiveBooleanFields extends SubClassWithTwoPrimitiveBooleanFields {
    private boolean privateField;

    public SubSubClassWithThreePrimitiveBooleanFields() {
    }
  }

  private static class SubSubClassWithThreePrimitiveByteFields extends SubClassWithTwoPrimitiveByteFields {
    private byte privateField;

    public SubSubClassWithThreePrimitiveByteFields() {
    }
  }

  private static class SubSubClassWithThreePrimitiveCharFields extends SubClassWithTwoPrimitiveCharFields {
    private char privateField;

    public SubSubClassWithThreePrimitiveCharFields() {
    }
  }

  private static class SubSubClassWithThreePrimitiveShortFields extends SubClassWithTwoPrimitiveShortFields {
    private short privateField;

    public SubSubClassWithThreePrimitiveShortFields() {
    }
  }

  private static class SubSubClassWithThreePrimitiveIntFields extends SubClassWithTwoPrimitiveIntFields {
    private int privateField;

    public SubSubClassWithThreePrimitiveIntFields() {
    }
  }

  private static class SubSubClassWithThreePrimitiveFloatFields extends SubClassWithTwoPrimitiveFloatFields {
    private float privateField;

    public SubSubClassWithThreePrimitiveFloatFields() {
    }
  }

  private static class SubSubClassWithThreePrimitiveLongFields extends SubClassWithTwoPrimitiveLongFields {
    private long privateField;

    public SubSubClassWithThreePrimitiveLongFields() {
    }
  }

  private static class SubSubClassWithThreePrimitiveDoubleFields extends SubClassWithTwoPrimitiveDoubleFields {
    private double privateField;

    public SubSubClassWithThreePrimitiveDoubleFields() {
    }
  }

  private static class ClassWithThreeObjectPointers {
    Object field1 = new Object();
    Object field2 = new Object();
    Object field3 = new Object();

    public ClassWithThreeObjectPointers() {
    }
  }

  private static class ClassWithArray {
    public Object[] array = new Object[0];

    public ClassWithArray() {
    }
  }

  /** See: https://shipilev.net/jvm/objects-inside-out/#_field_packing */
  private static class FieldPacking {
    boolean b;
    long l;
    char c;
    int i;

    public FieldPacking() {
    }
  }

  /** See: https://shipilev.net/jvm/objects-inside-out/#_observation_field_declaration_order_field_layout_order */
  private static class FieldOrder {
    boolean firstField;
    long secondField;
    char thirdField;
    int fourthField;

    public FieldOrder() {
    }
  }

  private static class ComplexClass {
    ClassWithTwoPrimitiveBooleanFields field1 = new ClassWithTwoPrimitiveBooleanFields();
    ClassWithTwoPrimitiveByteFields field2 = new ClassWithTwoPrimitiveByteFields();
    ClassWithTwoPrimitiveCharFields field3 = new ClassWithTwoPrimitiveCharFields();
    ClassWithTwoPrimitiveShortFields field4 = new ClassWithTwoPrimitiveShortFields();
    ClassWithTwoPrimitiveIntFields field5 = new ClassWithTwoPrimitiveIntFields();
    ClassWithTwoPrimitiveFloatFields field6 = new ClassWithTwoPrimitiveFloatFields();
    ClassWithTwoPrimitiveLongFields field7 = new ClassWithTwoPrimitiveLongFields();
    ClassWithTwoPrimitiveDoubleFields field8 = new ClassWithTwoPrimitiveDoubleFields();
    ClassWithThreeObjectPointers field9 = new ClassWithThreeObjectPointers();
    ClassWithArray field10 = new ClassWithArray();

    public ComplexClass() {
    }
  }
}

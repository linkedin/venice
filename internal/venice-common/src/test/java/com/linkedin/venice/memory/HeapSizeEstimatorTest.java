package com.linkedin.venice.memory;

import static com.linkedin.venice.memory.ClassSizeEstimator.getClassOverhead;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

import com.linkedin.venice.utils.Utils;
import java.lang.reflect.Constructor;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;


public abstract class HeapSizeEstimatorTest {
  private static final Logger LOGGER = LogManager.getLogger(HeapSizeEstimatorTest.class);
  /**
   * Some scenarios are tricky to compute dynamically without just copy-pasting the whole main code, so we just skip it
   * for now, though we could come back to it later...
   */
  private static final int SKIP_EXPECTED_FIELD_OVERHEAD = -1;
  private static final Runtime RUNTIME = Runtime.getRuntime();
  private static final int NUMBER_OF_ALLOCATIONS_WHEN_MEASURING = 200_000;
  protected static final int JAVA_MAJOR_VERSION = Utils.getJavaMajorVersion();
  protected static final int BOOLEAN_SIZE = 1;
  private static final int ALIGNMENT_SIZE;
  protected static final int OBJECT_HEADER_SIZE;
  protected static final int ARRAY_HEADER_SIZE;
  protected static final int POINTER_SIZE;

  static {
    // This duplicates the main code, which is not ideal, but there isn't much choice if we want the test to run in
    // various JVM scenarios...
    boolean is64bitsJVM = ClassSizeEstimator.is64bitsJVM();
    int markWordSize = is64bitsJVM ? 8 : 4;
    boolean isCompressedOopsEnabled = ClassSizeEstimator.isUseCompressedOopsEnabled();
    boolean isCompressedKlassPointersEnabled = ClassSizeEstimator.isCompressedKlassPointersEnabled();
    int classPointerSize = isCompressedKlassPointersEnabled ? 4 : 8;

    ALIGNMENT_SIZE = is64bitsJVM ? 8 : 4;
    OBJECT_HEADER_SIZE = markWordSize + classPointerSize;
    ARRAY_HEADER_SIZE = roundUpToNearestAlignment(OBJECT_HEADER_SIZE + Integer.BYTES);
    POINTER_SIZE = isCompressedOopsEnabled ? 4 : 8;
  }

  private final int[] resultRowCellLengths;

  protected HeapSizeEstimatorTest(Class classWithLongestName) {
    this.resultRowCellLengths = new int[] { 6, classWithLongestName.getSimpleName().length(), 9, 9 };
  }

  protected interface TestFunction {
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

  protected enum TestMethodology {
    THEORETICAL_EXPECTATION(
        o -> o::theoreticalExpectation, new String[] { "status", "class name", "predicted", "expected" }
    ),
    EMPIRICAL_MEASUREMENT(
        o -> o::empiricalClassMeasurement, new String[] { "status", "class name", "predicted", "allocated" }
    );

    final Function<HeapSizeEstimatorTest, TestFunction> tfProvider;
    final String[] resultsTableHeader;

    TestMethodology(Function<HeapSizeEstimatorTest, TestFunction> tfProvider, String[] resultsTableHeader) {
      this.tfProvider = tfProvider;
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

  protected void theoreticalExpectation(Class c, int expectedFieldOverhead, Supplier<Object> constructor) {
    int predictedClassOverhead = getClassOverhead(c);
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

  private void empiricalClassMeasurement(Class c, int expectedFieldOverhead, Supplier<Object> constructor) {
    empiricalMeasurement(c, getClassOverhead(c), constructor);
  }

  protected void empiricalMeasurement(Class c, int predictedUsage, Supplier<?> constructor) {
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

  /** Different algo than the main code because why not? It should be equivalent... */
  protected static int roundUpToNearestAlignment(int size) {
    double numberOfAlignmentWindowsFittingWithinTheSize = (double) size / ALIGNMENT_SIZE;
    double roundedUp = Math.ceil(numberOfAlignmentWindowsFittingWithinTheSize);
    int finalSize = (int) roundedUp * ALIGNMENT_SIZE;
    return finalSize;
  }

  private static long getCurrentlyAllocatedMemory() {
    System.gc();
    return RUNTIME.maxMemory() - RUNTIME.freeMemory();
  }

  @BeforeClass
  public void printEnvironmentInfo() {
    LOGGER.info("Java major version: " + JAVA_MAJOR_VERSION);
    LOGGER.info("Alignment size: " + ALIGNMENT_SIZE);
    LOGGER.info("Object header size: " + OBJECT_HEADER_SIZE);
    LOGGER.info("Array header size: " + ARRAY_HEADER_SIZE);
    LOGGER.info("Pointer size: " + POINTER_SIZE);
  }

  protected void printHeader(String... headerCells) {
    printResultSeparatorLine();
    printResultRow(headerCells);
    printResultSeparatorLine();
  }

  protected void printResultSeparatorLine() {
    StringBuilder sb = new StringBuilder();
    sb.append(" ");
    for (int i = 0; i < this.resultRowCellLengths.length; i++) {
      sb.append("+-");
      for (int j = 0; j < this.resultRowCellLengths[i] + 1; j++) {
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

  private void printResultRow(String... cells) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < cells.length; i++) {
      sb.append(" | ");
      String cell = cells[i];
      sb.append(cell);

      int remainder = this.resultRowCellLengths[i] - cell.length();
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
}

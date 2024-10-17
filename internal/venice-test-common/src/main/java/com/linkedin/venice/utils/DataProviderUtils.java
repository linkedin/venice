package com.linkedin.venice.utils;

import static com.linkedin.venice.compression.CompressionStrategy.GZIP;
import static com.linkedin.venice.compression.CompressionStrategy.NO_OP;
import static com.linkedin.venice.compression.CompressionStrategy.ZSTD_WITH_DICT;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.store.cache.backend.ObjectCacheConfig;
import com.linkedin.venice.kafka.validation.checksum.CheckSumType;
import com.linkedin.venice.meta.IngestionMode;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import org.apache.avro.Schema;
import org.testng.annotations.DataProvider;
import org.testng.collections.Lists;


/**
 * This class gathers all common data provider patterns in test cases. In order to leverage this util class,
 * make sure your test case has "test" dependency on "venice-test-common" module.
 */
public class DataProviderUtils {
  public static final Object[] BOOLEAN = { false, true };
  public static final Object[] BOOLEAN_FALSE = { false };
  public static final Object[] COMPRESSION_STRATEGIES = { NO_OP, GZIP, ZSTD_WITH_DICT };
  public static final Object[] PARTITION_COUNTS = { 1, 2, 3, 4, 8, 10, 16, 19, 92, 128 };

  public static final Object[] CHECKSUM_TYPES = { CheckSumType.MD5, CheckSumType.ADHASH };

  /**
   * To use these data providers, add (dataProvider = "<provider_name>", dataProviderClass = DataProviderUtils.class)
   * into the @Test annotation.
   */
  @DataProvider(name = "True-and-False")
  public static Object[][] trueAndFalseProvider() {
    return new Object[][] { { false }, { true } };
  }

  @DataProvider(name = "Compression-Strategies")
  public static Object[][] compressionProvider() {
    return allPermutationGenerator(COMPRESSION_STRATEGIES);
  }

  @DataProvider(name = "Two-True-and-False")
  public static Object[][] twoBoolean() {
    return allPermutationGenerator(BOOLEAN, BOOLEAN);
  }

  @DataProvider(name = "Three-True-and-False")
  public static Object[][] threeBoolean() {
    return allPermutationGenerator(BOOLEAN, BOOLEAN, BOOLEAN);
  }

  @DataProvider(name = "Four-True-and-False")
  public static Object[][] fourBoolean() {
    return allPermutationGenerator(BOOLEAN, BOOLEAN, BOOLEAN, BOOLEAN);
  }

  @DataProvider(name = "Five-True-and-False")
  public static Object[][] fiveBoolean() {
    return allPermutationGenerator(BOOLEAN, BOOLEAN, BOOLEAN, BOOLEAN, BOOLEAN);
  }

  @DataProvider(name = "CheckpointingSupported-CheckSum-Types")
  public static Object[][] checkpointingSupportedCheckSumTypes() {
    return new Object[][] { { CheckSumType.MD5 }, { CheckSumType.ADHASH } };
  }

  @DataProvider(name = "dv-client-config-provider")
  public static Object[][] daVinciConfigProvider() {
    DaVinciConfig defaultDaVinciConfig = new DaVinciConfig();
    defaultDaVinciConfig.setReadMetricsEnabled(true);

    DaVinciConfig cachingDaVinciConfig = new DaVinciConfig();
    cachingDaVinciConfig.setCacheConfig(new ObjectCacheConfig());

    return new Object[][] { { defaultDaVinciConfig }, { cachingDaVinciConfig } };
  }

  @DataProvider(name = "changelogConsumer")
  public static Object[][] changelogConsumer() {
    return new Object[][] { { 1 }, { 3 } };
  }

  @DataProvider(name = "Isolated-Ingestion")
  public static Object[][] isolatedIngestion() {
    return new Object[][] { { IngestionMode.BUILT_IN }, { IngestionMode.ISOLATED } };
  }

  @DataProvider(name = "Chunking-And-Partition-Counts")
  public static Object[][] chunkingAndPartitionCountsCombination() {
    return allPermutationGenerator(BOOLEAN, PARTITION_COUNTS);
  }

  @DataProvider(name = "Boolean-Compression")
  public static Object[][] booleanCompression() {
    return allPermutationGenerator(BOOLEAN, COMPRESSION_STRATEGIES);
  }

  @DataProvider(name = "Boolean-Boolean-Compression")
  public static Object[][] booleanBooleanCompression() {
    return allPermutationGenerator(BOOLEAN, BOOLEAN, COMPRESSION_STRATEGIES);
  }

  @DataProvider(name = "Boolean-Checksum")
  public static Object[][] booleanChecksumType() {
    return allPermutationGenerator(BOOLEAN, CHECKSUM_TYPES);
  }

  @DataProvider(name = "All-Avro-Schemas-Except-Null-And-Union")
  public static Object[][] allAvroTypesExceptNullAndUnion() {
    List<Object[]> resultingArray = Lists.newArrayList();

    List<Schema> simpleTypes = Arrays.asList(
        Schema.create(Schema.Type.BOOLEAN),
        Schema.create(Schema.Type.BYTES),
        Schema.create(Schema.Type.DOUBLE),
        Schema.createEnum(
            "MyColorEnum",
            "Who needs anything else than RGB?",
            "my.namespace",
            Arrays.asList("Red", "Green", "Blue")),
        Schema.createFixed("MyFixed", "", "", 16),
        Schema.create(Schema.Type.FLOAT),
        Schema.create(Schema.Type.INT),
        Schema.create(Schema.Type.LONG),
        Schema.create(Schema.Type.STRING));

    for (Schema schema: simpleTypes) {
      resultingArray.add(new Object[] { schema });
      resultingArray.add(new Object[] { Schema.createArray(schema) });
      resultingArray.add(new Object[] { Schema.createMap(schema) });
      List<Schema.Field> recordFields =
          Arrays.asList(AvroCompatibilityHelper.createSchemaField("MyField", schema, "", null));
      resultingArray.add(new Object[] { Schema.createRecord("MyRecord", "", "", false, recordFields) });
    }

    return resultingArray.toArray(new Object[resultingArray.size()][]);
  }

  /**
   * Generate permutations to be fed to a DataProvider.
   * For two boolean's we'd pass in allPermutationGenerator(BOOLEAN, BOOLEAN)
   * @param parameterSets Sets of valid values for each parameter
   * @return the permutations that can be returned from a {@link DataProvider}
   */
  public static Object[][] allPermutationGenerator(Object[]... parameterSets) {
    return allPermutationGenerator((permutation) -> true, parameterSets);
  }

  /**
   * Generate permutations to be fed to a DataProvider.
   * For two boolean's we'd pass in allPermutationGenerator(BOOLEAN, BOOLEAN)
   * @param parameterSets Sets of valid values for each parameter
   * @param permutationValidator A function that takes the permutation as an input and decides if it is valid
   * @return the permutations that can be returned from a {@link DataProvider}
   */
  public static Object[][] allPermutationGenerator(
      Function<Object[], Boolean> permutationValidator,
      Object[]... parameterSets) {
    PermutationIterator permutationIterator = new PermutationIterator(parameterSets);
    int totalPermutations = permutationIterator.size();
    Object[][] permutations = new Object[totalPermutations][];
    int i = 0;
    while (permutationIterator.hasNext()) {
      Object[] permutation = permutationIterator.next();
      if (permutationValidator.apply(permutation)) {
        permutations[i] = permutation;
      }
      i++;
    }
    return permutations;
  }

  private static class PermutationIterator implements Iterator<Object[]> {
    private int totalPermutations;
    private Object[][] parameterSets;
    private int[] markers;
    private boolean valueRead = false;
    private boolean hasNext;

    public PermutationIterator(Object[]... parameterSets) {
      this.parameterSets = parameterSets;
      this.markers = new int[parameterSets.length];
      totalPermutations = 1;
      hasNext = true;
      for (int i = 0; i < parameterSets.length; i++) {
        markers[i] = 0;
        if (parameterSets[i] == null || parameterSets[i].length == 0) {
          throw new IllegalArgumentException("Argument type cannot be null or empty");
        }
        totalPermutations *= parameterSets[i].length;
      }
    }

    @Override
    public boolean hasNext() {
      if (!valueRead) {
        return hasNext;
      }

      int i = 0;
      for (; i < markers.length; i++) {
        if (markers[i] < parameterSets[i].length - 1) {
          markers[i]++;
          valueRead = false;
          break;
        } else {
          markers[i] = 0;
        }
      }

      hasNext = i != markers.length;
      return hasNext;
    }

    @Override
    public Object[] next() {
      valueRead = true;
      Object[] permutation = new Object[parameterSets.length];
      for (int i = 0; i < parameterSets.length; i++) {
        permutation[i] = parameterSets[i][markers[i]];
      }
      return permutation;
    }

    public int size() {
      return totalPermutations;
    }
  }
}

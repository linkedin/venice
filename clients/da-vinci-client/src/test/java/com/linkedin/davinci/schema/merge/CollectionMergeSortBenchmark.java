package com.linkedin.davinci.schema.merge;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;


public class CollectionMergeSortBenchmark {
  // Schema with an array of records (expensive hashCode)
  private static final String RECORD_SCHEMA_STR = "{" + "  \"type\": \"record\","
      + "  \"namespace\": \"com.linkedin.avro\"," + "  \"name\": \"BenchRecord\"," + "  \"fields\": ["
      + "    { \"name\": \"Items\", \"type\": { \"type\": \"array\", \"items\": {"
      + "        \"type\": \"record\", \"name\": \"NestedRecord\", \"fields\": ["
      + "          { \"name\": \"id\", \"type\": \"int\" }," + "          { \"name\": \"name\", \"type\": \"string\" },"
      + "          { \"name\": \"value\", \"type\": \"double\" },"
      + "          { \"name\": \"tag\", \"type\": \"string\" },"
      + "          { \"name\": \"extra\", \"type\": \"string\" }" + "        ]" + "    }}, \"default\": [] }" + "  ]"
      + "}";

  // Schema with an array of ints (cheap hashCode)
  private static final String INT_SCHEMA_STR = "{" + "  \"type\": \"record\","
      + "  \"namespace\": \"com.linkedin.avro\"," + "  \"name\": \"IntBenchRecord\"," + "  \"fields\": ["
      + "    { \"name\": \"Items\", \"type\": { \"type\": \"array\", \"items\": \"int\" }, \"default\": [] }" + "  ]"
      + "}";

  private static final String LIST_FIELD_NAME = "Items";

  @Test(enabled = false) // Manual benchmark — run explicitly, not in CI
  public void runBenchmark() {
    main(new String[0]);
  }

  public static void main(String[] args) {
    System.out.println("=== Collection Merge Sort Benchmark ===\n");

    // Test with nested records (expensive hashCode) - the PR's target use case
    System.out.println("--- Nested Record elements (expensive hashCode) ---");
    for (int n: new int[] { 1000, 5000, 10000, 20000, 50000, 100000, 200000 }) {
      for (int m: new int[] { 5, 50 }) {
        benchmarkRecordElements(n, m);
      }
    }

    // Test with int elements (cheap hashCode)
    System.out.println("\n--- Integer elements (cheap hashCode) ---");
    for (int n: new int[] { 1000, 5000, 10000, 20000, 50000, 100000, 200000 }) {
      for (int m: new int[] { 5, 50 }) {
        benchmarkIntElements(n, m);
      }
    }
  }

  private static void benchmarkRecordElements(int n, int m) {
    Schema valueSchema = AvroCompatibilityHelper.parse(RECORD_SCHEMA_STR);
    Schema rmdSchema = RmdSchemaGenerator.generateMetadataSchema(valueSchema);
    Schema rmdTimestampSchema = rmdSchema.getField("timestamp").schema().getTypes().get(1);
    Schema nestedRecordSchema = valueSchema.getField(LIST_FIELD_NAME).schema().getElementType();

    SortBasedCollectionFieldOpHandler handler =
        new SortBasedCollectionFieldOpHandler(AvroCollectionElementComparator.INSTANCE);

    // Build initial state: a collection-merge list with n elements.
    // First, do a PUT to set up n elements, then do a MODIFY to transition to collection-merge state.
    GenericRecord valueRecord = new GenericData.Record(valueSchema);
    List<Object> initialElements = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      GenericRecord rec = new GenericData.Record(nestedRecordSchema);
      rec.put("id", i);
      rec.put("name", "element_" + i);
      rec.put("value", (double) i * 1.5);
      rec.put("tag", "tag_" + (i % 100));
      rec.put("extra", "extra_data_" + i + "_padding_to_make_it_longer");
      initialElements.add(rec);
    }
    valueRecord.put(LIST_FIELD_NAME, initialElements);

    // Create RMD in collection-merge state
    CollectionRmdTimestamp<Object> collectionRmd =
        buildCollectionMergeRmd(rmdTimestampSchema, n, 10L, nestedRecordSchema);

    // Do a PUT first, then a MODIFY to get into collection-merge state
    handler.handlePutList(10L, 1, initialElements, collectionRmd, valueRecord, valueSchema.getField(LIST_FIELD_NAME));

    // First MODIFY to transition to collection-merge state
    List<Object> firstModifyAdd = new ArrayList<>();
    GenericRecord transitionRec = new GenericData.Record(nestedRecordSchema);
    transitionRec.put("id", n + 1);
    transitionRec.put("name", "transition");
    transitionRec.put("value", 0.0);
    transitionRec.put("tag", "transition");
    transitionRec.put("extra", "transition_element");
    firstModifyAdd.add(transitionRec);
    handler.handleModifyList(
        20L,
        collectionRmd,
        valueRecord,
        valueSchema.getField(LIST_FIELD_NAME),
        firstModifyAdd,
        Collections.emptyList());

    // Now we're in collection-merge state. Benchmark subsequent MODIFY operations.
    int warmupIterations = n >= 50000 ? 20 : 50;
    int measuredIterations = n >= 50000 ? 50 : 200;

    // Warm up
    for (int iter = 0; iter < warmupIterations; iter++) {
      GenericRecord valueCopy = GenericData.get().deepCopy(valueSchema, valueRecord);
      CollectionRmdTimestamp<Object> rmdCopy = new CollectionRmdTimestamp<>(collectionRmd);
      List<Object> toAdd = new ArrayList<>(m);
      for (int j = 0; j < m; j++) {
        GenericRecord rec = new GenericData.Record(nestedRecordSchema);
        int id = n + 1000 + iter * m + j;
        rec.put("id", id);
        rec.put("name", "new_element_" + id);
        rec.put("value", (double) id * 2.5);
        rec.put("tag", "new_tag_" + (id % 50));
        rec.put("extra", "new_extra_data_" + id + "_padding");
        toAdd.add(rec);
      }
      handler.handleModifyList(
          30L + iter,
          rmdCopy,
          valueCopy,
          valueSchema.getField(LIST_FIELD_NAME),
          toAdd,
          Collections.emptyList());
    }

    // Measure
    long[] times = new long[measuredIterations];
    for (int iter = 0; iter < measuredIterations; iter++) {
      GenericRecord valueCopy = GenericData.get().deepCopy(valueSchema, valueRecord);
      CollectionRmdTimestamp<Object> rmdCopy = new CollectionRmdTimestamp<>(collectionRmd);
      List<Object> toAdd = new ArrayList<>(m);
      for (int j = 0; j < m; j++) {
        GenericRecord rec = new GenericData.Record(nestedRecordSchema);
        int id = n + 100000 + iter * m + j;
        rec.put("id", id);
        rec.put("name", "measure_element_" + id);
        rec.put("value", (double) id * 3.5);
        rec.put("tag", "measure_tag_" + (id % 50));
        rec.put("extra", "measure_extra_data_" + id + "_padding");
        toAdd.add(rec);
      }
      long start = System.nanoTime();
      handler.handleModifyList(
          30L + warmupIterations + iter,
          rmdCopy,
          valueCopy,
          valueSchema.getField(LIST_FIELD_NAME),
          toAdd,
          Collections.emptyList());
      times[iter] = System.nanoTime() - start;
    }

    Arrays.sort(times);
    double medianUs = times[measuredIterations / 2] / 1000.0;
    double p95Us = times[(int) (measuredIterations * 0.95)] / 1000.0;
    double avgUs = Arrays.stream(times).average().orElse(0) / 1000.0;
    System.out.printf("  n=%5d, m=%2d: median=%.0f us, avg=%.0f us, p95=%.0f us%n", n, m, medianUs, avgUs, p95Us);
  }

  private static void benchmarkIntElements(int n, int m) {
    Schema valueSchema = AvroCompatibilityHelper.parse(INT_SCHEMA_STR);
    Schema rmdSchema = RmdSchemaGenerator.generateMetadataSchema(valueSchema);
    Schema rmdTimestampSchema = rmdSchema.getField("timestamp").schema().getTypes().get(1);

    SortBasedCollectionFieldOpHandler handler =
        new SortBasedCollectionFieldOpHandler(AvroCollectionElementComparator.INSTANCE);

    GenericRecord valueRecord = new GenericData.Record(valueSchema);
    List<Object> initialElements = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      initialElements.add(i);
    }
    valueRecord.put(LIST_FIELD_NAME, initialElements);

    CollectionRmdTimestamp<Object> collectionRmd =
        buildCollectionMergeRmd(rmdTimestampSchema, n, 10L, Schema.create(Schema.Type.INT));

    handler.handlePutList(10L, 1, initialElements, collectionRmd, valueRecord, valueSchema.getField(LIST_FIELD_NAME));

    // First MODIFY to transition to collection-merge state
    handler.handleModifyList(
        20L,
        collectionRmd,
        valueRecord,
        valueSchema.getField(LIST_FIELD_NAME),
        Arrays.asList((Object) (n + 1)),
        Collections.emptyList());

    int warmupIterations = 100;
    int measuredIterations = 500;

    // Warm up
    for (int iter = 0; iter < warmupIterations; iter++) {
      GenericRecord valueCopy = GenericData.get().deepCopy(valueSchema, valueRecord);
      CollectionRmdTimestamp<Object> rmdCopy = new CollectionRmdTimestamp<>(collectionRmd);
      List<Object> toAdd = new ArrayList<>(m);
      for (int j = 0; j < m; j++) {
        toAdd.add(n + 1000 + iter * m + j);
      }
      handler.handleModifyList(
          30L + iter,
          rmdCopy,
          valueCopy,
          valueSchema.getField(LIST_FIELD_NAME),
          toAdd,
          Collections.emptyList());
    }

    // Measure
    long[] times = new long[measuredIterations];
    for (int iter = 0; iter < measuredIterations; iter++) {
      GenericRecord valueCopy = GenericData.get().deepCopy(valueSchema, valueRecord);
      CollectionRmdTimestamp<Object> rmdCopy = new CollectionRmdTimestamp<>(collectionRmd);
      List<Object> toAdd = new ArrayList<>(m);
      for (int j = 0; j < m; j++) {
        toAdd.add(n + 100000 + iter * m + j);
      }
      long start = System.nanoTime();
      handler.handleModifyList(
          30L + warmupIterations + iter,
          rmdCopy,
          valueCopy,
          valueSchema.getField(LIST_FIELD_NAME),
          toAdd,
          Collections.emptyList());
      times[iter] = System.nanoTime() - start;
    }

    Arrays.sort(times);
    double medianUs = times[measuredIterations / 2] / 1000.0;
    double p95Us = times[(int) (measuredIterations * 0.95)] / 1000.0;
    double avgUs = Arrays.stream(times).average().orElse(0) / 1000.0;
    System.out.printf("  n=%5d, m=%2d: median=%.0f us, avg=%.0f us, p95=%.0f us%n", n, m, medianUs, avgUs, p95Us);
  }

  private static CollectionRmdTimestamp<Object> buildCollectionMergeRmd(
      Schema rmdTimestampSchema,
      int numElements,
      long topLevelTimestamp,
      Schema elementSchema) {
    CollectionTimestampBuilder builder = new CollectionTimestampBuilder(elementSchema);
    builder.setCollectionTimestampSchema(rmdTimestampSchema.getField(LIST_FIELD_NAME).schema());
    builder.setTopLevelColoID(1);
    builder.setPutOnlyPartLength(0);
    builder.setTopLevelTimestamps(topLevelTimestamp);
    builder.setActiveElementsTimestamps(new LinkedList<>());
    builder.setDeletedElementTimestamps(new LinkedList<>());
    builder.setDeletedElements(elementSchema, new LinkedList<>());
    return new CollectionRmdTimestamp<>(builder.build());
  }
}

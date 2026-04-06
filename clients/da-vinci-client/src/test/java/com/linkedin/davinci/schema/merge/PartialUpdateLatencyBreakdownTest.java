package com.linkedin.davinci.schema.merge;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.serializer.avro.fast.MapOrderPreservingFastSerDeFactory;
import com.linkedin.davinci.utils.IndexedHashMap;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;


/**
 * Instrumented test to identify where partial update latency comes from.
 * Uses integer list elements (production use case) with Fast Avro.
 */
public class PartialUpdateLatencyBreakdownTest {
  private static final String LIST_FIELD_NAME = "Items";

  // Integer list elements (production use case)
  private static final String INT_VALUE_SCHEMA_STR =
      "{" + "  \"type\": \"record\"," + "  \"namespace\": \"com.linkedin.avro\"," + "  \"name\": \"IntTestRecord\","
          + "  \"fields\": [" + "    { \"name\": \"id\", \"type\": \"int\" },"
          + "    { \"name\": \"Items\", \"type\": { \"type\": \"array\", \"items\": \"int\" }, \"default\": [] }"
          + "  ]" + "}";

  @Test(enabled = false) // Manual benchmark — run explicitly, not in CI
  public void runBreakdown() {
    System.out.println("=== Partial Update Latency Breakdown (Integer elements, Fast Avro) ===\n");

    Schema valueSchema = AvroCompatibilityHelper.parse(INT_VALUE_SCHEMA_STR);
    Schema rmdSchema = RmdSchemaGenerator.generateMetadataSchema(valueSchema);
    Schema rmdTimestampSchema = rmdSchema.getField("timestamp").schema().getTypes().get(1);

    for (int n: new int[] { 10000, 50000, 100000 }) {
      for (int m: new int[] { 5 }) {
        runForSize(n, m, valueSchema, rmdTimestampSchema);
      }
    }
  }

  private void runForSize(int n, int m, Schema valueSchema, Schema rmdTimestampSchema) {
    System.out.printf("--- n=%d, m=%d ---%n", n, m);

    Schema.Field listField = valueSchema.getField(LIST_FIELD_NAME);
    Schema elementSchema = listField.schema().getElementType();

    // 1. Build the record with n integer elements.
    GenericRecord valueRecord = new GenericData.Record(valueSchema);
    valueRecord.put("id", 1);
    List<Object> elements = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      elements.add(i);
    }
    valueRecord.put(LIST_FIELD_NAME, elements);

    // 2. Serialize to bytes.
    RecordSerializer<GenericRecord> serializer = MapOrderPreservingFastSerDeFactory.getSerializer(valueSchema);
    byte[] serializedBytes = serializer.serialize(valueRecord);
    System.out
        .printf("  Serialized size: %d bytes (%.2f KB)%n", serializedBytes.length, serializedBytes.length / 1024.0);

    RecordDeserializer<GenericRecord> deserializer =
        MapOrderPreservingFastSerDeFactory.getDeserializer(valueSchema, valueSchema);

    // 3. Set up handler and transition to collection-merge state.
    SortBasedCollectionFieldOpHandler handler =
        new SortBasedCollectionFieldOpHandler(AvroCollectionElementComparator.INSTANCE);

    GenericRecord baseRecord = GenericData.get().deepCopy(valueSchema, valueRecord);
    CollectionRmdTimestamp<Object> baseRmd = buildCollectionMergeRmd(rmdTimestampSchema, n, 10L, elementSchema);
    handler.handlePutList(10L, 1, (List<Object>) baseRecord.get(listField.pos()), baseRmd, baseRecord, listField);

    // First MODIFY to transition to collection-merge state
    List<Object> transitionAdd = new ArrayList<>();
    transitionAdd.add(n + 1);
    handler.handleModifyList(20L, baseRmd, baseRecord, listField, transitionAdd, Collections.emptyList());

    // Serialize the collection-merge state.
    byte[] cmStateBytes = serializer.serialize(baseRecord);
    System.out
        .printf("  CM state serialized size: %d bytes (%.2f KB)%n", cmStateBytes.length, cmStateBytes.length / 1024.0);

    // Build the partial update payload (m new integers to add).
    List<Object> toAddElements = new ArrayList<>(m);
    for (int j = 0; j < m; j++) {
      toAddElements.add(n + 1000 + j);
    }

    // ========== WARMUP ==========
    int warmup = n >= 50000 ? 20 : 50;
    for (int iter = 0; iter < warmup; iter++) {
      GenericRecord deserialized = deserializer.deserialize(ByteBuffer.wrap(cmStateBytes));
      CollectionRmdTimestamp<Object> rmdCopy = new CollectionRmdTimestamp<>(baseRmd);
      handler.handleModifyList(30L + iter, rmdCopy, deserialized, listField, toAddElements, Collections.emptyList());
      serializer.serialize(deserialized);
    }

    // ========== MEASURE ==========
    int iterations = n >= 50000 ? 50 : 200;
    long[] deserTimes = new long[iterations];
    long[] rmdCopyTimes = new long[iterations];
    long[] indexedMapBuildTimes = new long[iterations];
    long[] mergeAlgoTimes = new long[iterations];
    long[] serTimes = new long[iterations];
    long[] totalTimes = new long[iterations];

    for (int iter = 0; iter < iterations; iter++) {
      long t0 = System.nanoTime();

      // Phase A: Deserialize value from bytes.
      GenericRecord deserialized = deserializer.deserialize(ByteBuffer.wrap(cmStateBytes));
      long t1 = System.nanoTime();

      // Phase B: Copy RMD.
      CollectionRmdTimestamp<Object> rmdCopy = new CollectionRmdTimestamp<>(baseRmd);
      long t2 = System.nanoTime();

      // Phase C: Build IndexedHashMap (isolated measurement).
      List<Object> currElements = (List<Object>) deserialized.get(listField.pos());
      IndexedHashMap<Object, Long> tempMap = Utils.createElementToActiveTsMap(
          currElements,
          rmdCopy.getActiveElementTimestamps(),
          rmdCopy.getTopLevelFieldTimestamp(),
          Long.MIN_VALUE,
          rmdCopy.getPutOnlyPartLength());
      long t3 = System.nanoTime();

      // Phase D: Full handleModifyList (includes its own IndexedHashMap build).
      GenericRecord deserialized2 = deserializer.deserialize(ByteBuffer.wrap(cmStateBytes));
      CollectionRmdTimestamp<Object> rmdCopy2 = new CollectionRmdTimestamp<>(baseRmd);
      long t3b = System.nanoTime();
      handler.handleModifyList(
          30L + warmup + iter,
          rmdCopy2,
          deserialized2,
          listField,
          toAddElements,
          Collections.emptyList());
      long t4 = System.nanoTime();

      // Phase E: Serialize merged result.
      serializer.serialize(deserialized2);
      long t5 = System.nanoTime();

      deserTimes[iter] = t1 - t0;
      rmdCopyTimes[iter] = t2 - t1;
      indexedMapBuildTimes[iter] = t3 - t2;
      mergeAlgoTimes[iter] = t4 - t3b;
      serTimes[iter] = t5 - t4;
      totalTimes[iter] = (t1 - t0) + (t2 - t1) + (t4 - t3b) + (t5 - t4);
    }

    System.out.printf(
        "  %-35s median=%8.0f us  p95=%8.0f us%n",
        "A) Deserialize value from bytes:",
        median(deserTimes),
        p95(deserTimes));
    System.out
        .printf("  %-35s median=%8.0f us  p95=%8.0f us%n", "B) Copy RMD:", median(rmdCopyTimes), p95(rmdCopyTimes));
    System.out.printf(
        "  %-35s median=%8.0f us  p95=%8.0f us%n",
        "C) Build IndexedHashMap (isolated):",
        median(indexedMapBuildTimes),
        p95(indexedMapBuildTimes));
    System.out.printf(
        "  %-35s median=%8.0f us  p95=%8.0f us%n",
        "D) handleModifyList (full merge):",
        median(mergeAlgoTimes),
        p95(mergeAlgoTimes));
    System.out.printf(
        "  %-35s median=%8.0f us  p95=%8.0f us%n",
        "E) Serialize merged result:",
        median(serTimes),
        p95(serTimes));
    System.out
        .printf("  %-35s median=%8.0f us  p95=%8.0f us%n", "TOTAL (A+B+D+E):", median(totalTimes), p95(totalTimes));

    double dMed = median(deserTimes);
    double rMed = median(rmdCopyTimes);
    double mMed = median(mergeAlgoTimes);
    double sMed = median(serTimes);
    double sumMed = dMed + rMed + mMed + sMed;
    System.out.printf(
        "%n  Breakdown: deser=%.1f%%  rmdCopy=%.1f%%  merge=%.1f%%  ser=%.1f%%%n",
        dMed / sumMed * 100,
        rMed / sumMed * 100,
        mMed / sumMed * 100,
        sMed / sumMed * 100);
    System.out.printf(
        "  Of merge: IndexedHashMap build alone = %.0f us (%.1f%% of merge)%n%n",
        median(indexedMapBuildTimes),
        median(indexedMapBuildTimes) / mMed * 100);
  }

  private static double median(long[] arr) {
    long[] sorted = arr.clone();
    Arrays.sort(sorted);
    return sorted[sorted.length / 2] / 1000.0;
  }

  private static double p95(long[] arr) {
    long[] sorted = arr.clone();
    Arrays.sort(sorted);
    return sorted[(int) (sorted.length * 0.95)] / 1000.0;
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

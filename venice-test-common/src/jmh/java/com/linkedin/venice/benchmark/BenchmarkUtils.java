package com.linkedin.venice.benchmark;

import com.google.common.collect.ImmutableSet;
import com.google.flatbuffers.FlatBufferBuilder;
import com.linkedin.avro.fastserde.PrimitiveFloatList;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import static com.linkedin.venice.serializer.FastSerializerDeserializerFactory.*;
import static java.lang.Float.*;


public class BenchmarkUtils {
  private static final Set<String> OLDGEN_GC_NAMES =
      ImmutableSet.of("MarkSweepCompact", "PS MarkSweep", "ConcurrentMarkSweep", "G1 Mixed Generation");
  private static final Set<String> NEWGEN_GC_NAMES =
      ImmutableSet.of("Copy", "PS Scavenge", "ParNew", "ParNew", "G1 Young Generation");
  private BenchmarkUtils() {
  }

  public static Map<String, Float> runWithMetrics(Runnable code,String test, int numQueries, int numThreads) {

    Map<String, Float> beforeMetrics = getJvmMetrics();

    if (numThreads == 1) {
      code.run();
    } else {
      ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
      for (int i = 0; i < numThreads; i++) {
        executorService.execute(code);
      }
      executorService.shutdown();
      try {
        if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
          executorService.shutdownNow();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    Map<String, Float> afterMetrics = getJvmMetrics();

    Map<String, Float> metricsDelta = new TreeMap<>();
    afterMetrics.forEach((k, v) -> metricsDelta.put(k, v - beforeMetrics.get(k)));

    printStatistics(test, metricsDelta, numQueries);
    return metricsDelta;
  }

  public static Map<String, Float> runWithMetrics(Runnable code) {
    return runWithMetrics(code);
  }

  public static Map<String, Float> getJvmMetrics() {
    Map<String, Float> metrics = new HashMap<>();
    List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
    for (GarbageCollectorMXBean gcBean : gcBeans) {
      String name = gcBean.getName();
      if (OLDGEN_GC_NAMES.contains(name)) {
        metrics.put("gc_old_count", (float) gcBean.getCollectionCount());
        metrics.put("gc_old_time", (float) gcBean.getCollectionTime());
      } else if (NEWGEN_GC_NAMES.contains(name)) {
        metrics.put("gc_new_count", (float) gcBean.getCollectionCount());
        metrics.put("gc_new_time", (float) gcBean.getCollectionTime());
      } else {
        metrics.put("gc_" + name + "_count", (float) gcBean.getCollectionCount());
        metrics.put("gc_" + name + "_time", (float) gcBean.getCollectionTime());
      }
    }
    metrics.put("uptime", (float) ManagementFactory.getRuntimeMXBean().getUptime());
    return metrics;
  }

  public static void printStatistics(String testCaseName, Map<String, Float> metricsDelta, long queryVolume) {
    System.out.println();
    System.out.format(testCaseName + " ===================\n");
    for (Map.Entry<String, Float> entry : metricsDelta.entrySet()) {
      String key = entry.getKey();
      if (key.equals("gc_new_time") || key.equals("gc_old_time")) {
        continue;
      }
      System.out.format(testCaseName + ".%s=%f\n", key, entry.getValue());
    }
    DecimalFormat decimalFormat = new DecimalFormat("###,###.###");
    System.out.format(testCaseName + ".query_Volume: %s, time lapse: %f ms\n", decimalFormat.format(queryVolume),
        metricsDelta.get("uptime"));
    System.out.format(testCaseName + ".average_QPS: %s\n",
        decimalFormat.format(1000d * (double) queryVolume / metricsDelta.get("uptime")));
    System.out.format(testCaseName + ".average_latency: %s us\n", decimalFormat.format(
        (double) TimeUnit.MICROSECONDS.convert((long) metricsDelta.get("uptime").floatValue(), TimeUnit.MILLISECONDS)
            / (double) queryVolume));
  }

  public static void avroBenchmark(boolean fastAvro, int array_size, long iteration) {
    float w = 0f;
    String schemaString = "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"inventory\", \"type\" : {  \"type\" : \"array\", \"items\" : \"float\" }}] }";

    Schema schema = Schema.parse(schemaString);
    RecordSerializer<Object> serializer =
        fastAvro ? getFastAvroGenericSerializer(schema) : getAvroGenericSerializer(schema);
    RecordDeserializer<Object> deserializer =
        fastAvro ? FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(schema, schema) : SerializerDeserializerFactory
            .getAvroGenericDeserializer(schema, schema);
    List<Float> floats = new ArrayList(array_size);
    for (int l = 0; l < array_size; l++) {
      floats.add((float) l);
    }

    GenericData.Record  record = new GenericData.Record(schema);
    GenericData.Record mons;
    for (int i = 0; i < iteration; i++) {
      record.put("inventory", floats);
      byte[] bytes = serializer.serialize(record);
      mons = (GenericData.Record) deserializer.deserialize(record, bytes);

      if (fastAvro) {
        if (mons.get(0) instanceof PrimitiveFloatList) {
          PrimitiveFloatList list = (PrimitiveFloatList) mons.get(0);
          for (int n = 0; n < list.size(); n++) {
            w += list.getPrimitive(n);
          }
        }
      } else {
        List<Float> list = (List<Float>) (mons).get(0);
        for (int n = 0; n < list.size(); n++) {
          w += list.get(n);
        }
      }
    }
  }

  public static void flatBuffersBenchmark(int array_size, int iteration) {
    FlatBufferBuilder builder = new FlatBufferBuilder(0);
    float w = 0f;
    Monster monster = new Monster();

    for (int k = 0; k < iteration; k++) {
      builder.clear();

      // Serialize the FlatBuffer data.
      builder.startVector(4, array_size, 4);
      for (int i = array_size - 1; i >= 0; i--) {
        builder.addFloat(i);
      }
      int inv = builder.endVector();

      Monster.startMonster(builder);
      Monster.addInventory(builder, inv);
      int orc = Monster.endMonster(builder);

      builder.finish(orc); // You could also call `Monster.finishMonsterBuffer(builder, orc);`.

      ByteBuffer buf = builder.dataBuffer();

      // Get access to the root:
      monster = Monster.getRootAsMonster(buf, monster);
      // Get and test the `inventory` FlatBuffer `vector`.
      int len =  monster.inventoryLength();
      for (int i = 0; i < len; i++) {
        //assert monster.inventory(i) == (float)i;
        w += monster.inventory(i);
      }
    }
  }

  public static void produceFloatVectorData(FloatVectorImpl.Builder vector, int start, int size) {
    int nextBase = start;
    vector.size(size);
    for (int i = 0; i < size; i++) {
      vector.set(i, Float.intBitsToFloat(floatToRawIntBits(i)));
    }
  }

  public static void floatVectorBenchmark(int array_size, int iteration) {
    float w = 0f;
    for (int l = 0; l < iteration; l++) {
      FloatVectorImpl.Builder vectorBuilder = new FloatVectorImpl.Builder(array_size);
      produceFloatVectorData(vectorBuilder, 0, array_size);
      FloatVectorImpl floatVector = new FloatVectorImpl();
      floatVector.init(vectorBuilder.getByteBuffer(), 0, array_size);
      for (int i = 0; i < floatVector.size(); i++) {
        w += floatVector.getFloat(i);
      }
    }
  }


}

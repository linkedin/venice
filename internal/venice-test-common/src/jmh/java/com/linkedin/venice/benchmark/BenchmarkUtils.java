package com.linkedin.venice.benchmark;

import static com.linkedin.venice.serializer.FastSerializerDeserializerFactory.getAvroGenericSerializer;
import static com.linkedin.venice.serializer.FastSerializerDeserializerFactory.getFastAvroGenericSerializer;

import com.linkedin.avro.api.PrimitiveFloatList;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.utils.Utils;
import java.io.ByteArrayInputStream;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
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
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.openjdk.jmh.infra.Blackhole;


public class BenchmarkUtils {
  private static final Set<String> OLDGEN_GC_NAMES =
      Utils.setOf("MarkSweepCompact", "PS MarkSweep", "ConcurrentMarkSweep", "G1 Mixed Generation");
  private static final Set<String> NEWGEN_GC_NAMES =
      Utils.setOf("Copy", "PS Scavenge", "ParNew", "ParNew", "G1 Young Generation");

  private BenchmarkUtils() {
  }

  public static Map<String, Float> runWithMetrics(Runnable code, String test, int numQueries, int numThreads) {

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
        int timeout = 60;
        if (!executorService.awaitTermination(timeout, TimeUnit.SECONDS)) {
          System.err.println("Failed to finish test within " + timeout + " seconds.");
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

  public static Map<String, Float> getJvmMetrics() {
    Map<String, Float> metrics = new HashMap<>();
    List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
    for (GarbageCollectorMXBean gcBean: gcBeans) {
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
    metricsDelta.forEach((key, value) -> {
      if (!key.equals("gc_new_time") && !key.equals("gc_old_time")) {
        System.out.format(testCaseName + ".%s=%f\n", key, value);
      }
    });
    DecimalFormat decimalFormat = new DecimalFormat("###,###.###");
    System.out.format(
        testCaseName + ".query_Volume: %s, time lapse: %f ms\n",
        decimalFormat.format(queryVolume),
        metricsDelta.get("uptime"));
    System.out.format(
        testCaseName + ".average_QPS: %s\n",
        decimalFormat.format(1000d * (double) queryVolume / metricsDelta.get("uptime")));
    System.out.format(
        testCaseName + ".average_latency: %s us\n",
        decimalFormat.format(
            (double) TimeUnit.MICROSECONDS
                .convert((long) metricsDelta.get("uptime").floatValue(), TimeUnit.MILLISECONDS)
                / (double) queryVolume));
  }

  public static void avroBenchmark(
      boolean fastAvro,
      int array_size,
      long iteration,
      boolean serializeOnce,
      boolean accessData,
      Blackhole blackhole) {
    float w = 0f;
    String schemaString =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"inventory\", \"type\" : {  \"type\" : \"array\", \"items\" : \"float\" }}] }";

    Schema schema = Schema.parse(schemaString);
    RecordSerializer<GenericRecord> serializer =
        fastAvro ? getFastAvroGenericSerializer(schema) : getAvroGenericSerializer(schema);
    RecordDeserializer<GenericRecord> deserializer = fastAvro
        ? FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(schema, schema)
        : SerializerDeserializerFactory.getAvroGenericDeserializer(schema, schema);
    List<Float> floats = new ArrayList(array_size);
    for (int l = 0; l < array_size; l++) {
      floats.add((float) l);
    }

    String fieldName = "inventory";
    GenericRecord recordToSerialize = new GenericData.Record(schema);
    GenericRecord recordToDeserialize = new GenericData.Record(schema);
    PrimitiveFloatList primitiveList;
    List<Float> list;
    int i;
    int n;
    BinaryDecoder decoder = AvroCompatibilityHelper.newBinaryDecoder(new byte[16]);
    byte[] bytes;

    // Initial serialization...
    recordToSerialize.put(fieldName, floats);
    bytes = serializer.serialize(recordToSerialize);
    ;

    for (i = 0; i < iteration; i++) {
      if (!serializeOnce) {
        recordToSerialize.put(fieldName, floats);
        bytes = serializer.serialize(recordToSerialize);
      }
      decoder = AvroCompatibilityHelper.newBinaryDecoder(new ByteArrayInputStream(bytes), false, decoder);
      recordToDeserialize = deserializer.deserialize(recordToDeserialize, decoder);
      blackhole.consume(recordToDeserialize);

      if (accessData) {
        list = (List<Float>) recordToDeserialize.get(0);
        if (list instanceof PrimitiveFloatList) {
          primitiveList = (PrimitiveFloatList) list;
          for (n = 0; n < primitiveList.size(); n++) {
            w += primitiveList.getPrimitive(n);
          }
        } else {
          for (n = 0; n < list.size(); n++) {
            w += list.get(n);
          }

          if (fastAvro && i == iteration - 1) {
            // Had enough time to warm up already... should be done by now.
            throw new VeniceException("list not of PrimitiveFloatList type!");
          }
        }
      }
    }
    blackhole.consume(w);
  }
}

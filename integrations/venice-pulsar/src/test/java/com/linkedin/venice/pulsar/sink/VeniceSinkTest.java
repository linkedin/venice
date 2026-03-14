package com.linkedin.venice.pulsar.sink;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.samza.VeniceSystemProducer;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class VeniceSinkTest {
  VenicePulsarSinkConfig config;
  VeniceSystemProducer producer;

  ScheduledExecutorService executor;

  @BeforeMethod
  public void setUp() {
    executor = Executors.newScheduledThreadPool(20);
    config = new VenicePulsarSinkConfig();
    config.setVeniceDiscoveryUrl("http://test:5555")
        .setVeniceRouterUrl("http://test:7777")
        .setStoreName("t1_n1_s1")
        .setKafkaSaslMechanism("PLAIN")
        .setKafkaSecurityProtocol("SASL_PLAINTEXT")
        .setKafkaSaslConfig("");

    producer = Mockito.mock(VeniceSystemProducer.class);
  }

  @AfterMethod
  public void tearDown() throws InterruptedException {
    if (executor != null) {
      executor.shutdownNow();
      executor.awaitTermination(5, TimeUnit.SECONDS);
      executor = null;
    }
  }

  @Test
  public void testVeniceSinkKvHappyPath() throws Exception {
    VenicePulsarSink sink = testSink(false, 1, 5);
    sink.close();
    verify(producer, atLeastOnce()).flush(anyString());
  }

  @Test
  public void testVeniceSinkStringHappyPath() throws Exception {
    VenicePulsarSink sink = testSink(true, 1, 5);
    sink.close();
    verify(producer, atLeastOnce()).flush(anyString());
  }

  /**
   * Test that the sink can handle messages when flush is slow.
   */
  @Test
  public void testVeniceSinkSlowFlush() throws Exception {
    VenicePulsarSink sink = testSink(false, 50, 100);
    sink.close();
    verify(producer, atLeastOnce()).flush(anyString());
    verify(sink, atLeastOnce()).throttle();
  }

  private VenicePulsarSink testSink(boolean valueAsString, int minFlushDelay, int maxFlushDelay) throws Exception {
    ConcurrentLinkedQueue<CompletableFuture<Void>> futures = new ConcurrentLinkedQueue<>();

    when(producer.put(Mockito.any(), Mockito.any())).thenAnswer((InvocationOnMock invocation) -> {
      CompletableFuture<Void> future = new CompletableFuture<>();
      executor.schedule(
          () -> future.complete(null),
          ThreadLocalRandom.current().nextInt(minFlushDelay, maxFlushDelay),
          TimeUnit.MILLISECONDS);

      futures.add(future);
      return future;
    });

    when(producer.delete(Mockito.any())).thenAnswer((InvocationOnMock invocation) -> {
      CompletableFuture<Void> future = new CompletableFuture<>();
      executor.schedule(
          () -> future.complete(null),
          ThreadLocalRandom.current().nextInt(minFlushDelay, maxFlushDelay),
          TimeUnit.MILLISECONDS);

      futures.add(future);
      return future;
    });

    // Flush mock: drain pending futures synchronously on the caller's thread (the sink's internal
    // scheduler). Optional sleep simulates slow flush for throttle tests. This avoids the previous
    // approach of scheduling a drain task on the test executor and blocking on f.get(), which caused
    // thread contention and flakiness under CI load.
    doAnswer((InvocationOnMock invocation) -> {
      if (minFlushDelay > 0) {
        Thread.sleep(ThreadLocalRandom.current().nextInt(minFlushDelay, maxFlushDelay));
      }
      CompletableFuture<Void> future;
      while ((future = futures.poll()) != null) {
        future.complete(null);
      }
      return null;
    }).when(producer).flush(anyString());

    VenicePulsarSink sink = Mockito.spy(new VenicePulsarSink());
    sink.open(config, producer, null);

    List<Record<GenericObject>> records = new LinkedList<>();

    // send a few records, enough to trigger a flush and throttle
    for (int i = 0; i < 100; i++) {
      Record<GenericObject> rec = getRecord(valueAsString, "k" + i, "v" + i);
      records.add(rec);
      sink.write(rec);
    }

    for (int i = 0; i < 100; i++) {
      Record<GenericObject> rec = getRecord(valueAsString, "k" + i, null);
      records.add(rec);
      sink.write(rec);
    }

    for (Record<GenericObject> rec: records) {
      verify(rec, timeout(5000).times(1)).ack();
    }

    return sink;
  }

  @Test
  public void testVeniceSinkFlushFail() throws Exception {
    ConcurrentLinkedQueue<CompletableFuture<Void>> futures = new ConcurrentLinkedQueue<>();

    AtomicInteger count = new AtomicInteger(0);
    when(producer.put(Mockito.any(), Mockito.any())).thenAnswer((InvocationOnMock invocation) -> {
      CompletableFuture<Void> future = new CompletableFuture<>();
      executor.schedule(() -> {
        if (count.incrementAndGet() % 10 == 0) {
          future.completeExceptionally(new Exception("Injected error"));
        } else {
          future.complete(null);
        }
      }, ThreadLocalRandom.current().nextInt(1, 25), TimeUnit.MILLISECONDS);

      futures.add(future);
      return future;
    });

    doAnswer((InvocationOnMock invocation) -> null).when(producer).flush(anyString());

    VenicePulsarSink sink = new VenicePulsarSink();
    sink.open(config, producer, null);

    try {
      for (int i = 0; i < 20; i++) {
        Record<GenericObject> rec = getRecord(false, "k" + i, "v" + i);
        sink.write(rec);
      }
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Error while flushing records"));
      assertTrue(e.getCause().getMessage().contains("Injected error"));
    }

    Record<GenericObject> rec = getRecord(false, "k", "v");
    try {
      sink.write(rec);
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains("Error while flushing records"));
      assertTrue(e.getCause().getMessage().contains("Injected error"));
    }

    sink.close();
  }

  @Test
  public void testVeniceSinkFlushThrow() throws Exception {
    ConcurrentLinkedQueue<CompletableFuture<Void>> futures = new ConcurrentLinkedQueue<>();

    AtomicInteger count = new AtomicInteger(0);
    when(producer.delete(Mockito.any())).thenAnswer((InvocationOnMock invocation) -> {
      CompletableFuture<Void> future = new CompletableFuture<>();
      executor.schedule(() -> {
        if (count.incrementAndGet() % 10 == 0) {
          future.completeExceptionally(new Exception("Injected error"));
        } else {
          future.complete(null);
        }
      }, ThreadLocalRandom.current().nextInt(1, 25), TimeUnit.MILLISECONDS);

      futures.add(future);
      return future;
    });

    doThrow(new RuntimeException("Injected error")).when(producer).flush(anyString());

    VenicePulsarSink sink = new VenicePulsarSink();
    sink.open(config, producer, null);

    try {
      for (int i = 0; i < 20; i++) {
        // null value means delete
        Record<GenericObject> rec = getRecord(false, "k" + i, null);
        sink.write(rec);
      }
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Error while flushing records"));
      assertTrue(e.getCause().getMessage().contains("Injected error"));
    }

    Record<GenericObject> rec = getRecord(false, "k", null);
    try {
      sink.write(rec);
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains("Error while flushing records"));
      assertTrue(e.getCause().getMessage().contains("Injected error"));
    }

    try {
      sink.close();
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      assertTrue(cause.getMessage().contains("Error while flushing records"));
      assertTrue(cause.getCause().getMessage().contains("Injected error"));
    }
  }

  private GenericObject getStringObj(String key, String value) {
    return new GenericObject() {
      @Override
      public SchemaType getSchemaType() {
        return SchemaType.STRING;
      }

      @Override
      public Object getNativeObject() {
        return value;
      }
    };
  }

  private Record<GenericObject> getRecord(boolean valueAsString, String key, String value) {
    Record<GenericObject> rec = Mockito.mock(Record.class);
    when(rec.getKey()).thenReturn(Optional.of(key));
    if (valueAsString) {
      when(rec.getValue()).thenReturn(getStringObj(key, value));
    } else {
      when(rec.getValue()).thenReturn(getGenericObject(key, value));
    }
    return rec;
  }

  private GenericObject getGenericObject(String key, String value) {
    return new GenericObject() {
      @Override
      public SchemaType getSchemaType() {
        return SchemaType.KEY_VALUE;
      }

      @Override
      public Object getNativeObject() {
        return new KeyValue<>(key, value);
      }
    };
  }
}

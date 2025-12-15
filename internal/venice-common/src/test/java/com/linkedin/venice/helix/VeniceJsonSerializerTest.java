package com.linkedin.venice.helix;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for VeniceJsonSerializer to verify per-instance ObjectMapper isolation
 * and thread safety of serializer instantiation.
 */
public class VeniceJsonSerializerTest {
  /**
   * Test that each serializer instance gets its own ObjectMapper.
   */
  @Test
  public void testEachSerializerHasOwnObjectMapper() {
    TestSerializer serializer1 = new TestSerializer();
    TestSerializer serializer2 = new TestSerializer();

    // Each serializer should have its own ObjectMapper instance
    Assert.assertNotSame(
        serializer1.getObjectMapper(),
        serializer2.getObjectMapper(),
        "Each serializer should have its own ObjectMapper instance");
  }

  /**
   * Test that mixins registered on one serializer don't affect another.
   */
  @Test
  public void testMixinIsolationBetweenSerializers() throws IOException {
    // Create two serializers with different mixins for the same class
    TestSerializerWithMixinA serializerA = new TestSerializerWithMixinA();
    TestSerializerWithMixinB serializerB = new TestSerializerWithMixinB();

    TestData data = new TestData("value1", "value2");

    // Serialize with both serializers
    byte[] dataA = serializerA.serialize(data, null);
    byte[] dataB = serializerB.serialize(data, null);

    // Both should work independently without interference
    TestData deserializedA = serializerA.deserialize(dataA, null);
    TestData deserializedB = serializerB.deserialize(dataB, null);

    Assert.assertEquals(deserializedA.field1, "value1");
    Assert.assertEquals(deserializedB.field1, "value1");
  }

  /**
   * Test concurrent serializer instantiation to ensure thread safety.
   */
  @Test
  public void testConcurrentSerializerInstantiation() throws InterruptedException {
    int threadCount = 20;
    int iterationsPerThread = 50;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(threadCount);
    AtomicInteger successCount = new AtomicInteger(0);
    List<Exception> exceptions = new ArrayList<>();

    for (int i = 0; i < threadCount; i++) {
      executor.submit(() -> {
        try {
          // Wait for all threads to be ready
          startLatch.await();

          // Create multiple serializers concurrently
          for (int j = 0; j < iterationsPerThread; j++) {
            TestSerializer serializer = new TestSerializer();
            TestData data = new TestData("field1_" + j, "field2_" + j);

            // Serialize and deserialize
            byte[] serialized = serializer.serialize(data, null);
            TestData deserialized = serializer.deserialize(serialized, null);

            // Verify correctness
            if (data.field1.equals(deserialized.field1) && data.field2.equals(deserialized.field2)) {
              successCount.incrementAndGet();
            }
          }
        } catch (Exception e) {
          synchronized (exceptions) {
            exceptions.add(e);
          }
        } finally {
          doneLatch.countDown();
        }
      });
    }

    // Start all threads at once
    startLatch.countDown();

    // Wait for completion
    boolean completed = doneLatch.await(30, TimeUnit.SECONDS);
    executor.shutdown();

    Assert.assertTrue(completed, "All threads should complete within timeout");
    Assert.assertTrue(exceptions.isEmpty(), "No exceptions should occur: " + exceptions);
    Assert.assertEquals(
        successCount.get(),
        threadCount * iterationsPerThread,
        "All serialization/deserialization operations should succeed");
  }

  /**
   * Test that ObjectMapper configuration is properly applied.
   */
  @Test
  public void testObjectMapperConfiguration() throws IOException {
    TestSerializer serializer = new TestSerializer();

    // Create JSON with unknown property
    String jsonWithUnknownField =
        "{\"field1\":\"value1\",\"field2\":\"value2\",\"unknownField\":\"should_be_ignored\"}";

    // Should not fail due to FAIL_ON_UNKNOWN_PROPERTIES = false
    TestData deserialized = serializer.deserialize(jsonWithUnknownField.getBytes(), null);

    Assert.assertEquals(deserialized.field1, "value1");
    Assert.assertEquals(deserialized.field2, "value2");
  }

  /**
   * Test that subclass can override createObjectMapper to add custom configuration.
   */
  @Test
  public void testSubclassCanCustomizeObjectMapper() {
    TestSerializerWithMixinA serializer = new TestSerializerWithMixinA();
    ObjectMapper mapper = serializer.getObjectMapper();

    // Verify that mixin was registered
    Assert.assertNotNull(mapper.findMixInClassFor(TestData.class));
  }

  // Test classes and serializers

  public static class TestData {
    public String field1;
    public String field2;

    public TestData() {
    }

    public TestData(String field1, String field2) {
      this.field1 = field1;
      this.field2 = field2;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof TestData)) {
        return false;
      }
      TestData other = (TestData) obj;
      return (field1 == null ? other.field1 == null : field1.equals(other.field1))
          && (field2 == null ? other.field2 == null : field2.equals(other.field2));
    }
  }

  public static class TestSerializer extends VeniceJsonSerializer<TestData> {
    public TestSerializer() {
      super(TestData.class);
    }
  }

  public static class TestSerializerWithMixinA extends VeniceJsonSerializer<TestData> {
    public TestSerializerWithMixinA() {
      super(TestData.class);
    }

    @Override
    protected ObjectMapper createObjectMapper() {
      ObjectMapper mapper = super.createObjectMapper();
      mapper.addMixIn(TestData.class, TestDataMixinA.class);
      return mapper;
    }

    public static class TestDataMixinA {
      @JsonCreator
      public TestDataMixinA(@JsonProperty("field1") String field1, @JsonProperty("field2") String field2) {
      }
    }
  }

  public static class TestSerializerWithMixinB extends VeniceJsonSerializer<TestData> {
    public TestSerializerWithMixinB() {
      super(TestData.class);
    }

    @Override
    protected ObjectMapper createObjectMapper() {
      ObjectMapper mapper = super.createObjectMapper();
      mapper.addMixIn(TestData.class, TestDataMixinB.class);
      return mapper;
    }

    public static class TestDataMixinB {
      @JsonCreator
      public TestDataMixinB(@JsonProperty("field1") String field1, @JsonProperty("field2") String field2) {
      }
    }
  }
}

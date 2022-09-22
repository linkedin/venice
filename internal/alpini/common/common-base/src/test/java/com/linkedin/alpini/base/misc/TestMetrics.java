package com.linkedin.alpini.base.misc;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class TestMetrics {
  public enum MetricNames {
    STORAGE_SERVER_LATENCY, STORAGE_MYSQL_LATENCY, STORAGE_INDEX_LATENCY, ROUTER_ROUTING_TIME, ROUTER_SERVER_TIME
  }

  /**
   * Create an EspressoResponseMetrics with some submetrics and test that it serializes and
   * deserializes as expected.
   * @throws IOException if serialization raises IOException
   */
  @Test(groups = "unit")
  public void testSerialization() throws IOException {
    // Metrics for a call to storage node 1. Metrics for storage node 1 are all in the hundreds
    Metrics storage1 = new Metrics(
        "http://storage1:1234",
        "GET",
        "/EspressoDB/EmailTest/12345",
        false,
        new TimeValue(400, TimeUnit.MILLISECONDS));
    storage1.setNumReadCapacityUnits(3);
    storage1.setNumWriteCapacityUnits(2);
    storage1.setMetric(MetricNames.STORAGE_SERVER_LATENCY, new TimeValue(300, TimeUnit.MILLISECONDS));
    storage1.setMetric(MetricNames.STORAGE_MYSQL_LATENCY, new TimeValue(200, TimeUnit.MILLISECONDS));
    storage1.setMetric(MetricNames.STORAGE_INDEX_LATENCY, new TimeValue(100, TimeUnit.MILLISECONDS));

    // Metrics for a call to storage node 2
    Metrics storage2 = new Metrics(
        "http://storage2:1234",
        "GET",
        "/EspressoDB/EmailTest/67890",
        false,
        new TimeValue(4000, TimeUnit.MILLISECONDS));
    storage2.setNumReadCapacityUnits(5);
    storage2.setNumWriteCapacityUnits(0);
    storage2.setMetric(MetricNames.STORAGE_SERVER_LATENCY, new TimeValue(3000, TimeUnit.MILLISECONDS));
    storage2.setMetric(MetricNames.STORAGE_MYSQL_LATENCY, new TimeValue(2000, TimeUnit.MILLISECONDS));
    storage2.setMetric(MetricNames.STORAGE_INDEX_LATENCY, new TimeValue(1000, TimeUnit.MILLISECONDS));

    // Metrics for a call to the router, which delegates to the two storage nodes above
    Metrics routerMetrics = new Metrics(
        "http://router:1234/",
        "GET",
        "/EspressoDB/EmailTest/12345,67890",
        false,
        new TimeValue(40000, TimeUnit.MILLISECONDS));
    routerMetrics.setNumReadCapacityUnits(15);
    routerMetrics.setNumWriteCapacityUnits(15);
    routerMetrics.setMetric(MetricNames.ROUTER_SERVER_TIME, new TimeValue(20000, TimeUnit.MILLISECONDS));
    routerMetrics.setMetric(MetricNames.ROUTER_ROUTING_TIME, new TimeValue(10000, TimeUnit.MILLISECONDS));

    // Add the storage node calls as subrequests to the router call
    routerMetrics.addSubrequest(storage1);
    routerMetrics.addSubrequest(storage2);

    // Serialize the metrics, then deserialize into a new instance
    String serialized = Metrics.toJson(routerMetrics);
    routerMetrics = Metrics.fromJson(serialized);

    // Check that the totals in the deserialized instance are as expected.
    Assert.assertEquals(
        routerMetrics.getMetricTotal(MetricNames.STORAGE_SERVER_LATENCY),
        new TimeValue(3300, TimeUnit.MILLISECONDS));
    Assert.assertEquals(
        routerMetrics.getMetricTotal(MetricNames.STORAGE_MYSQL_LATENCY),
        new TimeValue(2200, TimeUnit.MILLISECONDS));
    Assert.assertEquals(
        routerMetrics.getMetricTotal(MetricNames.STORAGE_INDEX_LATENCY),
        new TimeValue(1100, TimeUnit.MILLISECONDS));
    Assert.assertEquals(
        routerMetrics.getMetricTotal(MetricNames.ROUTER_SERVER_TIME),
        new TimeValue(20000, TimeUnit.MILLISECONDS));
    Assert.assertEquals(
        routerMetrics.getMetricTotal(MetricNames.ROUTER_ROUTING_TIME),
        new TimeValue(10000, TimeUnit.MILLISECONDS));

    Assert.assertEquals(routerMetrics.getNumReadCapacityUnits(), 15);
    Assert.assertEquals(routerMetrics.getNumWriteCapacityUnits(), 15);
    Assert.assertEquals(routerMetrics.computeTotalReadCapacityUnits(), 23);
    Assert.assertEquals(routerMetrics.computeTotalWriteCapacityUnits(), 17);

    // Check that toString returns the same text as toJson
    Assert.assertEquals(routerMetrics.toString(), serialized);
  }

  /**
   * Create an EspressoResponseMetrics with some submetrics and test that getMetricTotal returns
   * expected values.
   */
  @Test(groups = "unit")
  public void testTotalCalculation() {
    // Metrics for a call to storage node 1. Metrics for storage node 1 are all in the hundreds
    Metrics storage1 = new Metrics(
        "http://storage1:1234",
        "GET",
        "/EspressoDB/EmailTest/12345",
        false,
        new TimeValue(400, TimeUnit.MILLISECONDS));
    storage1.setMetric(MetricNames.STORAGE_SERVER_LATENCY, new TimeValue(300, TimeUnit.MILLISECONDS));
    storage1.setMetric(MetricNames.STORAGE_MYSQL_LATENCY, new TimeValue(200, TimeUnit.MILLISECONDS));
    storage1.setMetric(MetricNames.STORAGE_INDEX_LATENCY, new TimeValue(100, TimeUnit.MILLISECONDS));

    // Metrics for a call to storage node 2
    Metrics storage2 = new Metrics(
        "http://storage2:1234",
        "GET",
        "/EspressoDB/EmailTest/67890",
        false,
        new TimeValue(4000, TimeUnit.MILLISECONDS));
    storage2.setMetric(MetricNames.STORAGE_SERVER_LATENCY, new TimeValue(3000, TimeUnit.MILLISECONDS));
    storage2.setMetric(MetricNames.STORAGE_MYSQL_LATENCY, new TimeValue(2000, TimeUnit.MILLISECONDS));

    // Metrics for a call to the router, which delegates to the two storage nodes above
    Metrics routerMetrics = new Metrics(
        "http://router:1234/",
        "GET",
        "/EspressoDB/EmailTest/12345,67890",
        false,
        new TimeValue(40000, TimeUnit.MILLISECONDS));
    routerMetrics.setMetric(MetricNames.ROUTER_SERVER_TIME, new TimeValue(20000, TimeUnit.MILLISECONDS));
    routerMetrics.setMetric(MetricNames.ROUTER_ROUTING_TIME, new TimeValue(10000, TimeUnit.MILLISECONDS));

    // This metric does not belong at the router level, but the getMetricTotal function should count it regardless of
    // where it is at.
    routerMetrics.setMetric(MetricNames.STORAGE_INDEX_LATENCY, new TimeValue(30000, TimeUnit.MILLISECONDS));

    // This metric does not belong at the router level, but the getMetricTotal function should count it regardless of
    // where it is at.
    storage1.setMetric(MetricNames.ROUTER_ROUTING_TIME, new TimeValue(500, TimeUnit.MILLISECONDS));

    // Add the storage node calls as subrequests to the router call
    routerMetrics.addSubrequest(storage1);
    routerMetrics.addSubrequest(storage2);

    // Check that the totals are as expected.
    Assert.assertEquals(
        routerMetrics.getMetricTotal(MetricNames.STORAGE_SERVER_LATENCY),
        new TimeValue(3300, TimeUnit.MILLISECONDS));
    Assert.assertEquals(
        routerMetrics.getMetricTotal(MetricNames.STORAGE_MYSQL_LATENCY),
        new TimeValue(2200, TimeUnit.MILLISECONDS));
    Assert.assertEquals(
        routerMetrics.getMetricTotal(MetricNames.STORAGE_INDEX_LATENCY),
        new TimeValue(30100, TimeUnit.MILLISECONDS));
    Assert.assertEquals(
        routerMetrics.getMetricTotal(MetricNames.ROUTER_SERVER_TIME),
        new TimeValue(20000, TimeUnit.MILLISECONDS));
    Assert.assertEquals(
        routerMetrics.getMetricTotal(MetricNames.ROUTER_ROUTING_TIME),
        new TimeValue(10500, TimeUnit.MILLISECONDS));
  }

  /**
   * Create an EspressoResponseMetrics with some submetrics and test that getMetricMax returns
   * expected values.
   */
  @Test(groups = "unit")
  public void testMaxCalculation() throws IOException {
    // Metrics for a call to storage node 1. Metrics for storage node 1 are all in the hundreds
    Metrics storage1 = new Metrics(
        "http://storage1:1234",
        "GET",
        "/EspressoDB/EmailTest/12345",
        false,
        new TimeValue(400, TimeUnit.MILLISECONDS));
    storage1.setMetric(MetricNames.STORAGE_SERVER_LATENCY, new TimeValue(300, TimeUnit.MILLISECONDS));
    storage1.setMetric(MetricNames.STORAGE_MYSQL_LATENCY, new TimeValue(200, TimeUnit.MILLISECONDS));
    storage1.setMetric(MetricNames.STORAGE_INDEX_LATENCY, new TimeValue(100, TimeUnit.MILLISECONDS));

    // Metrics for a call to storage node 2
    Metrics storage2 = new Metrics(
        "http://storage2:1234",
        "GET",
        "/EspressoDB/EmailTest/67890",
        false,
        new TimeValue(4000, TimeUnit.MILLISECONDS));
    storage2.setMetric(MetricNames.STORAGE_SERVER_LATENCY, new TimeValue(3000, TimeUnit.MILLISECONDS));
    storage2.setMetric(MetricNames.STORAGE_MYSQL_LATENCY, new TimeValue(2000, TimeUnit.MILLISECONDS));

    // Metrics for a call to the router, which delegates to the two storage nodes above
    Metrics routerMetrics = new Metrics(
        "http://router:1234/",
        "GET",
        "/EspressoDB/EmailTest/12345,67890",
        false,
        new TimeValue(40000, TimeUnit.MILLISECONDS));
    routerMetrics.setMetric(MetricNames.ROUTER_SERVER_TIME, new TimeValue(20000, TimeUnit.MILLISECONDS));
    routerMetrics.setMetric(MetricNames.ROUTER_ROUTING_TIME, new TimeValue(10000, TimeUnit.MILLISECONDS));

    // This metric does not belong at the router level, but the getMetricTotal function should count it regardless of
    // where it is at.
    routerMetrics.setMetric(MetricNames.STORAGE_INDEX_LATENCY, new TimeValue(30000, TimeUnit.MILLISECONDS));

    // This metric does not belong at the router level, but the getMetricTotal function should count it regardless of
    // where it is at.
    storage1.setMetric(MetricNames.ROUTER_ROUTING_TIME, new TimeValue(500000, TimeUnit.MILLISECONDS));

    // Add the storage node calls as subrequests to the router call
    routerMetrics.addSubrequest(storage1);
    routerMetrics.addSubrequest(storage2);

    // Check that the totals are as expected.
    Assert.assertEquals(
        routerMetrics.getMetricMax(MetricNames.STORAGE_SERVER_LATENCY),
        new TimeValue(3000, TimeUnit.MILLISECONDS));
    Assert.assertEquals(
        routerMetrics.getMetricMax(MetricNames.STORAGE_MYSQL_LATENCY),
        new TimeValue(2000, TimeUnit.MILLISECONDS));
    Assert.assertEquals(
        routerMetrics.getMetricMax(MetricNames.STORAGE_INDEX_LATENCY),
        new TimeValue(30000, TimeUnit.MILLISECONDS));
    Assert.assertEquals(
        routerMetrics.getMetricMax(MetricNames.ROUTER_SERVER_TIME),
        new TimeValue(20000, TimeUnit.MILLISECONDS));
    Assert.assertEquals(
        routerMetrics.getMetricMax(MetricNames.ROUTER_ROUTING_TIME),
        new TimeValue(500000, TimeUnit.MILLISECONDS));
  }

  @Test(groups = "unit")
  public void testSerialization2() throws IOException {
    // Start with a know serialized value. Deserialize that to an EspressoResponseMetrics, then reserialize.
    String expected =
        "{\"method\":\"GET\",\"host\":null,\"uri\":\"/EspressoDB/EmailTest/298/1\",\"timedOut\":false,\"clientSideLatency\":null,\"metrics\":{\"ROUTER_SERVER_TIME\":{\"rawValue\":5001000,\"unit\":\"NANOSECONDS\"},\"ROUTER_RESPONSE_WAIT_TIME\":{\"rawValue\":4817000,\"unit\":\"NANOSECONDS\"},\"ROUTER_ROUTING_TIME\":{\"rawValue\":154000,\"unit\":\"NANOSECONDS\"}},\"subrequests\":[{\"method\":\"GET\",\"host\":\"jwesterm-md.linkedin.biz:12918\",\"uri\":\"/EspressoDB/EmailTest/298/1\",\"timedOut\":false,\"clientSideLatency\":{\"rawValue\":4817000,\"unit\":\"NANOSECONDS\"},\"metrics\":{\"STORAGE_INDEX_LATENCY\":null,\"STORAGE_PROCESSOR_LATENCY\":{\"rawValue\":1830000,\"unit\":\"NANOSECONDS\"},\"STORAGE_CONTAINER_LATENCY\":{\"rawValue\":407000,\"unit\":\"NANOSECONDS\"},\"STORAGE_SERVER_LATENCY\":{\"rawValue\":2241000,\"unit\":\"NANOSECONDS\"},\"STORAGE_INDEX_OPEN_LATENCY\":null,\"STORAGE_MYSQL_POOL_LATENCY\":{\"rawValue\":300000,\"unit\":\"NANOSECONDS\"},\"STORAGE_MYSQL_LATENCY\":{\"rawValue\":1253000,\"unit\":\"NANOSECONDS\"}},\"subrequests\":[]}]}";
    Metrics metrics = Metrics.fromJson(expected);
    String actual = Metrics.toJson(metrics);

    // We cannot compare the "actual" and "expected" strings directly, because serialization may
    // reorder elements (for example, it may put "host" after "uri"). Instead, we deserialized both
    // the expected and actual to a raw Map, and then compare the contents of the maps.
    ObjectMapper objectMapper = SimpleJsonMapper.getObjectMapper();
    Map<String, Object> expectedMap = objectMapper.readValue(expected, new TypeReference<Map<String, Object>>() {
    });
    Map<String, Object> actualMap = objectMapper.readValue(actual, new TypeReference<Map<String, Object>>() {
    });

    // Compare the maps
    Assert.assertEquals(actualMap, expectedMap);

    // Just to make sure nothing went terribly wrong, make sure the expectedMap has the right
    // number of elements.
    Assert.assertEquals(actualMap.size(), 7);
  }

  /**
   * Tests the workflow that will be typical in a client-server interaction. Specifically, the
   * server returns a serialized EspressoResponseMetrics with metrics but no host, URI, or client
   * side latency. The client must deserialize the metrics, then update these fields.
   */
  @Test(groups = "unit")
  public void testWorkflow() throws IOException {
    String serialized = "{\n" + "    \"metrics\": {\n" + "        \"ROUTER_SERVER_TIME\": {\n"
        + "            \"rawValue\": 20000,\n" + "            \"unit\": \"MILLISECONDS\"\n" + "        },\n"
        + "        \"ROUTER_ROUTING_TIME\": {\n" + "            \"rawValue\": 10000,\n"
        + "            \"unit\": \"MILLISECONDS\"\n" + "        }\n" + "    }\n" + "}";

    Metrics routerMetrics = Metrics.fromJson(serialized);
    routerMetrics.setHost("http://router:1234/");
    routerMetrics.setUri("/EspressoDB/EmailTest/12345,67890");
    routerMetrics.setMethod("GET");
    routerMetrics.setTimedOut(false);
    routerMetrics.setClientSideLatency(new TimeValue(40000, TimeUnit.MILLISECONDS));

    String expected =
        "{\"method\":\"GET\",\"host\":\"http://router:1234/\",\"uri\":\"/EspressoDB/EmailTest/12345,67890\",\"timedOut\":false,\"clientSideLatency\":{\"rawValue\":40000,\"unit\":\"MILLISECONDS\"},\"metrics\":{\"ROUTER_SERVER_TIME\":{\"rawValue\":20000,\"unit\":\"MILLISECONDS\"},\"ROUTER_ROUTING_TIME\":{\"rawValue\":10000,\"unit\":\"MILLISECONDS\"}},\"subrequests\":[]}";
    String actual = Metrics.toJson(routerMetrics);

    // We cannot compare the "actual" and "expected" strings directly, because serialization may
    // reorder elements (for example, it may put "host" after "uri"). Instead, we deserialized both
    // the expected and actual to a raw Map, and then compare the contents of the maps.
    ObjectMapper objectMapper = SimpleJsonMapper.getObjectMapper();
    Map<String, Object> expectedMap = objectMapper.readValue(expected, new TypeReference<Map<String, Object>>() {
    });
    Map<String, Object> actualMap = objectMapper.readValue(actual, new TypeReference<Map<String, Object>>() {
    });

    // Compare the maps
    Assert.assertEquals(actualMap, expectedMap);

    // Just to make sure nothing went terribly wrong, make sure the expectedMap has the right
    // number of elements.
    Assert.assertEquals(actualMap.size(), 7);
  }
}

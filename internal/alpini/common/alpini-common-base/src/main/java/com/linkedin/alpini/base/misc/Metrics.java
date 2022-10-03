/*
 * $Id$
 */
package com.linkedin.alpini.base.misc;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.annotation.Nonnull;


/**
 * @author Jemiah Westerman&lt;jwesterman@linkedin.com&gt;
 * @version $Revision$
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Metrics {
  @JsonIgnore
  private static final String READ_CAPACITY_UNITS = "READ_CAPACITY_UNITS";
  @JsonIgnore
  private static final String WRITE_CAPACITY_UNITS = "WRITE_CAPACITY_UNITS";

  private String _host;
  private String _uri;
  private String _method;
  private boolean _timedOut;
  private TimeValue _clientSideLatency;
  private @Nonnull Map<String, TimeValue> _metrics = new HashMap<>();
  private @Nonnull List<Metrics> _subrequests = new LinkedList<>();

  @JsonIgnore
  private transient Object _path;

  public Metrics() {
  }

  public Metrics(String host, String method, String uri, boolean timedOut, TimeValue clientSideLatency) {
    _host = host;
    _uri = uri;
    _method = method;
    _timedOut = timedOut;
    _clientSideLatency = clientSideLatency;
  }

  public String getHost() {
    return _host;
  }

  public void setHost(String host) {
    _host = host;
  }

  public String getUri() {
    return _uri;
  }

  public void setUri(String uri) {
    _uri = uri;
  }

  public String getMethod() {
    return _method;
  }

  public void setMethod(String method) {
    _method = method;
  }

  public boolean isTimedOut() {
    return _timedOut;
  }

  public void setTimedOut(boolean timedOut) {
    _timedOut = timedOut;
  }

  public TimeValue getClientSideLatency() {
    return _clientSideLatency;
  }

  public void setClientSideLatency(TimeValue clientSideLatency) {
    _clientSideLatency = clientSideLatency;
  }

  @JsonIgnore
  public long getNumReadCapacityUnits() {
    return Optional.ofNullable(_metrics.get(READ_CAPACITY_UNITS)).map(TimeValue::getRawValue).orElse(0L);
  }

  public void setNumReadCapacityUnits(long numReadCapacityUnits) {
    _metrics.computeIfAbsent(READ_CAPACITY_UNITS, k -> new TimeValue()).setRawValue(numReadCapacityUnits);
  }

  @JsonIgnore
  public long getNumWriteCapacityUnits() {
    return Optional.ofNullable(_metrics.get(WRITE_CAPACITY_UNITS)).map(TimeValue::getRawValue).orElse(0L);
  }

  public void setNumWriteCapacityUnits(long numWriteCapacityUnits) {
    _metrics.computeIfAbsent(WRITE_CAPACITY_UNITS, k -> new TimeValue()).setRawValue(numWriteCapacityUnits);
  }

  /**
   * Computes the total Read Capacity Units, summed across the top level request and all sub-requests
   * @return summed total RCUs
   */
  public long computeTotalReadCapacityUnits() {
    return getNumReadCapacityUnits()
        + getSubrequests().stream().mapToLong(Metrics::computeTotalReadCapacityUnits).sum();
  }

  /**
   * Computes the total Write Capacity Units, summed across the top level request and all sub-requests
   * @return summed total WCUs
   */
  public long computeTotalWriteCapacityUnits() {
    return getNumWriteCapacityUnits()
        + getSubrequests().stream().mapToLong(Metrics::computeTotalWriteCapacityUnits).sum();
  }

  public Map<String, TimeValue> getMetrics() {
    return _metrics;
  }

  public void setMetrics(Map<String, TimeValue> metrics) {
    _metrics = Objects.requireNonNull(metrics, "metrics cannot be null");
  }

  public void setMetric(String name, TimeValue value) {
    _metrics.put(name, value);
  }

  public <E extends Enum<E>> void setMetric(E name, TimeValue value) {
    setMetric(name.toString(), value);
  }

  /**
   * Return the total TimeValue for a metric, summed across the top level request and all sub-requests.
   * @param name metric to retrieve
   * @return the summed total value for the metric
   */
  public TimeValue getMetricTotal(String name) {
    TimeValue total = new TimeValue(0, TimeUnit.NANOSECONDS);
    TimeValue entry = _metrics.get(name);
    if (entry != null) {
      total = total.add(entry);
    }
    for (Metrics m: _subrequests) {
      total = total.add(m.getMetricTotal(name));
    }
    return total;
  }

  public <E extends Enum<E>> TimeValue getMetricTotal(E name) {
    return getMetricTotal(name.toString());
  }

  /**
   * Return the maximum TimeValue for a metric, across the top level request and all sub-requests.
   * @param name metric to retrieve
   * @return the single largest value for the metric
   */
  public TimeValue getMetricMax(String name) {
    TimeValue max = new TimeValue(0, TimeUnit.NANOSECONDS);
    TimeValue v = _metrics.get(name);
    if (v != null) {
      if (max.compareTo(v) < 0) {
        max = v;
      }
    }
    for (Metrics m: _subrequests) {
      v = m.getMetricMax(name);
      if (max.compareTo(v) < 0) {
        max = v;
      }
    }
    return max;
  }

  public <E extends Enum<E>> TimeValue getMetricMax(E name) {
    return getMetricMax(name.toString());
  }

  public @Nonnull List<Metrics> getSubrequests() {
    return _subrequests;
  }

  public void setSubrequests(@Nonnull List<Metrics> subrequests) {
    _subrequests = Objects.requireNonNull(subrequests, "subrequests");
  }

  public void addSubrequest(Metrics subrequest) {
    if (subrequest != null) {
      _subrequests.add(subrequest);
    }
  }

  private static final Supplier<ObjectMapper> OBJECT_MAPPER = SimpleJsonMapper::getObjectMapper;

  public static Metrics fromJson(String json) throws IOException {
    StringReader sr = new StringReader(json);
    try {
      return OBJECT_MAPPER.get().readValue(sr, Metrics.class);
    } catch (JsonParseException ex) {
      throw new IOException("JSON Parsing Failed.", ex);
    }
  }

  public static String toJson(Metrics metrics) {
    StringWriter sw = new StringWriter();
    ObjectMapper m = OBJECT_MAPPER.get();
    JsonFactory f = m.getFactory();
    return ExceptionUtil.checkException(() -> {
      try (JsonGenerator g = f.createGenerator(sw)) {
        m.writeValue(g, metrics);
        sw.flush();
        return sw.toString();
      }
      // Unlike "fromJson" where you could actually get a garbage string, toJson should always work.
      // Since we never expect to see an Exception here, we won't force the client to catch it. If
      // it ever does happen then raise as RuntimeException. Tests in TestEspressoResponseMetrics
      // should prevent this from ever occurring.
    }, "JSON Generation Failed");
  }

  @Override
  public String toString() {
    try {
      return Metrics.toJson(this);
    } catch (RuntimeException ex) {
      // Should never get RuntimeException serializing response metrics. We know this class can be
      // converted to JSON and have unit tests for this. Catching the RuntimeException anyway,
      // because it would be really lame to crash an application because of a log write that uses
      // toString() or something like that.
      assert false;
      return super.toString();
    }
  }

  @JsonIgnore
  public void setPath(Object path) {
    _path = path;
  }

  @JsonIgnore
  @SuppressWarnings("unchecked")
  public <P> P getPath() {
    return (P) _path;
  }
}

package com.linkedin.venice.listener.request;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;
import java.net.URI;
import java.util.List;
import java.util.Map;


/**
 * On-demand request that starts or stops a key/partition profiling session for a single store
 * version on this server. Routed via {@link com.linkedin.venice.meta.QueryAction#KEY_PARTITION_PROFILER}.
 *
 * <p>URL grammar:
 * <pre>
 *   POST /key_partition_profiler/&lt;store_version&gt;/start?duration=&lt;seconds&gt;[&amp;topK=&lt;n&gt;]
 *   POST /key_partition_profiler/&lt;store_version&gt;/stop
 * </pre>
 *
 * <p>Both {@code duration} and {@code topK} only apply to {@code start}. {@code duration} is
 * required for {@code start}.
 */
public class KeyPartitionProfilerRequest {
  /** Sub-action carried in the trailing URL path segment. */
  public enum Action {
    START, STOP
  }

  private final String storeVersion;
  private final String storeName;
  private final Action action;
  private final Long durationMs;
  private final Integer topK;

  private KeyPartitionProfilerRequest(String storeVersion, Action action, Long durationMs, Integer topK) {
    this.storeVersion = storeVersion;
    this.storeName = Version.parseStoreFromKafkaTopicName(storeVersion);
    this.action = action;
    this.durationMs = durationMs;
    this.topK = topK;
  }

  public static KeyPartitionProfilerRequest parseHttpRequest(HttpRequest request, URI fullUri) {
    // [0]""/[1]"key_partition_profiler"/[2]"store_version"/[3]"action"
    String[] requestParts = fullUri.getRawPath().split("/");
    if (requestParts.length != 4) {
      throw new VeniceException("Not a valid KEY_PARTITION_PROFILER request: " + request.uri());
    }
    String topicName = requestParts[2];
    if (!Version.isVersionTopic(topicName)) {
      throw new VeniceException("Invalid store version for KEY_PARTITION_PROFILER: " + request.uri());
    }
    Action action;
    try {
      action = Action.valueOf(requestParts[3].toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new VeniceException("Unknown KEY_PARTITION_PROFILER sub-action: " + requestParts[3], e);
    }

    Long durationMs = null;
    Integer topK = null;
    if (fullUri.getRawQuery() != null) {
      QueryStringDecoder query = new QueryStringDecoder(request.uri());
      Map<String, List<String>> params = query.parameters();
      if (params.containsKey("duration")) {
        try {
          durationMs = Long.parseLong(params.get("duration").get(0)) * 1_000L;
        } catch (NumberFormatException e) {
          throw new VeniceException("Invalid duration for KEY_PARTITION_PROFILER: " + request.uri(), e);
        }
      }
      if (params.containsKey("topK")) {
        try {
          topK = Integer.parseInt(params.get("topK").get(0));
        } catch (NumberFormatException e) {
          throw new VeniceException("Invalid topK for KEY_PARTITION_PROFILER: " + request.uri(), e);
        }
      }
    }

    return new KeyPartitionProfilerRequest(topicName, action, durationMs, topK);
  }

  public String getStoreName() {
    return storeName;
  }

  public String getStoreVersion() {
    return storeVersion;
  }

  public Action getAction() {
    return action;
  }

  /** Profiling window length in milliseconds (only meaningful for {@link Action#START}). */
  public Long getDurationMs() {
    return durationMs;
  }

  /** Top-K heap size per partition (only meaningful for {@link Action#START}). */
  public Integer getTopK() {
    return topK;
  }
}

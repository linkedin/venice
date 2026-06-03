package com.linkedin.venice.pubsub.api;

import static com.linkedin.venice.memory.ClassSizeEstimator.getClassOverhead;

import com.linkedin.venice.memory.Measurable;
import com.linkedin.venice.utils.collections.MeasurableLinkedHashMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;


/**
 * Set of key-value pairs to tagged with messages produced to a topic.
 * In case of headers with the same key, only the most recently added headers value will be kept.
 */
public class PubSubMessageHeaders implements Measurable, Iterable<PubSubMessageHeader> {
  public static final String EXECUTION_ID_KEY = "EXECUTION_ID";
  private static final int SHALLOW_CLASS_OVERHEAD = getClassOverhead(PubSubMessageHeaders.class);
  /**
   * N.B.: Kafka allows duplicate keys in the headers but some pubsub systems may not
   * allow it. Hence, we will enforce uniqueness of keys in headers from the beginning.
   */
  private final MeasurableLinkedHashMap<String, PubSubMessageHeader> headers = new MeasurableLinkedHashMap<>();

  public static final String VENICE_TRANSPORT_PROTOCOL_HEADER = "vtp";
  /** Header to denote whether the leader is completed or not */
  public static final String VENICE_LEADER_COMPLETION_STATE_HEADER = "lcs";
  /**
   * Header to provide the desired view partition mapping for the given message. Example usage:
   * 1. A VPJ containing large messages write chunks with the header {"view1":[0], "view2":[1, 2]}. Partition leaders
   * during NR pass-through in remote fabrics can forward chunks to their destination view partition(s) without any
   * further processing. In the example, this chunk should be sent to view1's partition 0 and view2's partitions 1 & 2.
   */
  public static final String VENICE_VIEW_PARTITIONS_MAP_HEADER = "vpm";
  /**
   * Header carrying per-partition record count on End-of-Push control messages.
   * Value is an 8-byte big-endian encoded long representing the number of records
   * written to that partition. Used for server-side batch push verification.
   */
  public static final String VENICE_PARTITION_RECORD_COUNT_HEADER = "prc";

  /**
   * Sentinel value stamped into {@link #VENICE_PARTITION_RECORD_COUNT_HEADER} when the producer
   * cannot supply a count (e.g. legacy code paths, partition-level fallback). Consumers must
   * treat a header carrying this value as "count unavailable" and skip the EOP record-count
   * verification rather than reading {@code -1} as a literal record count.
   */
  public static final long PRC_HEADER_UNAVAILABLE_SENTINEL = -1L;

  /**
   * VT data-record header: single byte carrying {@code +1} (key created), {@code -1} (key deleted),
   * or {@code 0} (invalidate). Absent = no change. Produced by A/A leader during DCR, consumed by
   * followers to maintain a per-partition active-key-count.
   */
  public static final String VENICE_KEY_COUNT_SIGNAL_HEADER = "kcs";

  /**
   * VT heartbeat header: 8-byte long carrying the leader's active-key-count at heartbeat-emission
   * time. Followers compare against their own count when processing the heartbeat — by VT
   * ordering, all {@link #VENICE_KEY_COUNT_SIGNAL_HEADER} signals on records prior to this heartbeat
   * have already been applied, so a difference indicates divergence across replicas. Absent = leader
   * not tracking or feature disabled; follower skips comparison.
   */
  public static final String VENICE_LEADER_KEY_COUNT_HEADER = "lkc";

  public PubSubMessageHeaders add(PubSubMessageHeader header) {
    headers.put(header.key(), header);
    return this;
  }

  public PubSubMessageHeaders add(String key, byte[] value) {
    add(new PubSubMessageHeader(key, value));
    return this;
  }

  public PubSubMessageHeaders remove(String key) {
    headers.remove(key);
    return this;
  }

  public PubSubMessageHeader get(String key) {
    return headers.get(key);
  }

  /**
   * @return the headers as a List<PubSubMessageHeader>.
   *    Mutating this list will not affect the PubSubMessageHeaders.
   *    If no headers are present an empty list is returned.
   */
  public List<PubSubMessageHeader> toList() {
    return headers.isEmpty() ? Collections.emptyList() : new ArrayList<>(headers.values());
  }

  public boolean isEmpty() {
    return headers.isEmpty();
  }

  /**
   * Returns a {@link PubSubMessageHeaders} without the {@link #VENICE_TRANSPORT_PROTOCOL_HEADER} (a.k.a.
   * {@code vtp}). The {@code vtp} value is the entire ~16 KB Avro JSON for {@link
   * com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope}; once the value envelope has been deserialized
   * it is dead weight and pinning it per queued record has been observed to cost upwards of 10 GB on the
   * DaVinci ingestion buffer queue during back-pressure.
   *
   * <p>Best-effort: when {@code vtp} is absent the input is returned as-is (no allocation). When present, the
   * helper attempts an in-place {@code remove} — allocation-free for the production hot path where callers
   * construct fresh mutable headers per record. If {@code remove()} throws any {@link RuntimeException}
   * (e.g. {@code UnsupportedOperationException} from an immutable variant), the helper falls back to
   * building a new headers object without {@code vtp}. Never throws — the strip is on the deserialization
   * hot path, where an escaping exception would kill the partition's ingestion thread.
   *
   * <p>Use {@link #stripProtocolSchemaHeaderCopy} instead when the caller's headers reference is shared
   * across reads (e.g. an in-memory broker that re-serves the same message), since silent in-place mutation
   * would corrupt subsequent observers.
   */
  public static PubSubMessageHeaders stripProtocolSchemaHeader(PubSubMessageHeaders headers) {
    if (headers == null || headers.get(VENICE_TRANSPORT_PROTOCOL_HEADER) == null) {
      return headers;
    }
    try {
      headers.remove(VENICE_TRANSPORT_PROTOCOL_HEADER);
      return headers;
    } catch (RuntimeException e) {
      return copyWithoutProtocolSchemaHeader(headers);
    }
  }

  /**
   * Always-copy variant of {@link #stripProtocolSchemaHeader}. Returns the input unchanged when {@code vtp}
   * is absent (no allocation); otherwise returns a fresh {@link PubSubMessageHeaders} omitting {@code vtp}
   * and leaves the input untouched. Use this from call sites where the headers reference is shared across
   * reads.
   */
  public static PubSubMessageHeaders stripProtocolSchemaHeaderCopy(PubSubMessageHeaders headers) {
    if (headers == null || headers.get(VENICE_TRANSPORT_PROTOCOL_HEADER) == null) {
      return headers;
    }
    return copyWithoutProtocolSchemaHeader(headers);
  }

  private static PubSubMessageHeaders copyWithoutProtocolSchemaHeader(PubSubMessageHeaders headers) {
    PubSubMessageHeaders filtered = new PubSubMessageHeaders();
    for (PubSubMessageHeader h: headers) {
      if (!VENICE_TRANSPORT_PROTOCOL_HEADER.equals(h.key())) {
        filtered.add(h);
      }
    }
    return filtered;
  }

  @Override
  public int getHeapSize() {
    return SHALLOW_CLASS_OVERHEAD + this.headers.getHeapSize();
  }

  @Override
  public Iterator<PubSubMessageHeader> iterator() {
    return headers.values().iterator();
  }
}

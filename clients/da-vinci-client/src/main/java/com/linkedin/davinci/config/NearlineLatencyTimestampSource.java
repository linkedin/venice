package com.linkedin.davinci.config;

import com.linkedin.venice.exceptions.VeniceException;


/**
 * Selects which timestamp the leader carries in
 * {@code LeaderMetadata.upstreamMessageTimestamp} when producing a record
 * to the version topic from a consumed upstream message.
 *
 * <p>Followers use this timestamp to compute true per-record end-to-end nearline
 * ingestion latency. Both options are millisecond-since-epoch values, but they
 * answer different operational questions:
 *
 * <ul>
 *   <li>{@link #BROKER} — the upstream pub-sub system's append timestamp,
 *       falling back to the upstream producer's wall clock when the broker
 *       timestamp is unavailable. Matches the SLI definition the leader uses
 *       today via {@code PubSubMessage.getPubSubMessageTime()} and is the
 *       infra-only latency view.</li>
 *   <li>{@link #PRODUCER} — the upstream producer's wall clock as embedded in
 *       {@code KafkaMessageEnvelope.producerMetadata.messageTimestamp}.
 *       Includes upstream-client enqueue-to-produce latency, giving the
 *       application-perceived end-to-end view.</li>
 * </ul>
 *
 * <p>Default is {@link #BROKER}.
 */
public enum NearlineLatencyTimestampSource {
  BROKER, PRODUCER;

  public static NearlineLatencyTimestampSource parse(String value) {
    if (value == null) {
      return BROKER;
    }
    String normalized = value.trim();
    for (NearlineLatencyTimestampSource s: values()) {
      if (s.name().equalsIgnoreCase(normalized)) {
        return s;
      }
    }
    throw new VeniceException(
        "Invalid value for nearline latency timestamp source: '" + value + "'. Expected one of: BROKER, PRODUCER.");
  }
}

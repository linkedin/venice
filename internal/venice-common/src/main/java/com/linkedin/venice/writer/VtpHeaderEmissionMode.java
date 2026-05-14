package com.linkedin.venice.writer;

/**
 * Controls whether {@link VeniceWriter} attaches the
 * {@link com.linkedin.venice.pubsub.api.PubSubMessageHeaders#VENICE_TRANSPORT_PROTOCOL_HEADER vtp}
 * protocol-schema header to outbound messages.
 *
 * <p>The vtp header carries the Avro JSON for {@code KafkaMessageEnvelope} (~16 KB) and is only
 * useful to consumers that need to bootstrap the schema for forward compatibility. Pre-existing
 * behavior emits it on the first message of every segment (segmentNumber=0 &amp; messageSequenceNumber=0).
 * Heartbeat control messages are constructed as {@code START_OF_SEGMENT} with the same coordinates,
 * so every heartbeat picks up the ~16 KB header even though heartbeat consumers never use it for
 * schema bootstrap. On busy ingestion paths with many partitions and frequent heartbeats this
 * dominates the per-record memory footprint.
 *
 * <p>This mode lets writers opt out of the heartbeat case (or out entirely) without changing the
 * semantics of regular data segment-start records.
 */
public enum VtpHeaderEmissionMode {
  /**
   * Emit the vtp header on every segment-start message, including heartbeat SOS records. Default,
   * preserves pre-existing behavior.
   */
  SOS_AND_HB,

  /**
   * Emit the vtp header on regular data segment-start records only; skip heartbeat SOS records.
   * Consumers are expected to obtain the {@code KafkaMessageEnvelope} schema by some other means
   * (controller-side schema cache, classpath fallback, or an earlier data SOS on the same segment).
   */
  SOS_ONLY,

  /**
   * Never emit the vtp header. Use only when all consumers can resolve the
   * {@code KafkaMessageEnvelope} schema without the per-segment hint.
   */
  NONE
}

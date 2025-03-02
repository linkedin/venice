package com.linkedin.venice.pubsub.api;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;


public interface DefaultPubSubMessage extends PubSubMessage<KafkaKey, KafkaMessageEnvelope, PubSubPosition> {
}

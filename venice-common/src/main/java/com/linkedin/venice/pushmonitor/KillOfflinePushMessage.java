package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.status.StatusMessage;
import java.util.Map;


public class KillOfflinePushMessage extends StatusMessage {
  private static final String KAFKA_TOPIC = "kafkaTopic";

  private final String kafkaTopic;

  public KillOfflinePushMessage(String kafkaTopic) {
    this.kafkaTopic = kafkaTopic;
  }

  public KillOfflinePushMessage(Map<String, String> fields) {
    super(fields);
    this.kafkaTopic = fields.get(KAFKA_TOPIC);
  }

  public String getKafkaTopic() {
    return kafkaTopic;
  }

  @Override
  public Map<String, String> getFields() {
    Map<String, String> map = super.getFields();
    map.put(KAFKA_TOPIC, kafkaTopic);
    return map;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    KillOfflinePushMessage that = (KillOfflinePushMessage) o;

    return kafkaTopic.equals(that.kafkaTopic);
  }

  @Override
  public int hashCode() {
    return kafkaTopic.hashCode();
  }

  @Override
  public String toString() {
    return "KillOfflinePushMessage{" + "kafkaTopic='" + kafkaTopic + '\'' + '}';
  }
}

package com.linkedin.venice.consumer;

import org.apache.avro.Schema;


public class ConsumerIntegrationTestWithProtocolHeader extends ConsumerIntegrationTest {
  @Override
  Schema getOverrideProtocolSchema() {
    return NEW_PROTOCOL_SCHEMA;
  }
}

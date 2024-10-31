package com.linkedin.venice.consumer;

import org.apache.avro.Schema;
import org.testng.annotations.Test;


@Test
public class ConsumerIntegrationTestWithProtocolHeader extends ConsumerIntegrationTest {
  @Override
  Schema getOverrideProtocolSchema() {
    return NEW_PROTOCOL_SCHEMA;
  }
}

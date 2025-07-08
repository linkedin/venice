package com.linkedin.venice.controller;

@Deprecated
public class TestTopicRequestOnHybridDeleteWithHMC extends TestTopicRequestOnHybridDelete {
  @Override
  protected boolean enableHelixMessagingChannel() {
    return true;
  }
}

package com.linkedin.venice.listener.response.stats;

public class ReadResponseLatencyInjector {
  public static void injectExtraLatency() {
    AbstractReadResponseStats.TEST_ONLY_INJECT_SLEEP_DURING_INCREMENT_TO_SIMULATE_RACE_CONDITION = true;
  }

  public static void removeExtraLatency() {
    AbstractReadResponseStats.TEST_ONLY_INJECT_SLEEP_DURING_INCREMENT_TO_SIMULATE_RACE_CONDITION = false;
  }

}

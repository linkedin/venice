package com.linkedin.venice.integration.utils;

import com.linkedin.venice.utils.TestMockTime;
import org.apache.kafka.common.utils.MockTime;


public class KafkaMockTimeWrapper extends MockTime {
  private final TestMockTime time;

  public KafkaMockTimeWrapper(TestMockTime time) {
    this.time = time;
  }

  @Override
  public long milliseconds() {
    return this.time.getMilliseconds();
  }

  @Override
  public long nanoseconds() {
    return this.time.getNanoseconds();
  }

  @Override
  public void sleep(long ms) {
    this.time.sleep(ms);
  }

  @Override
  public void setCurrentTimeMs(long newMs) {
    this.time.setTime(newMs);
  }

}

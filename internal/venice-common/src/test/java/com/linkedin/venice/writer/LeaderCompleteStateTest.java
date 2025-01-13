package com.linkedin.venice.writer;

import com.linkedin.venice.utils.CollectionUtils;
import com.linkedin.venice.utils.VeniceEnumValueTest;
import java.util.Map;


public class LeaderCompleteStateTest extends VeniceEnumValueTest<LeaderCompleteState> {
  public LeaderCompleteStateTest() {
    super(LeaderCompleteState.class);
  }

  @Override
  protected Map<Integer, LeaderCompleteState> expectedMapping() {
    return CollectionUtils.<Integer, LeaderCompleteState>mapBuilder()
        .put(0, LeaderCompleteState.LEADER_NOT_COMPLETED)
        .put(1, LeaderCompleteState.LEADER_COMPLETED)
        .build();
  }
}

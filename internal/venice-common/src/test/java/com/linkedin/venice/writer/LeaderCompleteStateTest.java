package com.linkedin.venice.writer;

import com.linkedin.alpini.base.misc.CollectionUtil;
import com.linkedin.venice.utils.VeniceEnumValueTest;
import java.util.Map;


public class LeaderCompleteStateTest extends VeniceEnumValueTest<LeaderCompleteState> {
  public LeaderCompleteStateTest() {
    super(LeaderCompleteState.class);
  }

  @Override
  protected Map<Integer, LeaderCompleteState> expectedMapping() {
    return CollectionUtil.<Integer, LeaderCompleteState>mapBuilder()
        .put(0, LeaderCompleteState.LEADER_NOT_COMPLETED)
        .put(1, LeaderCompleteState.LEADER_COMPLETED)
        .build();
  }
}

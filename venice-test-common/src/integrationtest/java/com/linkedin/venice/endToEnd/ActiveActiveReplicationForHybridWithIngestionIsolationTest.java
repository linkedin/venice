package com.linkedin.venice.endToEnd;

import com.linkedin.venice.utils.TestUtils;
import java.util.Map;


public class ActiveActiveReplicationForHybridWithIngestionIsolationTest extends
                                                                        com.linkedin.venice.endToEnd.ActiveActiveReplicationForHybridTest {

  @Override
  public Map<String, Object> getExtraServerProperties() {
    return TestUtils.getIngestionIsolationPropertyMap();
  }
}

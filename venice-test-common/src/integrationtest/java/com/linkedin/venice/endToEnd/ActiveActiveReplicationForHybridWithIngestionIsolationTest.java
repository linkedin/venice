package com.linkedin.venice.endToEnd;

import com.linkedin.venice.meta.IngestionMode;
import java.util.Collections;
import java.util.Map;

import static com.linkedin.venice.ConfigKeys.*;


public class ActiveActiveReplicationForHybridWithIngestionIsolationTest extends
                                                                        com.linkedin.venice.endToEnd.ActiveActiveReplicationForHybridTest {

  @Override
  public Map<String, String> getExtraServerProperties() {
    return Collections.singletonMap(SERVER_INGESTION_MODE, IngestionMode.ISOLATED.toString());
  }
}

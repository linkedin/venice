package com.linkedin.venice.jobs;

import static com.linkedin.venice.jobs.DataWriterComputeJob.PASS_THROUGH_CONFIG_PREFIXES;

import com.linkedin.venice.exceptions.VeniceException;
import org.testng.annotations.Test;


public class DataWriterComputeJobTest {
  @Test
  public void testNoOverlappingPassThroughConfigPrefixes() {
    int passThroughPrefixListSize = PASS_THROUGH_CONFIG_PREFIXES.size();
    /**
     * The following logic will make sure there is no prefix that is a prefix of another prefix.
     */
    for (int i = 0; i < passThroughPrefixListSize; ++i) {
      for (int j = i + 1; j < passThroughPrefixListSize; ++j) {
        String prefixI = PASS_THROUGH_CONFIG_PREFIXES.get(i);
        String prefixJ = PASS_THROUGH_CONFIG_PREFIXES.get(j);
        if (prefixI.startsWith(prefixJ)) {
          throw new VeniceException("Prefix: " + prefixJ + " shouldn't be a prefix of another prefix: " + prefixI);
        }

        if (prefixJ.startsWith(prefixI)) {
          throw new VeniceException("Prefix: " + prefixI + " shouldn't be a prefix of another prefix: " + prefixJ);
        }
      }
    }
  }
}

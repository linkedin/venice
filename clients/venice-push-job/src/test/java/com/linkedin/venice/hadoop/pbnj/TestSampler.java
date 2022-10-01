package com.linkedin.venice.hadoop.pbnj;

import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TestSampler {
  private static final Logger LOGGER = LogManager.getLogger(TestSampler.class);

  @DataProvider(name = "samplingRatios")
  public static Object[][] samplingRatios() {
    List<Object[]> returnList = new ArrayList<>();

    final long deterministicTotalRecords = 1000000;

    // Some decent values which should definitely work.
    returnList.add(new Object[] { deterministicTotalRecords, 0.001 });
    returnList.add(new Object[] { deterministicTotalRecords, 0.002 });
    returnList.add(new Object[] { deterministicTotalRecords, 0.005 });
    returnList.add(new Object[] { deterministicTotalRecords, 0.01 });
    returnList.add(new Object[] { deterministicTotalRecords, 0.02 });
    returnList.add(new Object[] { deterministicTotalRecords, 0.05 });
    returnList.add(new Object[] { deterministicTotalRecords, 0.1 });
    returnList.add(new Object[] { deterministicTotalRecords, 0.2 });
    returnList.add(new Object[] { deterministicTotalRecords, 0.5 });
    returnList.add(new Object[] { deterministicTotalRecords, 0.9 });
    returnList.add(new Object[] { deterministicTotalRecords, 1.0 });

    // A few random record counts and ratios as well, just for the heck of it.
    while (returnList.size() < 20) {
      double randomRatio = Utils.round(Math.random(), 4);
      if (randomRatio > 0 && randomRatio < 1 && returnList.stream().noneMatch(e -> e[1].equals(randomRatio))) {
        long nonDeterministicTotalRecords = ThreadLocalRandom.current().nextInt(1_000_000, 2_000_000);
        returnList.add(new Object[] { nonDeterministicTotalRecords, randomRatio });
      }
    }

    Object[][] valuesToReturn = new Object[returnList.size()][2];
    return returnList.toArray(valuesToReturn);
  }

  @Test(dataProvider = "samplingRatios")
  public void testSampler(long totalRecords, double expectedRatio) {
    LOGGER.info("totalRecords: {], expectedRatio: {}", totalRecords, expectedRatio);
    long skippedRecords = 0, queriedRecords = 0;

    Sampler sampler = new Sampler(expectedRatio);

    Assert.assertFalse(
        sampler.checkWhetherToSkip(queriedRecords, skippedRecords),
        "The first call to checkWhetherToSkip should always allow the query.");
    queriedRecords++;
    for (int i = 1; i < totalRecords; i++) {
      long totalRecordsSoFar = queriedRecords + skippedRecords;
      Assert.assertEquals(
          totalRecordsSoFar,
          i,
          "A sampler should never increment the queriedRecords counter on its own.");

      boolean skip = sampler.checkWhetherToSkip(queriedRecords, skippedRecords);
      // LOGGER.info("Record " + i + " should skip: " + skip);
      if (skip) {
        skippedRecords++;
      } else {
        queriedRecords++;
      }
    }

    double actualRatio = 1.0 - ((double) skippedRecords) / ((double) totalRecords);
    double roundedActualRatio = Utils.round(actualRatio, 5);

    Assert.assertEquals(roundedActualRatio, expectedRatio, "The actual ratio does not match the expected ratio.");
  }
}

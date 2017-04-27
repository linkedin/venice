package com.linkedin.venice.store.bdb;

import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.cleaner.FileSummary;
import com.sleepycat.je.cleaner.UtilizationProfile;
import com.sleepycat.je.dbi.EnvironmentImpl;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;


public class BdbSpaceUtilizationSummary {
  private final EnvironmentImpl envImpl;

  private SortedMap<Long, FileSummary> summaryMap;
  private long totalSpaceUsed = 0;
  private long totalSpaceUtilized = 0;

  public BdbSpaceUtilizationSummary(Environment env) {
    this(DbInternal.getEnvironmentImpl(env));
  }

  private BdbSpaceUtilizationSummary(EnvironmentImpl envImpl) {
    this.envImpl = envImpl;
    UtilizationProfile profile = this.envImpl.getUtilizationProfile();
    summaryMap = profile.getFileSummaryMap(true);

    Iterator<Map.Entry<Long, FileSummary>> fileItr = summaryMap.entrySet().iterator();
    while(fileItr.hasNext()) {
      Map.Entry<Long, FileSummary> entry = fileItr.next();
      FileSummary fs = entry.getValue();
      totalSpaceUsed += fs.totalSize;
      totalSpaceUtilized += fs.totalSize - fs.getObsoleteSize();
    }
  }

  public long getTotalSpaceUsed() {
    return totalSpaceUsed;
  }

  public long getTotalSpaceUtilized() {
    return totalSpaceUtilized;
  }
}

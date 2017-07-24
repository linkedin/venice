package com.linkedin.venice.stats;

import io.tehuti.metrics.MetricsRepository;
import java.util.function.Supplier;


public abstract class AbstractVersionedReporter<T extends AbstractVeniceStats> extends AbstractVeniceStats{
  private int currentVersion = 0;
  private int futureVersion = 0;
  private int backupVersion = 0;

  protected T currentReporter;
  protected T futureReporter;
  protected T backupReporter;

  public AbstractVersionedReporter(MetricsRepository metricsRepository, String storeName, StatsSupplier<T> statsSupplier) {
    super(metricsRepository, storeName);

    registerSensor("current_version",
        new VersionStat(() -> (double) currentVersion));
    registerSensor("future_version",
        new VersionStat(() -> (double) futureVersion));
    registerSensor("backup_version",
        new VersionStat(() -> (double) backupVersion));

    currentReporter = statsSupplier.get(metricsRepository, storeName + "_current");
    futureReporter = statsSupplier.get(metricsRepository, storeName + "_future");
    backupReporter = statsSupplier.get(metricsRepository, storeName + "_backup");
  }

  public int getCurrentVersion() {
    return currentVersion;
  }

  public int getFutureVersion() {
    return futureVersion;
  }

  public int getBackupVersion() {
    return backupVersion;
  }

  protected void setCurrentVersion(int currentVersion) {
    this.currentVersion = currentVersion;
  }

  protected void setFutureVersion(int futureVersion) {
    this.futureVersion = futureVersion;
  }

  protected void setBackupVersion(int backupVersion) {
    this.backupVersion = backupVersion;
  }

  public T getCurrentReporter() {
    return currentReporter;
  }

  public T getFutureReporter() {
    return futureReporter;
  }

  public T getBackupReporter() {
    return backupReporter;
  }

  private static class VersionStat extends LambdaStat {
    public VersionStat(Supplier<Double> supplier) {
      super(() -> supplier.get());
    }
  }
}
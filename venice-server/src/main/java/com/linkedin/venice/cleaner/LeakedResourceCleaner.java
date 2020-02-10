package com.linkedin.venice.cleaner;

import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.server.StorageEngineRepository;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.bdb.BdbStorageEngine;
import com.linkedin.venice.utils.LatencyUtils;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Environment;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;

import static com.linkedin.venice.store.bdb.BdbStoragePartition.*;


/**
 * LeakedResourceCleaner is a background thread which wakes up regularly
 * to check whether some BDB databases are removed recently; if so, do a checkpoint
 * to release disk space.
 */
public class LeakedResourceCleaner extends AbstractVeniceService {
  private static final Logger logger = Logger.getLogger(LeakedResourceCleaner.class);

  private long pollIntervalMs;
  private LeakedResourceCleanerRunnable cleaner;
  private Thread runner;
  private StorageEngineRepository storageEngineRepository;

  private long checkpointDelayInMinutes = 60;

  public LeakedResourceCleaner(StorageEngineRepository storageEngineRepository, long pollIntervalMs) {
    this.storageEngineRepository = storageEngineRepository;
    this.pollIntervalMs = pollIntervalMs;
  }

  public void setCheckpointDelayInMinutes(long checkpointDelayInMinutes) {
    this.checkpointDelayInMinutes = checkpointDelayInMinutes;
  }

  @Override
  public boolean startInner() {
    cleaner = new LeakedResourceCleanerRunnable(storageEngineRepository);
    runner = new Thread(cleaner);
    runner.setName("Storage Leaked Resource cleaner");
    runner.setDaemon(true);
    runner.start();

    return true;
  }

  @Override
  public void stopInner() {
    cleaner.setStop();
  }

  private class LeakedResourceCleanerRunnable implements Runnable {
    private StorageEngineRepository storageEngineRepository;
    private volatile boolean stop = false;
    public LeakedResourceCleanerRunnable(StorageEngineRepository storageEngineRepository) {
      this.storageEngineRepository = storageEngineRepository;
    }

    protected void setStop(){
      stop = true;
    }


    @Override
    public void run() {
      while (!stop) {
        try {
          Thread.sleep(pollIntervalMs);

          List<AbstractStorageEngine> storageEngines = storageEngineRepository.getAllLocalStorageEngines();
          for (AbstractStorageEngine storageEngine : storageEngines) {
            // only do checkpoint for BDB stores
            if (storageEngine.getType() == PersistenceType.BDB) {
              Environment bdbEnvironment = ((BdbStorageEngine)storageEngine).getBdbEnvironment();
              List<String> databaseNames = bdbEnvironment.getDatabaseNames();
              Set<String> deletedDatabaseFlags = new HashSet<>();
              for (String databaseName : databaseNames) {
                if (databaseName.endsWith(DELETE_FLAG_SUFFIX)) {
                  deletedDatabaseFlags.add(databaseName);
                }
              }
              databaseNames.clear();

              /**
               * Check whether all databases are deleted recently; if so, skip the checkpoint.
               *
               * The reason for this check is to avoid unnecessary checkpoints due to version deletion;
               * version deletion will remove the entire directory of the store at the end, so it's not
               * necessary to do checkpoint within the directory during version deletion.
               *
               */
              long currentTime = System.nanoTime();
              boolean hasOldDeletion = false;
              for (String databaseName : deletedDatabaseFlags) {
                String nameAndTimestamp = databaseName.substring(0, databaseName.lastIndexOf(DELETE_FLAG_SUFFIX));
                long timestamp = Long.valueOf(nameAndTimestamp.substring(nameAndTimestamp.lastIndexOf(DELETE_TIMESTAMP_SEPARATOR) + 1));
                long latencyInMinute = TimeUnit.NANOSECONDS.toMinutes(currentTime - timestamp);
                if (latencyInMinute > checkpointDelayInMinutes) {
                  hasOldDeletion = true;
                  break;
                }
              }

              if (hasOldDeletion) {
                logger.info("Start BDB checkpoint to release disk space. Databases deleted in the last "
                    + TimeUnit.MILLISECONDS.toMinutes(pollIntervalMs) + " minutes: "
                    + deletedDatabaseFlags.stream().map(databaseName -> {
                      String nameAndTimestamp = databaseName.substring(0, databaseName.lastIndexOf(DELETE_FLAG_SUFFIX));
                      return nameAndTimestamp.substring(0, nameAndTimestamp.lastIndexOf(DELETE_TIMESTAMP_SEPARATOR));
                }).collect(Collectors.toList()));
                long checkpointStartTimeInNS = System.nanoTime();
                bdbEnvironment.cleanLog();
                CheckpointConfig ckptConfig = new CheckpointConfig();
                ckptConfig.setMinimizeRecoveryTime(true);
                ckptConfig.setForce(true);
                bdbEnvironment.checkpoint(ckptConfig);
                double latency = LatencyUtils.getLatencyInMS(checkpointStartTimeInNS);
                logger.info("One BDB checkpoint completes which took " + latency + "ms");

                // remove the deleted database flag
                for (String databaseName : deletedDatabaseFlags) {
                  bdbEnvironment.removeDatabase(null, databaseName);
                }
              }
            }
          }
        } catch (Exception e) {
          logger.error(e.getMessage());
        }
      }
    }
  }
}

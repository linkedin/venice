package com.linkedin.davinci.store.rocksdb;

import static com.linkedin.venice.store.rocksdb.RocksDBUtils.extractTempSSTFileNo;
import static com.linkedin.venice.store.rocksdb.RocksDBUtils.isTempSSTFile;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.venice.exceptions.VeniceChecksumException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.validation.checksum.CheckSum;
import com.linkedin.venice.kafka.validation.checksum.CheckSumType;
import com.linkedin.venice.store.rocksdb.RocksDBUtils;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.LatencyUtils;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.EnvOptions;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.LiveFileMetaData;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileReader;
import org.rocksdb.SstFileReaderIterator;
import org.rocksdb.SstFileWriter;


public class RocksDBSstFileWriter {
  private static final Logger LOGGER = LogManager.getLogger(RocksDBSstFileWriter.class);

  /**
   * This class will be used in {@link #put(byte[], ByteBuffer)} to improve GC.
   * Since the only update is from {@literal com.linkedin.venice.kafka.consumer.StoreBufferService.StoreBufferDrainer}, the
   * total amount of memory pre-allocated is limited.
   */
  private static class ReusableObjects {
    public ByteBuffer directKeyBuffer;
    public ByteBuffer directValueBuffer;

    public ReusableObjects() {
      directKeyBuffer = ByteBuffer.allocateDirect(1024 * 1024);
      directValueBuffer = ByteBuffer.allocateDirect(1024 * 1024 + 128); // Adding another 128 bytes considering
                                                                        // potential overhead of metadata
    }
  }

  private static final ThreadLocal<ReusableObjects> threadLocalReusableObjects =
      ThreadLocal.withInitial(() -> new ReusableObjects());
  /**
   * This field is being stored during offset checkpointing in {@link com.linkedin.davinci.kafka.consumer.StoreIngestionTask}.
   * With the field, RocksDB could recover properly during restart.
   *
   * Essentially, during recovery, this class will remove all the un-committed files after {@link #ROCKSDB_LAST_FINISHED_SST_FILE_NO},
   * and start a new file with no: {@link #ROCKSDB_LAST_FINISHED_SST_FILE_NO} + 1.
   * With this way, we could achieve exact-once ingestion, which is required by {@link SstFileWriter}.
   */
  protected static final String ROCKSDB_LAST_FINISHED_SST_FILE_NO = "rocksdb_last_finished_sst_file_no";
  protected static final String ROCKSDB_LAST_FINISHED_RMD_SST_FILE_NO = "rocksdb_last_finished_rmd_sst_file_no";
  protected static final int DEFAULT_COLUMN_FAMILY_INDEX = 0;
  protected static final int REPLICATION_METADATA_COLUMN_FAMILY_INDEX = 1;
  private static final int REMOVE_ALL_SST_FILES = -1;
  private int lastFinishedSSTFileNo = -1;
  /**
   * Whether the input is sorted or not.
   */
  private int currentSSTFileNo = 0;
  private SstFileWriter currentSSTFileWriter;
  private long recordNumInCurrentSSTFile = 0;
  private long recordNumInAllSSTFiles = 0;
  private String fullPathForTempSSTFileDir;
  private Optional<Supplier<byte[]>> expectedChecksumSupplier;
  private final String storeName;
  private final int partitionId;
  private final EnvOptions envOptions;
  private final Options options;
  private final boolean isRMD;
  private final RocksDBServerConfig rocksDBServerConfig;

  @VisibleForTesting
  protected Checkpoint createCheckpoint(RocksDB rocksDB) {
    return Checkpoint.create(rocksDB);
  }

  @VisibleForTesting
  public String getLastCheckPointedSSTFileNum() {
    return lastCheckPointedSSTFileNum;
  }

  private final String lastCheckPointedSSTFileNum;

  public RocksDBSstFileWriter(
      String storeName,
      int partitionId,
      String dbDir,
      EnvOptions envOptions,
      Options options,
      String fullPathForTempSSTFileDir,
      boolean isRMD,
      RocksDBServerConfig rocksDBServerConfig) {
    this.storeName = storeName;
    this.partitionId = partitionId;
    this.envOptions = envOptions;
    this.options = options;
    this.fullPathForTempSSTFileDir = fullPathForTempSSTFileDir;
    this.isRMD = isRMD;
    this.lastCheckPointedSSTFileNum = isRMD ? ROCKSDB_LAST_FINISHED_RMD_SST_FILE_NO : ROCKSDB_LAST_FINISHED_SST_FILE_NO;
    this.rocksDBServerConfig = rocksDBServerConfig;
  }

  public void put(byte[] key, ByteBuffer valueBuffer) throws RocksDBException {
    if (currentSSTFileWriter == null) {
      throw new VeniceException(
          "currentSSTFileWriter is null for store: " + storeName + ", partition id: " + partitionId
              + ", 'beginBatchWrite' should be invoked before any write");
    }
    if (rocksDBServerConfig.isPutReuseByteBufferEnabled()) {
      ReusableObjects reusableObjects = threadLocalReusableObjects.get();
      reusableObjects.directKeyBuffer.clear();
      if (key.length > reusableObjects.directKeyBuffer.capacity()) {
        reusableObjects.directKeyBuffer = ByteBuffer.allocateDirect(key.length);
      }
      reusableObjects.directKeyBuffer.put(key);
      reusableObjects.directKeyBuffer.flip();
      reusableObjects.directValueBuffer.clear();
      if (valueBuffer.remaining() > reusableObjects.directValueBuffer.capacity()) {
        reusableObjects.directValueBuffer = ByteBuffer.allocateDirect(valueBuffer.remaining());
      }
      valueBuffer.mark();
      reusableObjects.directValueBuffer.put(valueBuffer);
      valueBuffer.reset();
      reusableObjects.directValueBuffer.flip();
      currentSSTFileWriter.put(reusableObjects.directKeyBuffer, reusableObjects.directValueBuffer);
    } else {
      currentSSTFileWriter.put(key, ByteUtils.extractByteArray(valueBuffer));
    }
    ++recordNumInCurrentSSTFile;
    ++recordNumInAllSSTFiles;
  }

  /**
   * This functions checks whether there is any discrepancy between the checkpoint vs the current state.
   * If the number of SST files and the checkpoint do not match:
   * 1. delete all the temporary SST files to be able to start ingestion from beginning
   * 2. the files that are already ingested to the DB will be removed during re-ingestion
   *    in {@link #deleteOldIngestion}
   * 3. return false for the upstream to reset its state and restart ingestion.
   */
  boolean checkDatabaseIntegrity(Map<String, String> checkpointedInfo) {
    // Create temp SST file dir if it doesn't exist
    File tempSSTFileDir = new File(fullPathForTempSSTFileDir);
    if (!tempSSTFileDir.exists()) {
      tempSSTFileDir.mkdirs();
    }

    if (!checkpointedInfo.containsKey(lastCheckPointedSSTFileNum)) {
      LOGGER.info(
          "No checkpointed info for store: {}, partition id: {} so RocksDB will start building sst file from beginning",
          storeName,
          partitionId);
      lastFinishedSSTFileNo = -1;
      currentSSTFileNo = 0;
      // Blindly remove all the temp sst files if found any. Will be recreated.
      removeAllSSTFiles();
    } else {
      lastFinishedSSTFileNo = Integer.parseInt(checkpointedInfo.get(lastCheckPointedSSTFileNum));
      LOGGER.info(
          "Received last finished sst file no: {} for store: {}, partition id: {}",
          lastFinishedSSTFileNo,
          storeName,
          partitionId);

      // This is not the first time this process is ingesting this partition,
      // check the integrity before proceeding
      if (lastFinishedSSTFileNo < 0) {
        throw new VeniceException("Last finished sst file no: " + lastFinishedSSTFileNo + " shouldn't be negative");
      }
      if (doesAllPreviousSSTFilesBeforeCheckpointingExist()) {
        // remove the unwanted sst files, as flow will continue from the checkpointed info
        removeSSTFilesAfterCheckpointing(this.lastFinishedSSTFileNo);
        currentSSTFileNo = lastFinishedSSTFileNo + 1;
        LOGGER.info(
            "Ingestion will continue from the last checkpoint for store: {} partition: {}",
            storeName,
            partitionId);
      } else {
        // remove all the temp sst files if found any as ingestion will be restarted from beginning
        removeAllSSTFiles();
        LOGGER.info("Ingestion will restart from the beginning for store: {} partition: {}", storeName, partitionId);
        return false;
      }
    }
    return true;
  }

  public void open(Map<String, String> checkpointedInfo, Optional<Supplier<byte[]>> expectedChecksumSupplier) {
    LOGGER.info(
        "'beginBatchWrite' got invoked for RocksDB store: {}, partition: {} with checkpointed info: {} ",
        storeName,
        partitionId,
        checkpointedInfo);
    if (!checkDatabaseIntegrity(checkpointedInfo)) {
      // defensive check: this issue should have been dealt with while subscribing to the partition
      throw new VeniceException(
          "Checkpointed info and SST files in " + fullPathForTempSSTFileDir
              + " directory doesn't match for RocksDB store: " + storeName + " partition: " + partitionId);
    }
    String fullPathForCurrentSSTFile = composeFullPathForSSTFile(currentSSTFileNo);
    currentSSTFileWriter = new SstFileWriter(envOptions, options);
    try {
      currentSSTFileWriter.open(fullPathForCurrentSSTFile);
      recordNumInCurrentSSTFile = 0;
    } catch (RocksDBException e) {
      throw new VeniceException("Failed to open file: " + fullPathForCurrentSSTFile + " with SstFileWriter");
    }
    this.expectedChecksumSupplier = expectedChecksumSupplier;
  }

  public void close() {
    if (currentSSTFileWriter != null) {
      currentSSTFileWriter.close();
    }
  }

  /**
   * Closes currentSSTFileWriter, update lastCheckPointedSSTFileNum with the current SST file number,
   * validates checksum on this SST file and return updated checkpointingInfo with this lastCheckPointedSSTFileNum.
   */
  public Map<String, String> sync() {
    try {
      /**
       * {@link SstFileWriter#finish()} will throw exception if the current SST file is empty.
       */
      if (recordNumInCurrentSSTFile > 0) {
        currentSSTFileWriter.finish();
        lastFinishedSSTFileNo = currentSSTFileNo;
        ++currentSSTFileNo;
        String fullPathForLastFinishedSSTFile = composeFullPathForSSTFile(lastFinishedSSTFileNo);
        String fullPathForCurrentSSTFile = composeFullPathForSSTFile(currentSSTFileNo);
        currentSSTFileWriter.open(fullPathForCurrentSSTFile);

        LOGGER.info(
            "Sync gets invoked for store: {}, partition id: {}, last finished sst file: {} current sst file: {}",
            storeName,
            partitionId,
            fullPathForLastFinishedSSTFile,
            fullPathForCurrentSSTFile);
        long recordNumInLastSSTFile = recordNumInCurrentSSTFile;
        recordNumInCurrentSSTFile = 0;

        if (!isRMD && expectedChecksumSupplier.isPresent()) {
          byte[] checksumToMatch = expectedChecksumSupplier.get().get();
          long startMs = System.currentTimeMillis();
          if (!verifyChecksum(fullPathForLastFinishedSSTFile, recordNumInLastSSTFile, checksumToMatch)) {
            throw new VeniceChecksumException(
                "verifyChecksum: failure. last sstFile checksum didn't match for store: " + storeName + ", partition: "
                    + partitionId + ", sstFile: " + fullPathForLastFinishedSSTFile + ", records: "
                    + recordNumInLastSSTFile + ", latency(ms): " + LatencyUtils.getElapsedTimeFromMsToMs(startMs));
          }
        }
      } else if (!isRMD) {
        LOGGER.warn(
            "Sync gets invoked for store: {}, partition id: {}, but the last sst file: {} is empty",
            storeName,
            partitionId,
            composeFullPathForSSTFile(currentSSTFileNo));
      }
    } catch (RocksDBException e) {
      throw new VeniceException("Failed to sync SstFileWriter", e);
    }
    /**
     * Return the recovery related info to upper layer to checkpoint.
     */
    Map<String, String> checkpointingInfo = new HashMap<>();
    if (lastFinishedSSTFileNo >= 0) {
      checkpointingInfo.put(lastCheckPointedSSTFileNum, Integer.toString(lastFinishedSSTFileNo));
    }
    return checkpointingInfo;
  }

  private void removeSSTFilesAfterCheckpointing(int lastFinishedSSTFileNo) {
    File tempSSTFileDir = new File(fullPathForTempSSTFileDir);
    String[] sstFiles = tempSSTFileDir.list((File dir, String name) -> RocksDBUtils.isTempSSTFile(name));
    if (sstFiles == null) {
      throw new VeniceException("Failed to list sst files in " + tempSSTFileDir.getAbsolutePath());
    }

    for (String sstFile: sstFiles) {
      int sstFileNo = extractTempSSTFileNo(sstFile);
      if (sstFileNo > lastFinishedSSTFileNo) {
        String fullPathForSSTFile = fullPathForTempSSTFileDir + File.separator + sstFile;
        boolean ret = new File(fullPathForSSTFile).delete();
        if (!ret) {
          throw new VeniceException("Failed to delete file: " + fullPathForSSTFile);
        }
      }
    }
  }

  private void removeAllSSTFiles() {
    removeSSTFilesAfterCheckpointing(REMOVE_ALL_SST_FILES);
  }

  private boolean doesAllPreviousSSTFilesBeforeCheckpointingExist() {
    if (lastFinishedSSTFileNo < 0) {
      LOGGER.info("Since last finished sst file no is negative, there is nothing to verify");
      return true;
    }
    int currFileNo = 0;
    for (; currFileNo <= lastFinishedSSTFileNo; ++currFileNo) {
      String sstFilePath = composeFullPathForSSTFile(currFileNo);
      File sstFile = new File(sstFilePath);
      if (!sstFile.exists()) {
        break;
      }
    }

    if (currFileNo > lastFinishedSSTFileNo) {
      LOGGER.info(
          "Number of {} files matches with the checkpoint for store: {} partition: {}",
          isRMD ? "RMD SST" : "SST",
          storeName,
          partitionId);
      return true;
    }

    /**
     * The number of SST files found and checkpointed does not match:
     * This implies that the ingestion began but the process crashed before
     * OffsetRecord with EOP as true could be synced.
     *
     * This could indicate one of these scenarios:
     * 1. Crash after ingestion completion: Number of files is 0 in this case
     * 2. Crash during the ingestion: The current state of the system in this
     *    case is undefined. Some files might have been moved to the DB while
     *    some might not have, and some might remain on both the locations
     *    depending on how RocksDB recovers after a crash. But we are taking
     *    a safer approach and starting the ingestion from beginning in this case.
     */
    LOGGER.info(
        "Number of {} files don't match with the checkpoint for store: {} partition: {}",
        isRMD ? "RMD SST" : "SST",
        storeName,
        partitionId);
    return false;
  }

  private String composeFullPathForSSTFile(int sstFileNo) {
    return fullPathForTempSSTFileDir + File.separator + RocksDBUtils.composeTempSSTFileName(sstFileNo);
  }

  /**
   * This function calculates checksum of all the key/value pair stored in the input sstFilePath. It then
   * verifies if the checksum matches with the input checksumToMatch and return the result.
   * A SstFileReader handle is used to perform bulk scan through the entire SST file. fillCache option is
   * explicitly disabled to not pollute the rocksdb internal block caches. And also implicit checksum verification
   * is disabled to reduce latency of the entire operation.
   *
   * @param sstFilePath the full absolute path of the SST file
   * @param expectedRecordNumInSSTFile expected number of key/value pairs in the SST File
   * @param checksumToMatch pre-calculated checksum to match against.
   * @return true if the sstFile checksum matches with the provided checksum.
   */
  private boolean verifyChecksum(String sstFilePath, long expectedRecordNumInSSTFile, byte[] checksumToMatch) {
    SstFileReader sstFileReader = null;
    SstFileReaderIterator sstFileReaderIterator = null;

    try {
      sstFileReader = new SstFileReader(options);
      sstFileReader.open(sstFilePath);
      final ReadOptions readOptions = new ReadOptions();
      readOptions.setVerifyChecksums(false);
      readOptions.setFillCache(false);

      long actualRecordCounts = sstFileReader.getTableProperties().getNumEntries();
      if (actualRecordCounts != expectedRecordNumInSSTFile) {
        LOGGER.error(
            "verifyChecksum: failure. SSTFile record count does not match expected: {} actual: {}",
            expectedRecordNumInSSTFile,
            actualRecordCounts);
        return false;
      }

      long recordCount = 0;
      CheckSum sstFileFinalCheckSum = CheckSum.getInstance(CheckSumType.MD5);
      sstFileReaderIterator = sstFileReader.newIterator(readOptions);
      sstFileReaderIterator.seekToFirst();
      while (sstFileReaderIterator.isValid()) {
        sstFileFinalCheckSum.update(sstFileReaderIterator.key());
        sstFileFinalCheckSum.update(sstFileReaderIterator.value());
        sstFileReaderIterator.next();
        recordCount++;
      }
      final byte[] finalChecksum = sstFileFinalCheckSum.getCheckSum();
      boolean result = Arrays.equals(finalChecksum, checksumToMatch);
      if (!result) {
        LOGGER.error(
            "Checksum mismatch in SSTFile. recordCount: {} expectedChecksum: {}, actualChecksum: {}",
            recordCount,
            ByteUtils.toHexString(checksumToMatch),
            ByteUtils.toHexString(finalChecksum));
      }
      return result;
    } catch (Exception e) {
      throw new VeniceChecksumException("Checksum mismatch in SST files.", e);
    } finally {
      /**
       * close the iterator first before closing the reader, otherwise iterator is not closed at all, based on implementation
       * here {@link AbstractRocksIterator#disposeInternal()}
       */
      if (sstFileReaderIterator != null) {
        sstFileReaderIterator.close();
      }
      if (sstFileReader != null) {
        sstFileReader.close();
      }
    }
  }

  public boolean validateBatchIngestion() {
    List<String> files = getTemporarySSTFilePaths();
    if (files.isEmpty()) {
      return true;
    }
    for (String path: files) {
      File file = new File(path);
      if (file.length() != 0) {
        LOGGER.error("Non-empty sst found when validating batch ingestion: {}", path);
        return false;
      }
    }
    return true;
  }

  /**
   * If there are files already ingested in DB and we get here, then it means that the ingestion started but faced issues
   * before completion or the process crashed before the status of EOP was synced to OffsetRecord. In both these cases,
   * let's delete these files from the database and start a fresh ingestion as the new files will hold the complete data anyway.
   */
  private void deleteOldIngestion(RocksDB rocksDB, ColumnFamilyHandle columnFamilyHandle) throws RocksDBException {
    List<LiveFileMetaData> oldIngestedSSTFiles = rocksDB.getLiveFilesMetaData();
    if (oldIngestedSSTFiles.size() != 0) {
      int count = 0;
      for (LiveFileMetaData file: oldIngestedSSTFiles) {
        if (Arrays.equals(file.columnFamilyName(), columnFamilyHandle.getName())) {
          count++;
          rocksDB.deleteFile(file.fileName());
        }
      }
      LOGGER.info(
          "Deleting {} ingested {} file in rocksDB for store: {}",
          count,
          columnFamilyHandle.getID() == DEFAULT_COLUMN_FAMILY_INDEX ? "SST" : "RMD SST",
          storeName);
    }
  }

  public void ingestSSTFiles(RocksDB rocksDB, List<ColumnFamilyHandle> columnFamilyHandleList) {
    List<String> sstFilePaths = getTemporarySSTFilePaths();
    if (sstFilePaths.isEmpty()) {
      LOGGER.info(
          "No valid sst file found, so will skip the sst file ingestion for store: {}, partition: {}",
          storeName,
          partitionId);
      return;
    }
    LOGGER.info(
        "Start ingesting to store: " + storeName + ", partition id: " + partitionId + " from files: " + sstFilePaths);
    try (IngestExternalFileOptions ingestOptions = new IngestExternalFileOptions()) {
      ingestOptions.setMoveFiles(true);
      final ColumnFamilyHandle columnFamilyHandle = isRMD
          ? columnFamilyHandleList.get(REPLICATION_METADATA_COLUMN_FAMILY_INDEX)
          : columnFamilyHandleList.get(DEFAULT_COLUMN_FAMILY_INDEX);

      deleteOldIngestion(rocksDB, columnFamilyHandle);

      rocksDB.ingestExternalFile(columnFamilyHandle, sstFilePaths, ingestOptions);

      LOGGER.info(
          "Finished {} ingestion to store: {}, partition id: {} from files: {}",
          isRMD ? "RMD" : "data",
          storeName,
          partitionId,
          sstFilePaths);
    } catch (RocksDBException e) {
      throw new VeniceException("Received exception during RocksDB#ingestExternalFile", e);
    }
  }

  private List<String> getTemporarySSTFilePaths() {
    File tempSSTFileDir = new File(fullPathForTempSSTFileDir);
    String[] sstFiles = tempSSTFileDir.list((dir, name) -> isTempSSTFile(name) && new File(dir, name).length() > 0);
    List<String> sstFilePaths = new ArrayList<>();
    if (sstFiles == null) {
      return sstFilePaths;
    }
    for (String sstFile: sstFiles) {
      sstFilePaths.add(tempSSTFileDir + File.separator + sstFile);
    }
    return sstFilePaths;
  }

  // Visible for testing
  public long getRecordNumInAllSSTFiles() {
    return recordNumInAllSSTFiles;
  }
}

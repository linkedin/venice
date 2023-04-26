package com.linkedin.davinci.store.rocksdb;

import static com.linkedin.venice.store.rocksdb.RocksDBUtils.extractTempSSTFileNo;
import static com.linkedin.venice.store.rocksdb.RocksDBUtils.isTempSSTFile;

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
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.EnvOptions;
import org.rocksdb.IngestExternalFileOptions;
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
  private int lastFinishedSSTFileNo = -1;
  /**
   * Whether the input is sorted or not.
   */
  private int currentSSTFileNo = 0;
  private SstFileWriter currentSSTFileWriter;
  private long recordNumInCurrentSSTFile = 0;
  private String fullPathForTempSSTFileDir;
  private Optional<Supplier<byte[]>> expectedChecksumSupplier;
  private final String storeName;
  private final int partitionId;
  private final EnvOptions envOptions;
  private final Options options;
  private final boolean isRMD;
  private final RocksDBServerConfig rocksDBServerConfig;
  private final String lastCheckPointedSSTFileNum;
  private final RocksDBIngestThrottler rocksDBIngestThrottler;
  private final String dbDir;

  public RocksDBSstFileWriter(
      String storeName,
      int partitionId,
      String dbDir,
      EnvOptions envOptions,
      Options options,
      String fullPathForTempSSTFileDir,
      boolean isRMD,
      RocksDBServerConfig rocksDBServerConfig,
      RocksDBIngestThrottler rocksDBIngestThrottler) {
    this.storeName = storeName;
    this.partitionId = partitionId;
    this.dbDir = dbDir;
    this.envOptions = envOptions;
    this.options = options;
    this.fullPathForTempSSTFileDir = fullPathForTempSSTFileDir;
    this.isRMD = isRMD;
    this.lastCheckPointedSSTFileNum = isRMD ? ROCKSDB_LAST_FINISHED_RMD_SST_FILE_NO : ROCKSDB_LAST_FINISHED_SST_FILE_NO;
    this.rocksDBServerConfig = rocksDBServerConfig;
    this.rocksDBIngestThrottler = rocksDBIngestThrottler;
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
  }

  public void open(Map<String, String> checkpointedInfo, Optional<Supplier<byte[]>> expectedChecksumSupplier) {
    LOGGER.info(
        "'beginBatchWrite' got invoked for RocksDB store: {}, partition: {} with checkpointed info: {} ",
        storeName,
        partitionId,
        checkpointedInfo);
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
    } else {
      lastFinishedSSTFileNo = Integer.parseInt(checkpointedInfo.get(lastCheckPointedSSTFileNum));
      LOGGER.info(
          "Received last finished sst file no: {} for store: {}, partition id: {}",
          lastFinishedSSTFileNo,
          storeName,
          partitionId);
      if (lastFinishedSSTFileNo < 0) {
        throw new VeniceException("Last finished sst file no: " + lastFinishedSSTFileNo + " shouldn't be negative");
      }
      makeSureAllPreviousSSTFilesBeforeCheckpointingExist();
      removeSSTFilesAfterCheckpointing();
      currentSSTFileNo = lastFinishedSSTFileNo + 1;
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
                    + recordNumInLastSSTFile + ", latency(ms): " + LatencyUtils.getElapsedTimeInMs(startMs));
          }
        }
      } else {
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

  private void removeSSTFilesAfterCheckpointing() {
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

  private void makeSureAllPreviousSSTFilesBeforeCheckpointingExist() {
    if (lastFinishedSSTFileNo < 0) {
      LOGGER.info("Since last finished sst file no is negative, there is nothing to verify");
      return;
    }
    for (int cur = 0; cur <= lastFinishedSSTFileNo; ++cur) {
      String sstFilePath = composeFullPathForSSTFile(cur);
      File sstFile = new File(sstFilePath);
      if (!sstFile.exists()) {
        throw new VeniceException(
            "SST File: " + sstFilePath + " doesn't exist, but last finished sst file no is: " + lastFinishedSSTFileNo);
      }
    }
  }

  private String composeFullPathForSSTFile(int sstFileNo) {
    return fullPathForTempSSTFileDir + File.separator + RocksDBUtils.composeTempSSTFileName(sstFileNo);
  }

  /**
   * This function calculates checksum of all the key/value pair stored in the input sstFilePath. It then
   * verifies if the checksum matches with the input checksumToMatch and return the result.
   * A SstFileReader handle is used to perform bulk scan through the entire SST file. fillCache option is
   * explicitely disabled to not pollute the rocksdb internal block caches. And also implicit checksum verification
   * is disabled to reduce latency of the entire operation.
   *
   * @param sstFilePath the full absolute path of the SST file
   * @param expectedRecordNumInSSTFile expected number of key/value pairs in the SST File
   * @param checksumToMatch pre-calculated checksum to match against.
   * @return true if the the sstFile checksum matches with the provided checksum.
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
      Optional<CheckSum> sstFileFinalCheckSum = CheckSum.getInstance(CheckSumType.MD5);
      sstFileReaderIterator = sstFileReader.newIterator(readOptions);
      sstFileReaderIterator.seekToFirst();
      while (sstFileReaderIterator.isValid()) {
        sstFileFinalCheckSum.get().update(sstFileReaderIterator.key());
        sstFileFinalCheckSum.get().update(sstFileReaderIterator.value());
        sstFileReaderIterator.next();
        recordCount++;
      }
      final byte[] finalChecksum = sstFileFinalCheckSum.get().getCheckSum();
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
      // TODO: Further explore re-ingestion of these files if SN crashes before EOP can be synced to disk
      rocksDBIngestThrottler.throttledIngest(
          dbDir,
          () -> rocksDB.ingestExternalFile(
              isRMD
                  ? columnFamilyHandleList.get(REPLICATION_METADATA_COLUMN_FAMILY_INDEX)
                  : columnFamilyHandleList.get(DEFAULT_COLUMN_FAMILY_INDEX),
              sstFilePaths,
              ingestOptions));

      LOGGER.info(
          "Finished ingestion to store: " + storeName + ", partition id: " + partitionId + " from files: "
              + sstFilePaths);
    } catch (RocksDBException | InterruptedException e) {
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
}

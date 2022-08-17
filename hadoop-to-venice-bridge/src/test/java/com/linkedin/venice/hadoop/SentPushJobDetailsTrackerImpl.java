package com.linkedin.venice.hadoop;

import com.linkedin.venice.status.protocol.PushJobDetails;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;


/**
 * This class is used keep track of push job details sent to the Venice controller for testing purpose. Instead of using
 * a mock and captors to capture input argument, this class is used since that to capture each {@link PushJobDetails},
 * a copy of it needs to be made to simulate the real situation in which KPJ sends messages to the Venice controller.
 */
public class SentPushJobDetailsTrackerImpl implements SentPushJobDetailsTracker {
  private final List<String> storeNames;
  private final List<Integer> versions;
  private final List<PushJobDetails> recordedPushJobDetails;

  SentPushJobDetailsTrackerImpl() {
    storeNames = new ArrayList<>();
    versions = new ArrayList<>();
    recordedPushJobDetails = new ArrayList<>();
  }

  @Override
  public void record(String storeName, int version, PushJobDetails pushJobDetails) {
    storeNames.add(storeName);
    versions.add(version);
    recordedPushJobDetails.add(makeCopyOf(pushJobDetails));
  }

  List<String> getStoreNames() {
    return Collections.unmodifiableList(storeNames);
  }

  List<Integer> getVersions() {
    return Collections.unmodifiableList(versions);
  }

  List<PushJobDetails> getRecordedPushJobDetails() {
    return Collections.unmodifiableList(recordedPushJobDetails);
  }

  private PushJobDetails makeCopyOf(PushJobDetails pushJobDetails) {
    PushJobDetails copy = new PushJobDetails();
    copy.reportTimestamp = pushJobDetails.reportTimestamp;
    copy.overallStatus = new ArrayList<>(pushJobDetails.overallStatus);
    copy.coloStatus =
        pushJobDetails.coloStatus == null ? pushJobDetails.coloStatus : new HashMap<>(pushJobDetails.coloStatus);
    copy.pushId = pushJobDetails.pushId;
    copy.partitionCount = pushJobDetails.partitionCount;
    copy.valueCompressionStrategy = pushJobDetails.valueCompressionStrategy;
    copy.chunkingEnabled = pushJobDetails.chunkingEnabled;
    copy.jobDurationInMs = pushJobDetails.jobDurationInMs;
    copy.totalNumberOfRecords = pushJobDetails.totalNumberOfRecords;
    copy.totalKeyBytes = pushJobDetails.totalKeyBytes;
    copy.totalRawValueBytes = pushJobDetails.totalRawValueBytes;
    copy.pushJobConfigs = pushJobDetails.pushJobConfigs == null
        ? pushJobDetails.pushJobConfigs
        : new HashMap<>(pushJobDetails.pushJobConfigs);
    copy.producerConfigs = pushJobDetails.producerConfigs == null
        ? pushJobDetails.producerConfigs
        : new HashMap<>(pushJobDetails.producerConfigs);
    copy.pushJobLatestCheckpoint = pushJobDetails.pushJobLatestCheckpoint;
    copy.failureDetails = pushJobDetails.failureDetails;
    return copy;
  }
}

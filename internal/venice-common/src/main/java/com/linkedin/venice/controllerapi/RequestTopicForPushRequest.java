package com.linkedin.venice.controllerapi;

import com.linkedin.venice.meta.Version.PushType;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;


public class RequestTopicForPushRequest {
  private final String clusterName;
  private final String storeName;
  private final PushType pushType;
  private final String pushJobId;

  private boolean sendStartOfPush = false;
  private boolean sorted = false; // an inefficient but safe default
  private boolean isWriteComputeEnabled = false;
  private boolean separateRealTimeTopicEnabled = false;
  private long rewindTimeInSecondsOverride = -1L;
  private boolean deferVersionSwap = false;
  private String targetedRegions = null;
  private int repushSourceVersion = -1;
  private Set<String> partitioners = Collections.emptySet();
  private String compressionDictionary = null;
  private X509Certificate certificateInRequest = null;
  private String sourceGridFabric = null;
  private String emergencySourceRegion = null;

  public RequestTopicForPushRequest(String clusterName, String storeName, PushType pushType, String pushJobId) {
    if (clusterName == null || clusterName.isEmpty()) {
      throw new IllegalArgumentException("clusterName is required");
    }
    if (storeName == null || storeName.isEmpty()) {
      throw new IllegalArgumentException("storeName is required");
    }
    if (pushType == null) {
      throw new IllegalArgumentException("pushType is required");
    }

    if (pushJobId == null || pushJobId.isEmpty()) {
      throw new IllegalArgumentException("pushJobId is required");
    }

    this.clusterName = clusterName;
    this.storeName = storeName;
    this.pushType = pushType;
    this.pushJobId = pushJobId;
  }

  public static PushType extractPushType(String pushTypeString) {
    try {
      return PushType.valueOf(pushTypeString);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          pushTypeString + " is an invalid push type. Valid push types are: " + Arrays.toString(PushType.values()));
    }
  }

  public String getClusterName() {
    return clusterName;
  }

  public String getStoreName() {
    return storeName;
  }

  public PushType getPushType() {
    return pushType;
  }

  public String getPushJobId() {
    return pushJobId;
  }

  public boolean isSendStartOfPush() {
    return sendStartOfPush;
  }

  public boolean isSorted() {
    return sorted;
  }

  public boolean isWriteComputeEnabled() {
    return isWriteComputeEnabled;
  }

  public String getSourceGridFabric() {
    return sourceGridFabric;
  }

  public long getRewindTimeInSecondsOverride() {
    return rewindTimeInSecondsOverride;
  }

  public boolean isDeferVersionSwap() {
    return deferVersionSwap;
  }

  public String getTargetedRegions() {
    return targetedRegions;
  }

  public int getRepushSourceVersion() {
    return repushSourceVersion;
  }

  public Set<String> getPartitioners() {
    return partitioners;
  }

  public String getCompressionDictionary() {
    return compressionDictionary;
  }

  public X509Certificate getCertificateInRequest() {
    return certificateInRequest;
  }

  public String getEmergencySourceRegion() {
    return emergencySourceRegion;
  }

  public void setSendStartOfPush(boolean sendStartOfPush) {
    this.sendStartOfPush = sendStartOfPush;
  }

  public void setSorted(boolean sorted) {
    this.sorted = sorted;
  }

  public void setWriteComputeEnabled(boolean writeComputeEnabled) {
    isWriteComputeEnabled = writeComputeEnabled;
  }

  public void setSourceGridFabric(String sourceGridFabric) {
    this.sourceGridFabric = sourceGridFabric;
  }

  public void setRewindTimeInSecondsOverride(long rewindTimeInSecondsOverride) {
    this.rewindTimeInSecondsOverride = rewindTimeInSecondsOverride;
  }

  public void setDeferVersionSwap(boolean deferVersionSwap) {
    this.deferVersionSwap = deferVersionSwap;
  }

  public void setTargetedRegions(String targetedRegions) {
    this.targetedRegions = targetedRegions;
  }

  public void setRepushSourceVersion(int repushSourceVersion) {
    this.repushSourceVersion = repushSourceVersion;
  }

  public void setPartitioners(String commaSeparatedPartitioners) {
    if (commaSeparatedPartitioners == null || commaSeparatedPartitioners.isEmpty()) {
      return;
    }
    setPartitioners(new HashSet<>(Arrays.asList(commaSeparatedPartitioners.split(","))));
  }

  public void setPartitioners(Set<String> partitioners) {
    this.partitioners = partitioners;
  }

  public void setCompressionDictionary(String compressionDictionary) {
    this.compressionDictionary = compressionDictionary;
  }

  public void setCertificateInRequest(X509Certificate certificateInRequest) {
    this.certificateInRequest = certificateInRequest;
  }

  public void setEmergencySourceRegion(String emergencySourceRegion) {
    this.emergencySourceRegion = emergencySourceRegion;
  }

  public boolean isSeparateRealTimeTopicEnabled() {
    return separateRealTimeTopicEnabled;
  }

  public void setSeparateRealTimeTopicEnabled(boolean separateRealTimeTopicEnabled) {
    this.separateRealTimeTopicEnabled = separateRealTimeTopicEnabled;
  }
}

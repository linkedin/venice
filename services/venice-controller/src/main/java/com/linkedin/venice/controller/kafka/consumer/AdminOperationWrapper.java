package com.linkedin.venice.controller.kafka.consumer;

import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;


public class AdminOperationWrapper {
  private final AdminOperation adminOperation;
  private final long offset;
  private final long producerTimestamp;
  private final long localBrokerTimestamp;
  private final long delegateTimestamp;

  private Long startProcessingTimestamp = null;

  /**
   * Constructor for the wrapper of an {@link AdminOperation}, the wrapper includes additional information about the
   * operation that is used for processing and metrics emission.
   * @param adminOperation of this wrapper.
   * @param offset for the corresponding {@link AdminOperation}.
   * @param producerTimestamp the time when this admin operation was first produced in the parent controller.
   * @param localBrokerTimestamp the time when this admin operation arrived at the local admin kafka topic or broker.
   * @param delegateTimestamp the time when this admin operation was read and placed in the in-memory topics.
   */
  AdminOperationWrapper(
      AdminOperation adminOperation,
      long offset,
      long producerTimestamp,
      long localBrokerTimestamp,
      long delegateTimestamp) {
    this.adminOperation = adminOperation;
    this.offset = offset;
    this.producerTimestamp = producerTimestamp;
    this.localBrokerTimestamp = localBrokerTimestamp;
    this.delegateTimestamp = delegateTimestamp;
  }

  public AdminOperation getAdminOperation() {
    return adminOperation;
  }

  public long getOffset() {
    return offset;
  }

  public long getProducerTimestamp() {
    return producerTimestamp;
  }

  public long getLocalBrokerTimestamp() {
    return localBrokerTimestamp;
  }

  public long getDelegateTimestamp() {
    return delegateTimestamp;
  }

  public Long getStartProcessingTimestamp() {
    return startProcessingTimestamp;
  }

  public void setStartProcessingTimestamp(long startProcessingTimestamp) {
    this.startProcessingTimestamp = startProcessingTimestamp;
  }
}

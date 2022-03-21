package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.helix.LeaderFollowerPartitionStateModel;
import com.linkedin.venice.utils.PartitionUtils;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.function.IntSupplier;


class AmplificationAdapter {
  private final int amplificationFactor;
  private final ReportStatusAdapter reportStatusAdapter;
  private final PriorityBlockingQueue<ConsumerAction> consumerActionsQueue;
  private final ConcurrentMap<Integer, PartitionConsumptionState> partitionConsumptionStateMap;
  private final IntSupplier nextSeqNum;

  public AmplificationAdapter(
      int amplificationFactor,
      ReportStatusAdapter reportStatusAdapter,
      PriorityBlockingQueue<ConsumerAction> consumerActionsQueue,
      ConcurrentMap<Integer, PartitionConsumptionState> partitionConsumptionStateMap,
      IntSupplier nextConsumerActionSeqNumProvider
  ) {
    this.amplificationFactor = amplificationFactor;
    this.reportStatusAdapter = reportStatusAdapter;
    this.consumerActionsQueue = consumerActionsQueue;
    this.partitionConsumptionStateMap = partitionConsumptionStateMap;
    this.nextSeqNum = nextConsumerActionSeqNumProvider;
  }

  public void subscribePartition(String topic, int partition, Optional<LeaderFollowerStateType> leaderState) {
    reportStatusAdapter.preparePartitionStatusCleanup(partition);
    /*
     * RT topic partition count : VT topic partition count is 1 : amp_factor. For every user partition, there should
     * be only one sub-partition to act as leader to process and produce records from real-time topic partition to all
     * sub-partitions of the same user partition of the version topic.
     */
    int leaderSubPartition = PartitionUtils.getLeaderSubPartition(partition, amplificationFactor);
    for (int subPartition : PartitionUtils.getSubPartitions(partition, amplificationFactor)) {
      if (leaderSubPartition == subPartition) {
        consumerActionsQueue.add(new ConsumerAction(ConsumerActionType.SUBSCRIBE, topic, subPartition, nextSeqNum.getAsInt(), leaderState));
      } else {
        consumerActionsQueue.add(new ConsumerAction(ConsumerActionType.SUBSCRIBE, topic, subPartition, nextSeqNum.getAsInt(), Optional.empty()));
      }
    }
  }

  public void unSubscribePartition(String topic, int partition) {
    for (int subPartition : PartitionUtils.getSubPartitions(partition, amplificationFactor)) {
      consumerActionsQueue.add(new ConsumerAction(ConsumerActionType.UNSUBSCRIBE, topic, subPartition, nextSeqNum.getAsInt()));
    }
  }

  public void resetPartitionConsumptionOffset(String topic, int partition) {
    for (int subPartition : PartitionUtils.getSubPartitions(partition, amplificationFactor)) {
      consumerActionsQueue.add(new ConsumerAction(ConsumerActionType.RESET_OFFSET, topic, subPartition, nextSeqNum.getAsInt()));
    }
  }

  public void promoteToLeader(String topic, int partition, LeaderFollowerPartitionStateModel.LeaderSessionIdChecker checker) {
    for (int subPartition : PartitionUtils.getSubPartitions(partition, amplificationFactor)) {
      consumerActionsQueue.add(new ConsumerAction(ConsumerActionType.STANDBY_TO_LEADER, topic, subPartition, nextSeqNum.getAsInt(), checker));
    }
  }

  public void demoteToStandby(String topic, int partition, LeaderFollowerPartitionStateModel.LeaderSessionIdChecker checker) {
    for (int subPartition : PartitionUtils.getSubPartitions(partition, amplificationFactor)) {
      consumerActionsQueue.add(new ConsumerAction(ConsumerActionType.LEADER_TO_STANDBY, topic, subPartition, nextSeqNum.getAsInt(), checker));
    }
  }

  public LeaderFollowerStateType getLeaderState(final int partition) {
    int leaderSubPartition = PartitionUtils.getLeaderSubPartition(partition, amplificationFactor);
    if (partitionConsumptionStateMap.containsKey(leaderSubPartition)) {
      return partitionConsumptionStateMap.get(leaderSubPartition).getLeaderFollowerState();
    }
    // By default L/F state is STANDBY
    return LeaderFollowerStateType.STANDBY;
  }

  public boolean isPartitionConsuming(final int partition) {
    for (int subPartition : PartitionUtils.getSubPartitions(partition, amplificationFactor)) {
      if (partitionConsumptionStateMap.containsKey(subPartition)) {
        return true;
      }
    }
    return false;
  }

  public boolean isLeaderSubPartition(final int subPartition) {
    int userPartition = PartitionUtils.getUserPartition(subPartition, amplificationFactor);
    int leaderSubPartition = PartitionUtils.getLeaderSubPartition(userPartition, amplificationFactor);
    return subPartition == leaderSubPartition;
  }
}

package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.utils.PartitionUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Predicate;


/**
 * This class hides and handles amplification factor concept for each user partition. All the sub-partition logics and
 * concepts are handled here.
 */
public class AmplificationFactorAdapter {
  private final int amplificationFactor;
  private final ConcurrentMap<Integer, PartitionConsumptionState> partitionConsumptionStateMap;

  public AmplificationFactorAdapter(
      int amplificationFactor,
      ConcurrentMap<Integer, PartitionConsumptionState> partitionConsumptionStateMap) {
    this.amplificationFactor = amplificationFactor;
    this.partitionConsumptionStateMap = partitionConsumptionStateMap;
  }

  /**
   * This method takes in customized logic and execute it on every subPartition's {@link PartitionConsumptionState} of
   * the user partition.
   */
  public void executePartitionConsumptionState(int userPartition, Consumer<PartitionConsumptionState> pcsConsumer) {
    for (int subPartition: PartitionUtils.getSubPartitions(userPartition, amplificationFactor)) {
      if (partitionConsumptionStateMap.containsKey(subPartition)) {
        pcsConsumer.accept(partitionConsumptionStateMap.get(subPartition));
      }
    }
  }

  /**
   * This method takes in customized logic for each subPartition number and be executed on every subPartition of the
   * specified user partition.
   */
  public void execute(int userPartition, Consumer<Integer> consumer) {
    for (int subPartition: PartitionUtils.getSubPartitions(userPartition, amplificationFactor)) {
      consumer.accept(subPartition);
    }
  }

  /**
   * This method returns True when the predicate is tested True for any of the subPartition in the user partition.
   */
  public boolean meetsAny(int userPartition, Predicate<Integer> predicate) {
    for (int subPartition: PartitionUtils.getSubPartitions(userPartition, amplificationFactor)) {
      if (predicate.test(subPartition)) {
        return true;
      }
    }
    return false;
  }

  /**
   * @return True if a subPartition's index is the first among all subPartitions of its user partition.
   */
  public boolean isLeaderSubPartition(final int subPartition) {
    int userPartition = PartitionUtils.getUserPartition(subPartition, amplificationFactor);
    int leaderSubPartition = PartitionUtils.getLeaderSubPartition(userPartition, amplificationFactor);
    return subPartition == leaderSubPartition;
  }

  /**
   * @return {@link LeaderFollowerStateType} of the user partition.
   */
  public LeaderFollowerStateType getLeaderState(final int partition) {
    int leaderSubPartition = PartitionUtils.getLeaderSubPartition(partition, amplificationFactor);
    if (partitionConsumptionStateMap.containsKey(leaderSubPartition)) {
      return partitionConsumptionStateMap.get(leaderSubPartition).getLeaderFollowerState();
    }
    // By default, L/F state is STANDBY
    return LeaderFollowerStateType.STANDBY;
  }

  /**
   * @return List of {@link PartitionConsumptionState} of leader subPartitions.
   */
  public List<PartitionConsumptionState> getLeaderPcsList(Collection<PartitionConsumptionState> pcsList) {
    List<PartitionConsumptionState> leaderPcsList = new ArrayList<>();
    for (PartitionConsumptionState pcs: pcsList) {
      if (isLeaderSubPartition(pcs.getPartition())) {
        leaderPcsList.add(pcs);
      }
    }
    return leaderPcsList;
  }

  public int getAmplificationFactor() {
    return amplificationFactor;
  }
}

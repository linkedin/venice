package com.linkedin.alpini.router.api;

import java.util.List;
import javax.annotation.Nonnull;


/**
 * Provides methods to find the partition name for a piece of data.
 *
 * @author Jemiah Westerman<jwesterman@linkedin.com>
 */
public interface PartitionFinder<K> {
  /**
   * Returns the name of partition for the given key within the given table.
   *
   * @param resourceName name of the database
   * @param partitionKey partition key field for the data
   * @return name of the partition
   * @throws RouterException if there was an error
   */
  @Nonnull
  String findPartitionName(@Nonnull String resourceName, @Nonnull K partitionKey) throws RouterException;

  @Nonnull
  List<String> getAllPartitionNames(@Nonnull String resourceName) throws RouterException;

  /**
   * Returns the number of partitions for a given db
   * @param resourceName name of the database
   */
  int getNumPartitions(@Nonnull String resourceName) throws RouterException;
}

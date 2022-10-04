package com.linkedin.alpini.router.api;

import com.linkedin.alpini.base.misc.Pair;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import javax.annotation.Nonnull;


/**
 * Created by acurtis on 1/26/18.
 */
public interface AsyncPartitionFinder<K> {
  /**
   * Returns the name of partition for the given key within the given table.
   *
   * @param resourceName name of the database
   * @param partitionKey partition key field for the data
   * @return name of the partition
   * @throws RouterException if there was an error
   */
  @Nonnull
  CompletionStage<String> findPartitionName(@Nonnull String resourceName, @Nonnull K partitionKey);

  @Nonnull
  default CompletionStage<String> findPartitionName(@Nonnull Pair<String, K> resource) {
    return findPartitionName(resource.getFirst(), resource.getSecond());
  }

  @Nonnull
  CompletionStage<List<String>> getAllPartitionNames(@Nonnull String resourceName);

  /**
   * Returns the number of partitions for a given db
   * @param resourceName name of the database
   */
  CompletionStage<Integer> getNumPartitions(@Nonnull String resourceName);

  static <K> AsyncPartitionFinder<K> adapt(PartitionFinder<K> partitionFinder, Executor executor) {

    class Supply<T> implements Supplier<T> {
      private final Callable<T> _callable;

      Supply(Callable<T> callable) {
        _callable = callable;
      }

      @Override
      public T get() {
        try {
          return _callable.call();
        } catch (Exception e) {
          throw new CompletionException(e);
        }
      }
    }

    return new AsyncPartitionFinder<K>() {
      @Nonnull
      @Override
      public CompletionStage<String> findPartitionName(@Nonnull String resourceName, @Nonnull K partitionKey) {
        return CompletableFuture
            .supplyAsync(new Supply<>(() -> partitionFinder.findPartitionName(resourceName, partitionKey)), executor);
      }

      @Nonnull
      @Override
      public CompletionStage<List<String>> getAllPartitionNames(@Nonnull String resourceName) {
        return CompletableFuture
            .supplyAsync(new Supply<>(() -> partitionFinder.getAllPartitionNames(resourceName)), executor);
      }

      @Override
      public CompletionStage<Integer> getNumPartitions(@Nonnull String resourceName) {
        return CompletableFuture
            .supplyAsync(new Supply<>(() -> partitionFinder.getNumPartitions(resourceName)), executor);
      }
    };
  }
}

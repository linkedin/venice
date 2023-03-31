package com.linkedin.venice.producer;

import com.linkedin.venice.writer.update.UpdateBuilder;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;


/**
 * The API for online applications to write to Venice. These APIs are only eventually consistent and the futures
 * returned by them only guarantee that the write operation was durable. There is no option to block until the data
 * is readable.
 * @param <K> Key of the record that needs to be updated
 * @param <V> Value that needs to be written
 */
public interface VeniceProducer<K, V> {
  /**
   * A write operation where a full value is written to replace the existing value.
   * @param key Key of the record that needs to be updated
   * @param value The full value that needs to be written
   * @return A {@link CompletableFuture} that completes when the write operation is durable. It does not imply that the
   *         data is available to readers.
   */
  CompletableFuture<DurableWrite> asyncPut(K key, V value);

  /**
   * A write operation where a full value is written to replace the existing value. It offers the writers to specify a
   * logical time. This value is used to specify the ordering of operations and perform conflict resolution in
   * Active/Active replication.
   * @param logicalTime The value used during conflict resolution in Active/Active replication
   * @param key Key of the record that needs to be updated
   * @param value The full value that needs to be written
   * @return A {@link CompletableFuture} that completes when the write operation is durable. It does not imply that the
   *         data is available to readers.
   */
  CompletableFuture<DurableWrite> asyncPut(long logicalTime, K key, V value);

  /**
   * A write operation to delete the record for a key.
   * @param key The key associated with the record that should be deleted
   * @return A {@link CompletableFuture} that completes when the write operation is durable. It does not imply that the
   *         data is available to readers.
   */
  CompletableFuture<DurableWrite> asyncDelete(K key);

  /**
   * A write operation to delete the record for a key. It offers the writers to specify a logical time. This value is
   * used to specify the ordering of operations and perform conflict resolution in Active/Active replication.
   * @param logicalTime The value used during conflict resolution in Active/Active replication
   * @param key Key of the record that needs to be deleted
   * @return A {@link CompletableFuture} that completes when the write operation is durable. It does not imply that the
   *         data is available to readers.
   */
  CompletableFuture<DurableWrite> asyncDelete(long logicalTime, K key);

  /**
   * A write operation to modify a subset of fields in the record for a key.
   * @param key Key of the record that needs to be updated
   * @param updateFunction A {@link Consumer} that takes in an {@link UpdateBuilder} object and updates it to specify
   *                       which fields to modify and the operations that must be done on them.
   * @return A {@link CompletableFuture} that completes when the write operation is durable. It does not imply that the
   *         data is available to readers.
   */
  CompletableFuture<DurableWrite> asyncUpdate(K key, Consumer<UpdateBuilder> updateFunction);

  /**
   * A write operation to modify a subset of fields in the record for a key. It offers the writers to specify a logical
   * time. This value is used to specify the ordering of operations and perform conflict resolution in Active/Active
   * replication.
   * @param logicalTime The value used during conflict resolution in Active/Active replication
   * @param key Key of the record that needs to be updated
   * @param updateFunction A {@link Consumer} that takes in an {@link UpdateBuilder} object and updates it to specify
   *                       which fields to modify and the operations that must be done on them.
   * @return A {@link CompletableFuture} that completes when the write operation is durable. It does not imply that the
   *         data is available to readers.
   */
  CompletableFuture<DurableWrite> asyncUpdate(long logicalTime, K key, Consumer<UpdateBuilder> updateFunction);
}

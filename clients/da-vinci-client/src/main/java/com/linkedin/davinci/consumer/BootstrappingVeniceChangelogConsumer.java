package com.linkedin.davinci.consumer;

import com.linkedin.venice.annotation.Experimental;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;


/**
 * This interface is meant for users where local state must be built off of the entirety of a venice data set
 * (i.e. Non-idempotent event ingestion), rather than dealing with an event at a time. THIS IS EXPENSIVE.
 * It's highly recommended that users use the {@link VeniceChangelogConsumer} interface as a means to consume Venice
 * Change capture data.
 *
 * Implementations of this interface rely on access to a compacted view to the data and scanning the entirety of that
 * compacted view initial calls to poll(). This is the only supported pattern with this interface today. {@link VeniceChangelogConsumer}
 * enables finer control.  This interface is intentionally limited as implementors rely on local checkpointing and
 * maintenance of state which might be easily corrupted with byzantine seek() calls.
 * @param <K>
 * @param <V>
 */
@Experimental
public interface BootstrappingVeniceChangelogConsumer<K, V> {
  /**
   * Start performs both a topic subscription and catch up. The client will look at the latest offset in the server and
   * sync bootstrap data up to that point in changes. Once that is done for all partitions, the future will complete.
   *
   * NOTE: This future may take some time to complete depending on how much data needs to be ingested in order to catch
   * up with the time that this client started.
   *
   * @param partitions which partition id's to catch up with
   * @return a future that completes once catch up is complete for all passed in partitions.
   */
  CompletableFuture<Void> start(Set<Integer> partitions);

  CompletableFuture<Void> start();

  void stop() throws Exception;

  /**
   * polls for the next batch of change events. The first records returned following calling 'start()' will be from the bootstrap state.
   * Once this state is consumed, subsequent calls to poll will be based off of recent updates to the Venice store.
   * @param timeoutInMs
   * @return
   */
  Collection<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> poll(long timeoutInMs);

}

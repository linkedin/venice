package com.linkedin.davinci.consumer;

import com.linkedin.venice.pubsub.api.PubSubMessage;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;


public interface BootstrappingVeniceChangelogConsumer<K, V> {
  /**
   *
   * seekWithBootstrap(), no timestamp.  Goes to end of offset for whatever topic we were consuming on.  Consume it all.
   * NOTE: Provide config to optionally abort in case the data volume is huge.  Should log a message that says how many messages
   * we'll consume? Whatever.
   *
   * So steps.
   *
   * If local bootstrap is enabled
   *      -poll will persist to local rocksdb.
   *      -seekWithBootstrap will catch up to the current time the local state and subsequent poll calls will
   *       return the scan of the rocksdb data until it gets to the end. Once it gets to the end, we'll start returning
   *       topic poll results.
   *      -On startup after subscribe we'll check the current version and see what version we have locally. If the version
   *       doesn't match, we'll need to make a call on if we clean up or not. This one is hard. I think we should do it like this.
   *       If the LAST version we had persisted still has live CC topics and the next version is a repush, consume normally
   *       and jump to the next version.  If the last version is no longer live drop, and if the next version isn't a repush drop.
   *      -When a versionSwap is encountered, if the versionSwap isn't a repush, nuke everything. If it's a repush, keep it.
   *
   *
   * Implementation:
   *  -New config for local persistence.
   *  -calls to seekWithBootstrap will just not do anything? Throw exception? Hum.
   *  -Wraps the VeniceChangelog consumer and overrides some methods.  We'll need to override poll() to act on stuff.
   *   actually doing that is kind of nice, we don't need to worry about compression or anything, we'll just let the internal client handle it.
   *  -Use rocksdb storage partitions to store the data.
   *
   *
   * @param partitions
   * @param timeStamp
   * @return
   */
  CompletableFuture<Void> start(Set<Integer> partitions);

  CompletableFuture<Void> start();

  void stop() throws Exception;

  Collection<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> poll(long timeoutInMs);

}

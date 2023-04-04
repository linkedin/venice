package com.linkedin.venice.producer;

/**
 * This class is used as the return type of the {@link java.util.concurrent.CompletableFuture} that is returned
 * by the Venice producer. It only signifies that the write operation is durable in the PubSub system (and eventually in
 * Venice storage). It does not imply that the data is available to readers.
 */
public class DurableWrite {
}

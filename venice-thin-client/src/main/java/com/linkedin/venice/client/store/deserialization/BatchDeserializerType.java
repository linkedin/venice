package com.linkedin.venice.client.store.deserialization;

import com.linkedin.venice.client.store.ClientConfig;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

/**
 * This enum controls the behavior of the user payload deserialization phase of the batch get response handling.
 */
public enum BatchDeserializerType {
  BLOCKING((executor, clientConfig) -> new BlockingDeserializer(executor, clientConfig)),
  ONE_FUTURE_PER_RECORD((executor, clientConfig) -> new OneFuturePerRecordDeserializer(executor, clientConfig)),
  ALWAYS_ON_MULTI_THREADED_PIPELINE((executor, clientConfig) -> new AlwaysOnMultiThreadedDeserializerPipeline(executor, clientConfig)),
  BLACK_HOLE((executor, clientConfig) -> new BlackHoleDeserializer(executor, clientConfig));

  private final BiFunction<Executor, ClientConfig, BatchDeserializer> generator;

  BatchDeserializerType(BiFunction<Executor, ClientConfig, BatchDeserializer> generator) {
    this.generator = generator;
  }

  public BatchDeserializer get(Executor executor, ClientConfig clientConfig) {
    return generator.apply(executor, clientConfig);
  }
}

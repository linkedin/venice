package com.linkedin.venice.fastclient;

import com.linkedin.alpini.base.concurrency.TimeoutProcessor;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.utils.MultiKeyLongTailRetryPolicy;
import java.util.function.Supplier;
import org.apache.avro.specific.SpecificRecord;


public class RetriableAvroSpecificStoreClient<K, V extends SpecificRecord> extends RetriableAvroGenericStoreClient<K, V>
    implements AvroSpecificStoreClient<K, V> {
  public RetriableAvroSpecificStoreClient(
      InternalAvroStoreClient<K, V> delegate,
      ClientConfig clientConfig,
      TimeoutProcessor timeoutProcessor) {
    super(delegate, clientConfig, timeoutProcessor);
  }

  public RetriableAvroSpecificStoreClient(
      InternalAvroStoreClient<K, V> delegate,
      ClientConfig clientConfig,
      TimeoutProcessor timeoutProcessor,
      Supplier<MultiKeyLongTailRetryPolicy> serverRetryPolicy) {
    super(delegate, clientConfig, timeoutProcessor, serverRetryPolicy);
  }
}

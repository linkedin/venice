package com.linkedin.venice.beam.consumer;

import com.linkedin.davinci.consumer.ChangeEvent;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;


/**
 * Uses {@link KvCoder} to encode/decode {@link com.linkedin.venice.pubsub.api.PubSubMessage} key
 * and value.
 */
public final class VeniceMessageCoder<K, V> extends StructuredCoder<KV<K, ChangeEvent<V>>> {
  private static final long serialVersionUID = 1L;
  private final AvroCoder<K> keyCoder = AvroCoder.of(new TypeDescriptor<K>() {
  });
  private final AvroCoder<V> valueCoder = AvroCoder.of(new TypeDescriptor<V>() {
  });

  private final ListCoder<V> listCoder = ListCoder.of(valueCoder);
  private final KvCoder<K, List<V>> _kvCoder = KvCoder.of(keyCoder, listCoder);

  private VeniceMessageCoder() {
  }

  public static <K, V> VeniceMessageCoder<K, V> of() {
    return new VeniceMessageCoder<>();
  }

  @Override
  public void encode(KV<K, ChangeEvent<V>> value, OutputStream outStream) throws IOException {
    ChangeEvent<V> changeEvent = value.getValue();
    List<V> values = new ArrayList<>();
    values.add(changeEvent.getCurrentValue());
    values.add(changeEvent.getPreviousValue());
    _kvCoder.encode(KV.of(value.getKey(), values), outStream);
  }

  @Override
  public KV<K, ChangeEvent<V>> decode(InputStream inStream) throws IOException {
    KV<K, List<V>> kv = _kvCoder.decode(inStream);
    return KV.of(kv.getKey(), new ChangeEvent<>(kv.getValue().get(1), kv.getValue().get(0)));
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Collections.singletonList(_kvCoder);
  }

  @Override
  public void verifyDeterministic() {
  }
}

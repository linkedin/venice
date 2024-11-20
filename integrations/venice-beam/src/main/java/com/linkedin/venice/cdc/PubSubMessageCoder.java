package com.linkedin.venice.cdc;

import com.linkedin.davinci.consumer.ChangeEvent;
import com.linkedin.davinci.consumer.VeniceChangeCoordinate;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;


/** Uses {@link AvroCoder} to encode/decode {@link PubSubMessage}s. */
public class PubSubMessageCoder<K, V>
    extends StructuredCoder<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> {
  private static final long serialVersionUID = 1L;

  private final AvroCoder<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> pubSubMessageAvroCoder =
      AvroCoder.of(new TypeDescriptor<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>>() {
      });

  public static <K, V> PubSubMessageCoder<K, V> of() {
    return new PubSubMessageCoder<>();
  }

  @Override
  public void encode(
      PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate> value,
      @UnknownKeyFor @NonNull @Initialized OutputStream outStream)
      throws @UnknownKeyFor @NonNull @Initialized IOException {
    pubSubMessageAvroCoder.encode(value, outStream);
  }

  @Override
  public PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate> decode(
      @UnknownKeyFor @NonNull @Initialized InputStream inStream)
      throws @UnknownKeyFor @NonNull @Initialized IOException {
    return pubSubMessageAvroCoder.decode(inStream);
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized List<? extends @UnknownKeyFor @NonNull @Initialized Coder<@UnknownKeyFor @NonNull @Initialized ?>> getCoderArguments() {
    return Collections.singletonList(pubSubMessageAvroCoder);
  }

  @Override
  public void verifyDeterministic() {
  }
}

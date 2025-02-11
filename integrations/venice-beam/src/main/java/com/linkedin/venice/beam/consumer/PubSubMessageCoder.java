package com.linkedin.venice.beam.consumer;

import com.linkedin.davinci.consumer.ChangeEvent;
import com.linkedin.davinci.consumer.VeniceChangeCoordinate;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.values.TypeDescriptor;


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
  public void encode(PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate> value, @Nonnull OutputStream outStream)
      throws IOException {
    pubSubMessageAvroCoder.encode(value, outStream);
  }

  @Override
  public PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate> decode(@Nonnull InputStream inStream)
      throws IOException {
    return pubSubMessageAvroCoder.decode(inStream);
  }

  @Override
  public @Nonnull List<? extends Coder<?>> getCoderArguments() {
    return Collections.singletonList(pubSubMessageAvroCoder);
  }

  @Override
  public void verifyDeterministic() {
  }
}

package com.linkedin.venice.serializer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import java.util.ArrayList;
import java.util.List;


public class AvroGenericDeserializer<V> implements RecordDeserializer<V> {
  /**
   * This enum determines how a series of records are going to be deserialized. This is relevant for the
   * envelope deserialization phase of the batch get response handling.
   */
  public enum IterableImpl {
    /**
     * Deserialize all records in a blocking fashion at {@link Iterable} creation-time, stores them into
     * a {@link List}, and returns the list. Since it is a list, it allows replaying, including concurrently.
     *
     * This can be used in the backend, where iterating multiple times is sometimes necessary.
     */
    BLOCKING,

    /**
     * Returns an {@link Iterable} immediately, and then lazily deserialize one record at a time as requested
     * from the Iterable's {@link Iterator}.
     *
     * N.B.: Only supports one pass over the items. Does not support replaying. If this assumption is
     *       violated, a {@link VeniceSerializationException} will be thrown.
     *
     * This is for use in the thin client, where only one pass is required, and we want to start operating
     * on the records as soon as they become available.
     */
    LAZY,

    /**
     * Returns an {@link Iterable} immediately, and then lazily deserialize one record at a time as requested
     * from the Iterable's {@link Iterator}. Also this implementation internally caches the items in a list
     * as they are deserialized, in order to support efficient replaying.
     *
     * N.B.: Concurrent iteration is not supported. The first pass must be finished completely before a
     *       second pass can be begun. If this assumption is violated, a {@link VeniceSerializationException}
     *       will be thrown.
     *
     * This can be used in the backend, where iterating multiple times is sometimes necessary.
     */
    LAZY_WITH_REPLAY_SUPPORT;
  }

  /**
   * This default is intended for the backend. The client overrides this, and its default is defined in
   * the ClientConfig class, along with all the other client defaults.
   */
  private static final IterableImpl ITERABLE_IMPL_DEFAULT = IterableImpl.BLOCKING;

  private final DatumReader<V> datumReader;
  private final IterableImpl iterableImpl;

  public AvroGenericDeserializer(Schema writer, Schema reader) {
    this(new GenericDatumReader<>(writer, reader));
  }

  protected AvroGenericDeserializer(DatumReader<V> datumReader) {
    this(datumReader, null);
  }

  protected AvroGenericDeserializer(DatumReader<V> datumReader, IterableImpl iterableImpl) {
    this.datumReader = datumReader;
    this.iterableImpl = Optional.ofNullable(iterableImpl).orElse(ITERABLE_IMPL_DEFAULT);
  }

  @Override
  public V deserialize(byte[] bytes) throws VeniceSerializationException {
    return deserialize(null, bytes);
  }

  @Override
  public V deserialize(ByteBuffer byteBuffer) throws VeniceSerializationException {
    return deserialize(null, byteBuffer, null);
  }

  @Override
  public V deserialize(V reuse, ByteBuffer byteBuffer, BinaryDecoder reusedDecoder)
      throws VeniceSerializationException {
    BinaryDecoder decoder = DecoderFactory.defaultFactory()
        .createBinaryDecoder(byteBuffer.array(), byteBuffer.position(), byteBuffer.remaining(), reusedDecoder);
    return deserialize(reuse, decoder);
  }

  @Override
  public V deserialize(V reuseRecord, byte[] bytes) throws VeniceSerializationException {
    // This param is to re-use a decoder instance. TODO: explore GC tuning later.
    BinaryDecoder decoder = DecoderFactory.defaultFactory().createBinaryDecoder(bytes, null);
    return deserialize(reuseRecord, decoder);
  }

  @Override
  public V deserialize(BinaryDecoder decoder) throws VeniceSerializationException {
    return deserialize(null, decoder);
  }

  @Override
  public V deserialize(V reuseRecord, BinaryDecoder decoder) throws VeniceSerializationException {
    try {
      return datumReader.read(reuseRecord, decoder);
    } catch (Exception e) {
      throw new VeniceSerializationException("Could not deserialize bytes back into Avro object", e);
    }
  }

  @Override
  public V deserialize(V reuseRecord, InputStream in, BinaryDecoder reusedDecoder) throws VeniceSerializationException {
    BinaryDecoder decoder = DecoderFactory.defaultFactory().createBinaryDecoder(in, reusedDecoder);
    return deserialize(reuseRecord, decoder);
  }

  @Override
  public Iterable<V> deserializeObjects(byte[] bytes) throws VeniceSerializationException {
    // This param is to re-use a decoder instance. TODO: explore GC tuning later.
    return deserializeObjects(DecoderFactory.defaultFactory().createBinaryDecoder(bytes, null));
  }

  @Override
  public Iterable<V> deserializeObjects(BinaryDecoder decoder) throws VeniceSerializationException {
    switch (iterableImpl) {
      case BLOCKING:
        List<V> objects = new ArrayList();
        try {
          while (!decoder.isEnd()) {
            objects.add(datumReader.read(null, decoder));
          }
        } catch (Exception e) {
          throw new VeniceSerializationException("Could not deserialize bytes back into Avro objects", e);
        }

        return objects;
      case LAZY:
        return new LazyCollectionDeserializerIterable<>(decoder, datumReader, false);
      case LAZY_WITH_REPLAY_SUPPORT:
        return new LazyCollectionDeserializerIterable<>(decoder, datumReader, true);
      default:
        throw new IllegalStateException("Unrecognized IterableImpl: " + iterableImpl);
    }
  }

  private static class LazyCollectionDeserializerIterable<T> implements Iterable<T> {
    /**
     * Not final because the {@link #iterator()} function has the side effect of setting this to null, if
     * replay is enabled, in order to avoid giving it away more than once.
     */
    private LazyCollectionDeserializerIterator lazyIterator;

    /**
     * @param decoder containing the bytes to decode
     * @param datumReader which will use the decoder to extract records
     * @param enableReplay if true, deserialized records are stored in a backing list, in order to allow efficient replay
     *                     if false, deserialization can happen only once
     */
    LazyCollectionDeserializerIterable(BinaryDecoder decoder, DatumReader<T> datumReader, boolean enableReplay) {
      this.lazyIterator = new LazyCollectionDeserializerIterator(decoder, datumReader, enableReplay ? new ArrayList() : null);
    }

    @Override
    public synchronized Iterator<T> iterator() {
      // We introspect the state of the instance, in order to understand what mode it is operating in (with replay
      // or not), and how to behave accordingly (depending on whether the iterator has already been used or not).
      if (null == lazyIterator) {
        // Replay not enabled, and iterator already requested once. Fail fast.
        throw new VeniceSerializationException(this.getClass().getSimpleName() + " does not support iterating more than once.");
      } else if (null == lazyIterator.objects) {
        // Replay not enabled, and iterator not requested yet. Will return iterator for the last time.
        LazyCollectionDeserializerIterator lazyIteratorReference = this.lazyIterator;
        this.lazyIterator = null;
        return lazyIteratorReference;
      } else if (!lazyIterator.hasNext()) {
        // Replay enabled, iterator fully drained (whether it had something to begin with or not).
        return lazyIterator.objects.iterator();
      } else if (lazyIterator.objects.isEmpty()) {
        // Replay enabled, fresh iterator
        return lazyIterator;
      } else {
        // Replay enabled, but the iterator is neither fresh nor drained
        throw new VeniceSerializationException(this.getClass().getSimpleName() + " does not support concurrent iteration. "
            + "If you need to iterate more than once, you must iterate until the end before iterating again.");
      }
    }
  }

  /**
   * An {@link Iterator} which lazily decodes records from a {@link org.apache.avro.io.Decoder}, and optionally
   * stores them into a {@link List}, for future replay.
   */
  private static class LazyCollectionDeserializerIterator<T> implements Iterator<T> {
    protected final BinaryDecoder decoder;
    protected final DatumReader<T> datumReader;
    /** May be null, if replay support is disabled. */
    protected final List<T> objects;

    LazyCollectionDeserializerIterator(BinaryDecoder decoder, DatumReader<T> datumReader, List<T> objects) {
      this.decoder = decoder;
      this.datumReader = datumReader;
      this.objects = objects;
    }

    @Override
    public boolean hasNext() {
      try {
        return !decoder.isEnd();
      } catch (IOException e) {
        throw new VeniceSerializationException("Could not deserialize bytes back into Avro objects", e);
      }
    }

    @Override
    public T next() {
      try {
        if (!decoder.isEnd()) {
          T next = datumReader.read(null, decoder);
          if (objects != null) {
            objects.add(next);
          }
          return next;
        } else {
          throw new NoSuchElementException();
        }
      } catch (IOException e) {
        throw new VeniceSerializationException("Could not deserialize bytes back into Avro objects", e);
      }
    }
  }
}

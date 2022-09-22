package com.linkedin.alpini.base.safealloc;

import io.netty.buffer.ByteBuf;
import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nonnull;


/**
 * A {@linkplain PhantomReference} which holds on to a reference to a {@linkplain ByteBuf}
 * so that if the {@link SafeByteBuf} referent were to be garbage collected before the
 * object was released, it may be released by the allocator later.
 */
final class SafeReference extends PhantomReference<SafeByteBuf> {
  Object _hint;
  private ByteBuf _store;
  private final int _hashCode;

  /**
   * Creates a new phantom reference that refers to the given object and
   * is registered with the given queue.
   *
   * @param referent the object the new phantom reference will refer to
   * @param q the queue with which the reference is to be registered,
   */
  SafeReference(@Nonnull SafeByteBuf referent, @Nonnull ReferenceQueue<? super SafeByteBuf> q, @Nonnull ByteBuf store) {
    super(referent, q);
    _store = store;
    _hashCode = ThreadLocalRandom.current().nextInt();
  }

  ByteBuf store() {
    return _store;
  }

  ByteBuf store(ByteBuf store) {
    _store = store;
    return store;
  }

  int capacity() {
    return _store != null ? _store.capacity() : -1;
  }

  boolean release() {
    return _store != null && _store.release();
  }

  void touch() {
    _store.touch();
  }

  void touch(Object hint) {
    _store.touch(hint);
    _hint = hint;
  }

  @Override
  public int hashCode() {
    return _hashCode;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SafeReference that = (SafeReference) o;
    return _hashCode == that._hashCode && Objects.equals(_hint, that._hint) && _store.equals(that._store);
  }
}

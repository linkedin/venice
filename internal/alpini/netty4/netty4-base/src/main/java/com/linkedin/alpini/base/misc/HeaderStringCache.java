package com.linkedin.alpini.base.misc;

import io.netty.util.AsciiString;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class HeaderStringCache {
  private static final Logger LOG = LogManager.getLogger(HeaderStringCache.class);

  /**
   * The default initial capacity - MUST be a power of two.
   */
  private static final int DEFAULT_INITIAL_CAPACITY = 1 << 4; // aka 16

  private static final int EXPIRE_MILLISECONDS = 1000; // A + B Map == 2 seconds retention

  private static final ThreadLocal<Cache> CACHE_THREAD_LOCAL = ThreadLocal.withInitial(Cache::new);

  private HeaderStringCache() {
    // never instantiated
  }

  public static Cache getAndExpireOld() {
    Cache cache = CACHE_THREAD_LOCAL.get();
    cache.expire();
    return cache;
  }

  public static class Cache {
    private final Deque<Map<CharSequence, CharSequence>> _cache = new ArrayDeque<>(3);
    private long _expirationTime;

    private Cache() {
      _cache.add(new HashMap<>());
      _cache.add(new HashMap<>());
    }

    /**
     * Header names are cached case insensitive.
     * @param key CharSequence
     * @return cached instance
     */
    public CharSequence lookupName(CharSequence key) {
      NameKey tmpKey = new NameKey(key);
      CharSequence cachedName = _cache.peekFirst().get(tmpKey);
      if (cachedName == null) {
        cachedName = _cache.peekLast().get(tmpKey);
        if (cachedName == null) {
          cachedName = key.toString();
        }
        _cache.peekFirst().put(new NameKey(cachedName), cachedName);
      }
      return cachedName;
    }

    /**
     * Header values are cached case sensitive.
     * @param key CharSequence
     * @return cached instance
     */
    public CharSequence lookupValue(CharSequence key) {
      ValueKey tmpKey = new ValueKey(key);
      CharSequence cachedValue = _cache.peekFirst().get(tmpKey);
      if (cachedValue == null) {
        cachedValue = _cache.peekLast().get(tmpKey);
        if (cachedValue == null) {
          cachedValue = AsciiString.of(key);
        }
        _cache.peekFirst().put(new ValueKey(cachedValue), cachedValue);
      }
      return cachedValue;
    }

    void expire() {
      long now = Time.nanoTime();
      if (now >= _expirationTime) {
        _cache.addFirst(new HashMap<>(Math.max(DEFAULT_INITIAL_CAPACITY, _cache.peekFirst().size())));
        int removedSize = _cache.removeLast().size();
        LOG.debug("removing old map (size={})", removedSize);
        _expirationTime = now + TimeUnit.MILLISECONDS.toNanos(EXPIRE_MILLISECONDS);
      }
    }
  }

  private static abstract class Key implements CharSequence {
    final CharSequence _charSequence;
    private final int _hashCode;

    private Key(CharSequence charSequence, int hashCode) {
      _charSequence = charSequence;
      _hashCode = hashCode;
    }

    @Override
    public int length() {
      return _charSequence.length();
    }

    @Override
    public char charAt(int index) {
      return _charSequence.charAt(index);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
      return _charSequence.subSequence(start, end);
    }

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
      return test((Key) o);
    }

    protected abstract boolean test(Key other);

    @Override
    @Nonnull
    public String toString() {
      return _charSequence.toString();
    }
  }

  private static final class NameKey extends Key {
    private NameKey(CharSequence charSequence) {
      super(charSequence, ByteBufAsciiString.hashCode(LowerCaseAsciiCharSequence.toLowerCase(charSequence)));
    }

    protected boolean test(Key other) {
      return ByteBufAsciiString.contentEqualsIgnoreCase(_charSequence, other._charSequence);
    }
  }

  private static final class ValueKey extends Key {
    private ValueKey(CharSequence charSequence) {
      super(charSequence, ByteBufAsciiString.hashCode(charSequence));
    }

    protected boolean test(Key other) {
      return ByteBufAsciiString.contentEquals(_charSequence, other._charSequence);
    }
  }
}

package com.linkedin.alpini.base.cache;

import com.linkedin.alpini.base.misc.Time;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class ByteBufHashMap<K, V> extends AbstractMap<K, V> implements SerializedMap<K, V> {
  private static final Logger LOG = LogManager.getLogger(ByteBufHashMap.class);

  /* Default block size */
  public static final int DEFAULT_BLOCK_SIZE = 65536;

  /**
   * Serialization/Deserialization interface. A simple implemmentation is retured by
   * {@link #javaSerialization()} but users of this class may provide alternate implementations.
   * @param <V> class to serialize/deserialize
   */
  public interface SerDes<V> {
    /**
     * Perform deserialization
     * @param inputStream {@link java.io.InputStream} of bytes
     * @return Object deserialization of the presented bytes
     */
    V deserialize(@Nonnull ByteBufInputStream inputStream);

    /**
     * Perform serialization
     * @param outputStream Target {@link java.io.OutputStream}
     * @param value Object to deserialize
     * @return {@code true} if successfully deserialized
     */
    boolean serialize(@Nonnull ByteBufOutputStream outputStream, @Nonnull V value);
  }

  /**
   * Simple serialization/deserialization using Java's object serialization implementation.
   * @param <V> class to serialize/deserialize
   * @return instance of {@linkplain java.io.Serializable}
   */
  @SuppressWarnings("unchecked")
  public static <V extends java.io.Serializable> SerDes<V> javaSerialization() {
    return JavaSerialization.INSTANCE;
  }

  /** Default allocator, currently {@link PooledByteBufAllocator#buffer(int)} */
  public static final IntFunction<ByteBuf> DEFAULT_ALLOCATOR = PooledByteBufAllocator.DEFAULT::buffer;

  private final SerDes<V> _serDes;

  /** Map which maps from key to a position within a block */
  private final ConcurrentMap<K, Ref> _keyMap;

  /** Allocator for {@link ByteBuf}s */
  private final IntFunction<ByteBuf> _allocator;

  /** List of {@link ByteBuf}s which are to be purged */
  private final LinkedList<ByteBuf> _oldBuffers;

  /** Semaphore to ensure that there is only one cleanup in progress */
  private final Semaphore _cleanupSemaphore;

  /** List of blocks used in this map, in order of construction */
  private final LinkedList<Block> _blockQueue;

  /** Current block to which new entries may be appended */
  private Block _currentBlock;

  /** Size for new blocks */
  private int _blockSize;

  /** Max age for a block before it is expired, in nanos */
  private long _maxBlockAge;

  /** Max duration that a block may be appended, in nanos */
  private long _incubationAge = TimeUnit.SECONDS.toNanos(1);

  /** Maximum memory to be used for blocks before old blocks are expired, in bytes */
  private long _maxAllocatedMemory = Long.MAX_VALUE;

  /** Number of bytes currently in use for blocks in {@linkplain #_blockQueue} */
  private long _allocatedBytes;

  /** {@code true} if there are blocks awaiting cleanup */
  private boolean _cleanupRequired;

  /** transient entrySet, returned by {@link #entrySet()} */
  private transient Set<Entry<K, V>> _entrySet;

  /**
   * Equivalent to calling {@link #ByteBufHashMap(SerDes, int)} with {@link #DEFAULT_ALLOCATOR}
   * @param serDes Serialization/Deserialization interface
   */
  public ByteBufHashMap(@Nonnull SerDes<V> serDes) {
    this(serDes, DEFAULT_ALLOCATOR);
  }

  /**
   * Equivalent to calling {@link #ByteBufHashMap(SerDes, int, IntFunction)} with {@link #DEFAULT_BLOCK_SIZE}
   * @param serDes Serialization/Deserialization interface
   * @param allocator block allocator
   */
  public ByteBufHashMap(@Nonnull SerDes<V> serDes, @Nonnull IntFunction<ByteBuf> allocator) {
    this(serDes, DEFAULT_BLOCK_SIZE, allocator, ConcurrentHashMap::new);
  }

  /**
   * Equivalent to calling {@link #ByteBufHashMap(SerDes, int, IntFunction)} with {#link #DEFAULT_ALLOCATOR}
   * @param serDes Serialization/Deserialization interface
   * @param blockSize size for data blocks
   */
  public ByteBufHashMap(@Nonnull SerDes<V> serDes, int blockSize) {
    this(serDes, blockSize, DEFAULT_ALLOCATOR);
  }

  /**
   * Construct an instance of a {@link ByteBuf} backed hash map.
   * @param serDes Serialization/Deserialization interface
   * @param blockSize size for data blocks
   * @param allocator block allocator
   */
  public ByteBufHashMap(@Nonnull SerDes<V> serDes, int blockSize, @Nonnull IntFunction<ByteBuf> allocator) {
    this(serDes, blockSize, allocator, ConcurrentHashMap::new);
  }

  /**
   * Construct an instance of a {@link ByteBuf} backed hash map.
   * @param serDes Serialization/Deserialization interface
   * @param blockSize size for data blocks
   * @param allocator block allocator
   * @param initialCapacity see {@link ConcurrentHashMap#ConcurrentHashMap(int, float)}
   * @param loadFactor see {@link ConcurrentHashMap#ConcurrentHashMap(int, float)}
   */
  public ByteBufHashMap(
      @Nonnull SerDes<V> serDes,
      int blockSize,
      @Nonnull IntFunction<ByteBuf> allocator,
      int initialCapacity,
      float loadFactor) {
    this(serDes, blockSize, allocator, () -> new ConcurrentHashMap<>(initialCapacity, loadFactor));
  }

  private ByteBufHashMap(
      @Nonnull SerDes<V> serDes,
      int blockSize,
      @Nonnull IntFunction<ByteBuf> allocator,
      @Nonnull Supplier<ConcurrentMap<K, Ref>> suppier) {
    _serDes = Objects.requireNonNull(serDes, "serDes");
    setBlockSize(blockSize);
    _allocator = Objects.requireNonNull(allocator, "allocator");
    _keyMap = suppier.get();
    _cleanupSemaphore = new Semaphore(1);
    _blockQueue = new LinkedList<>();
    _oldBuffers = new LinkedList<>();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public @Nonnull ByteBufHashMap<K, V> setBlockSize(int blockSize) {
    if (blockSize < 4096) {
      throw new IllegalArgumentException("Minimum block size of 4096 bytes");
    }

    // round up to a power of 2
    blockSize--;
    blockSize |= blockSize >>> 1;
    blockSize |= blockSize >>> 2;
    blockSize |= blockSize >>> 4;
    blockSize |= blockSize >>> 8;
    blockSize |= blockSize >>> 16;
    blockSize++;

    _blockSize = blockSize;
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public @Nonnull ByteBufHashMap<K, V> setMaxBlockAge(long time, @Nonnull TimeUnit unit) {
    if (time <= 0 || time == Long.MAX_VALUE) {
      _maxBlockAge = 0;
    } else {
      _maxBlockAge = unit.toNanos(time);
    }
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public @Nonnull ByteBufHashMap<K, V> setIncubationAge(long time, @Nonnull TimeUnit unit) {
    if (time <= 0 || time == Long.MAX_VALUE) {
      _incubationAge = 0;
    } else {
      _incubationAge = unit.toNanos(time);
    }
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public @Nonnull ByteBufHashMap<K, V> setMaxAllocatedMemory(long memory) {
    if (memory < 4096) {
      throw new IllegalArgumentException("Minumum max memory size of 4096 bytes");
    }
    _maxAllocatedMemory = memory;
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getMaxBlockAge(@Nonnull TimeUnit unit) {
    if (_maxBlockAge <= 0) {
      return Long.MAX_VALUE;
    } else {
      return unit.convert(_maxBlockAge, TimeUnit.NANOSECONDS);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getIncubationAge(@Nonnull TimeUnit unit) {
    if (_incubationAge <= 0) {
      return Long.MAX_VALUE;
    } else {
      return unit.convert(_incubationAge, TimeUnit.NANOSECONDS);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getBlockSize() {
    return _blockSize;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getMaxAllocatedMemory() {
    return _maxAllocatedMemory;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getAllocatedBytes() {
    return _allocatedBytes;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isEmpty() {
    return entrySet().isEmpty();
  }

  @SuppressWarnings("unchecked")
  protected final K castKey(Object key) {
    return (K) key;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean containsKey(Object key) {
    Ref ref = _keyMap.get(castKey(key));
    return ref != null && ref._block._buffer != Unpooled.EMPTY_BUFFER;
  }

  private V getValueFromRef(Ref ref) {
    if (ref != null) {
      ByteBuf buffer = ref._block._buffer.retain();
      if (buffer != Unpooled.EMPTY_BUFFER) {
        try {
          return ref.readValue(this, buffer);
        } finally {
          buffer.release();
        }
      }
    }
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public V get(Object key) {
    return getValueFromRef(_keyMap.get(castKey(key)));
  }

  private synchronized Ref allocate(ByteBufOutputStream bbos) throws IOException {
    long now = Time.nanoTime();
    int size = bbos.writtenBytes();

    if (size >= _blockSize) {
      checkBlocksToExpire(now);
      Block block = newBlock(size);
      _blockQueue.add(block);
      return block.allocate(bbos);
    }

    Block block = _currentBlock;
    Ref ref;
    if (block == null || (_incubationAge > 0 && block._creationTime + _incubationAge < now)
        || (ref = block.allocate(bbos)) == null) { // SUPPRESS CHECKSTYLE InnerAssignment
      if (block != null) {
        checkBlocksToExpire(now);
        _blockQueue.add(block);
      }
      _currentBlock = newBlock(size);
      ref = _currentBlock.allocate(bbos);
    }
    return ref;
  }

  private Block newBlock(int size) {
    Block block = new Block(_allocator.apply(Math.max(_blockSize, size)));
    _allocatedBytes += block._buffer.capacity();
    return block;
  }

  private void checkBlocksToExpire(long now) {
    long oldest = _maxBlockAge > 0 ? now - _maxBlockAge : Long.MIN_VALUE;
    Block oldBlock;
    while ((oldBlock = _blockQueue.peekFirst()) != null // SUPPRESS CHECKSTYLE InnerAssignment
        && (oldBlock._creationTime < oldest || _allocatedBytes > _maxAllocatedMemory)) {
      if (_blockQueue.remove(oldBlock)) {
        ByteBuf buffer = oldBlock._buffer;
        oldBlock._buffer = Unpooled.EMPTY_BUFFER;
        if (buffer != Unpooled.EMPTY_BUFFER) {
          _allocatedBytes -= buffer.capacity();
          _cleanupRequired = true;
          _oldBuffers.add(buffer);
        }
      }
    }
    if (_cleanupRequired && _cleanupSemaphore.tryAcquire()) {
      _cleanupRequired = false;
      List<ByteBuf> clear = new ArrayList<>(_oldBuffers.size());
      clear.addAll(_oldBuffers);
      _oldBuffers.clear();
      CompletableFuture
          .runAsync(
              () -> _keyMap.entrySet().removeIf(entry -> entry.getValue()._block._buffer == Unpooled.EMPTY_BUFFER))
          .whenComplete((aVoid, ex) -> {
            _cleanupSemaphore.release();
            clear.forEach(ByteBuf::release);
          });
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public V put(K key, V value) {
    if (value == null) {
      return remove(key);
    }

    Ref ref = _keyMap.get(Objects.requireNonNull(key));
    Ref newRef;
    if (ref != null) {
      Ref[] newRefA = { null };
      ByteBuf buffer;
      while ((buffer = ref._block._buffer.retain()) != Unpooled.EMPTY_BUFFER) {
        try {
          Optional<Optional<V>> result = ref.put(this, key, value, buffer, newRefA);
          if (result.isPresent()) {
            return result.get().orElse(null);
          }
        } finally {
          buffer.release();
        }
        ref = _keyMap.get(key);
        if (ref == null) {
          break;
        }
      }
      newRef = newRefA[0];
    } else {
      newRef = null;
    }

    if (newRef == null) {
      ByteBuf buf = PooledByteBufAllocator.DEFAULT.heapBuffer();

      try (ByteBufOutputStream bbos = new ByteBufOutputStream(buf)) {
        _serDes.serialize(bbos, value);
        newRef = allocate(bbos);
      } catch (IOException e) {
        LOG.warn("Failed to serialize for key={}", key, e);
        throw new IllegalStateException(e);
      } finally {
        buf.release();
      }
    }

    return getValueFromRef(_keyMap.put(key, newRef));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public V remove(Object key) {
    return getValueFromRef(_keyMap.remove(castKey(key)));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean removeEntry(K key) {
    Ref ref = _keyMap.remove(key);
    return ref != null && ref._block._buffer != Unpooled.EMPTY_BUFFER;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @Nonnull
  public Set<K> keySet() {
    return _keyMap.keySet();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @Nonnull
  public Set<Entry<K, V>> entrySet() {
    if (_entrySet == null) {
      _entrySet = new AbstractSet<Entry<K, V>>() {
        @Override
        @Nonnull
        public Iterator<Entry<K, V>> iterator() {
          Iterator<Entry<K, Ref>> it = _keyMap.entrySet().iterator();
          return new Iterator<Entry<K, V>>() {
            Entry<K, Ref> _nextEntry;
            Entry<K, Ref> _prevEntry;
            @Nonnull
            ByteBuf _nextBuffer = Unpooled.EMPTY_BUFFER;

            @Override
            protected void finalize() throws Throwable {
              _nextBuffer.release();
              super.finalize();
            }

            @Override
            public boolean hasNext() {
              while (_nextEntry == null) {
                if (!it.hasNext()) {
                  return false;
                }
                _nextEntry = it.next();
                _nextBuffer.release();
                _nextBuffer = _nextEntry.getValue()._block._buffer.retain();
                if (_nextBuffer == Unpooled.EMPTY_BUFFER) {
                  _nextEntry = null;
                }
              }
              return true;
            }

            @Override
            public Entry<K, V> next() {
              if (!hasNext()) {
                throw new IndexOutOfBoundsException();
              }
              Entry<K, Ref> entry = _nextEntry;
              return new Entry<K, V>() {
                private V value;
                private ByteBuf buffer = _nextBuffer.retain();
                {
                  _prevEntry = entry;
                  _nextEntry = null;
                  _nextBuffer = Unpooled.EMPTY_BUFFER;
                }

                @Override
                protected void finalize() throws Throwable {
                  buffer.release();
                  super.finalize();
                }

                @Override
                public K getKey() {
                  return entry.getKey();
                }

                @Override
                public V getValue() {
                  V value = this.value;
                  if (value == null) {
                    value = entry.getValue().getAndSet(ByteBufHashMap.this, buffer, v -> this.value = v);
                    buffer.release();
                    buffer = Unpooled.EMPTY_BUFFER;
                  }
                  return value;
                }

                @Override
                public V setValue(V value) {
                  if (value == null) {
                    entry.getValue()._length = 0;
                    this.value = null;
                    return ByteBufHashMap.this.remove(entry.getKey());
                  }
                  this.value = value;
                  return ByteBufHashMap.this.put(entry.getKey(), value);
                }

                @Override
                public boolean equals(Object other) {
                  return other == this;
                }

                @Override
                public int hashCode() {
                  int hashCode = getKey().hashCode();
                  if (value != null) {
                    return hashCode * 31 + value.hashCode();
                  } else {
                    return hashCode * 31 + entry.getValue().hashCode();
                  }
                }
              };
            }

            @Override
            public void remove() {
              it.remove();
            }
          };
        }

        @Override
        public boolean isEmpty() {
          return _keyMap.isEmpty();
        }

        @Override
        public int size() {
          return _keyMap.size();
        }

        @Override
        public void clear() {
          if (_currentBlock != null) {
            _blockQueue.add(_currentBlock);
            _blockQueue.removeIf(block -> _oldBuffers.add(block._buffer));
            _blockQueue.clear();
            _keyMap.clear();
            _cleanupRequired = true;
            checkBlocksToExpire(Time.nanoTime());
            _currentBlock = null;
          }
        }
      };
    }
    return _entrySet;
  }

  private static class Block {
    private final long _creationTime = Time.nanoTime();
    private @Nonnull ByteBuf _buffer;

    private Block(ByteBuf buffer) {
      _buffer = buffer;
    }

    private synchronized Ref allocate(ByteBufOutputStream bbos) throws IOException {
      int size = bbos.writtenBytes();
      if (_buffer == Unpooled.EMPTY_BUFFER || _buffer.writerIndex() + size > _buffer.capacity()) {
        return null;
      }

      int pos = _buffer.writerIndex();
      _buffer.writeBytes(bbos.buffer());
      return new Ref(this, pos, bbos.writtenBytes());
    }
  }

  private static class Ref {
    private final Block _block;
    private final int _offset;
    private final int _allocated;
    private int _length;

    private Ref(Block block, int offset, int allocated) {
      _block = block;
      _offset = offset;
      _allocated = allocated;
      _length = allocated;
    }

    private synchronized <K, V> V readValue(ByteBufHashMap<K, V> map, ByteBuf buffer) {
      return _length > 0 ? map._serDes.deserialize(new ByteBufInputStream(buffer.slice(_offset, _length))) : null;
    }

    public synchronized <K, V> V getAndSet(ByteBufHashMap<K, V> map, ByteBuf buffer, Consumer<V> consumer) {
      V value = readValue(map, buffer);
      consumer.accept(value);
      return value;
    }

    public synchronized <K, V> Optional<Optional<V>> put(
        ByteBufHashMap<K, V> map,
        K key,
        V value,
        ByteBuf buffer,
        Ref[] newRef) {
      V oldValue;
      if (_length > 0) {
        oldValue = readValue(map, buffer);

        try (ByteBufOutputStream bbos = new ByteBufOutputStream(_block._buffer.slice(_offset, _allocated))) {
          if (map._serDes.serialize(bbos, value)) {
            _length = bbos.writtenBytes();
            return Optional.of(Optional.ofNullable(oldValue));
          }
        } catch (Exception e) {
          LOG.debug("Failed to serialize into available space", e);
        }
        _length = 0;
      } else {
        oldValue = null;
      }

      if (newRef[0] == null) {
        ByteBuf buf = map._allocator.apply(map._blockSize);
        try (ByteBufOutputStream bbos = new ByteBufOutputStream(buf)) {
          if (map._serDes.serialize(bbos, value)) {
            newRef[0] = _block.allocate(bbos);
            if (newRef[0] == null) {
              newRef[0] = map.allocate(bbos);
            }
          } else {
            LOG.warn("Failed to serialize into available space");
          }
        } catch (Exception e) {
          LOG.warn("Failed to serialize into available space", e);
          return Optional.empty();
        } finally {
          buf.release();
        }
      }

      if (newRef[0] != null && map._keyMap.replace(key, this, newRef[0])) {
        return Optional.of(Optional.ofNullable(oldValue));
      }
      return Optional.empty();
    }
  }

  private static class JavaSerialization<V extends java.io.Serializable> implements SerDes<V> {
    private static final SerDes INSTANCE = new JavaSerialization<>();

    @Override
    @SuppressWarnings("unchecked")
    public V deserialize(@Nonnull ByteBufInputStream byteBuf) {
      try (java.io.ObjectInputStream in = new java.io.ObjectInputStream(byteBuf)) {
        return (V) in.readObject();
      } catch (Exception e) {
        LOG.warn("Failed to deserialize", e);
      }
      return null;
    }

    @Override
    public boolean serialize(@Nonnull ByteBufOutputStream outputStream, @Nonnull V value) {
      try (java.io.ObjectOutputStream out = new java.io.ObjectOutputStream(outputStream)) {
        out.writeObject(value);
        return true;
      } catch (Exception e) {
        LOG.debug("Failed to serialize", e);
      }
      return false;
    }
  }
}

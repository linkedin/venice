package com.linkedin.alpini.base.misc;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public enum CollectionUtil {
  SINGLETON; // Effective Java, Item 3 - Enum singleton

  public static boolean isNotEmpty(Collection<?> collection) {
    return collection != null && !collection.isEmpty();
  }

  public static <T> Set<T> setOf(T value) {
    return Collections.singleton(value);
  }

  @SafeVarargs
  public static <T> Set<T> setOf(T... values) {
    if (values == null || values.length == 0) {
      return Collections.emptySet();
    } else if (values.length == 1) {
      return setOf(values[0]);
    } else {
      return setOf(Arrays.asList(values));
    }
  }

  public static <T> Set<T> setOf(Collection<T> values) {
    if (values == null || values.isEmpty()) {
      return Collections.emptySet();
    } else {
      return Collections.unmodifiableSet(new HashSet<>(values));
    }
  }

  public static <T> List<T> listOf(T value) {
    return Collections.singletonList(value);
  }

  @SafeVarargs
  public static <T> List<T> listOf(T... values) {
    if (values == null || values.length == 0) {
      return Collections.emptyList();
    } else if (values.length == 1) {
      return listOf(values[0]);
    } else {
      return listOf(Arrays.asList(values));
    }
  }

  public static <T> List<T> listOf(Collection<T> values) {
    if (values == null || values.isEmpty()) {
      return Collections.emptyList();
    } else {
      return Collections.unmodifiableList(new ArrayList<>(values));
    }
  }

  public static <K, V> Map<K, V> mapOf(K key, V value) {
    return Collections.singletonMap(key, value);
  }

  @SafeVarargs
  public static <K, V> Map<K, V> mapOfEntries(ImmutableMapEntry<K, V>... values) {
    if (values == null || values.length == 0) {
      return Collections.emptyMap();
    } else if (values.length == 1) {
      return mapOf(values[0].getKey(), values[0].getValue());
    } else {
      return mapOfEntries(setOf(values));
    }
  }

  private static <K, V> Map<K, V> mapOfEntries(Set<Map.Entry<K, V>> entries) {
    return Collections.unmodifiableMap(new HashMap<>(new AbstractMap<K, V>() {
      @Override
      @Nonnull
      public Set<Entry<K, V>> entrySet() {
        return entries;
      }
    }));
  }

  public static <K, V> Map<K, V> mapOf(K key1, V value1, K key2, V value2) {
    return mapOfEntries(ImmutableMapEntry.make(key1, value1), ImmutableMapEntry.make(key2, value2));
  }

  public static <K, V> Map<K, V> mapOf(K key1, V value1, K key2, V value2, K key3, V value3) {
    return mapOfEntries(
        ImmutableMapEntry.make(key1, value1),
        ImmutableMapEntry.make(key2, value2),
        ImmutableMapEntry.make(key3, value3));
  }

  public static <K, V> Map<K, V> mapOf(K key1, V value1, K key2, V value2, K key3, V value3, K key4, V value4) {
    return mapOfEntries(
        ImmutableMapEntry.make(key1, value1),
        ImmutableMapEntry.make(key2, value2),
        ImmutableMapEntry.make(key3, value3),
        ImmutableMapEntry.make(key4, value4));
  }

  public static <T> Collector<T, ?, List<List<T>>> batchOf(int size) {
    return new BatchCollector<>(size);
  }

  private static class BatchCollector<T> implements Collector<T, LinkedList<List<T>>, List<List<T>>> {
    private final int _batchSize;

    private BatchCollector(int batchSize) {
      Preconditions.checkState(batchSize > 0);
      _batchSize = batchSize;
    }

    @Override
    public Supplier<LinkedList<List<T>>> supplier() {
      return LinkedList::new;
    }

    @Override
    public BiConsumer<LinkedList<List<T>>, T> accumulator() {
      return this::accumulate;
    }

    private void accumulate(LinkedList<List<T>> list, T element) {
      if (list.isEmpty() || list.getLast().size() >= _batchSize) {
        list.addLast(new ArrayList<>(_batchSize));
      }
      list.getLast().add(Objects.requireNonNull(element));
    }

    @Override
    public BinaryOperator<LinkedList<List<T>>> combiner() {
      return this::combine;
    }

    private LinkedList<List<T>> combine(LinkedList<List<T>> a, LinkedList<List<T>> b) {
      LinkedList<List<T>> result = new LinkedList<>();
      result.addAll(a);
      result.addAll(b);
      return result;
    }

    @Override
    public Function<LinkedList<List<T>>, List<List<T>>> finisher() {
      return builders -> builders;
    }

    @Override
    public Set<Characteristics> characteristics() {
      return Collections.singleton(Characteristics.IDENTITY_FINISH);
    }
  }

  public static <T> SetBuilder<T> setBuilder() {
    return new SetBuilder<>();
  }

  public static <T> ListBuilder<T> listBuilder() {
    return new ListBuilder<>();
  }

  public static <K, V> MapBuilder<K, V> mapBuilder() {
    return new MapBuilder<>();
  }

  private static class CollectionBuilder<T, B extends CollectionBuilder<T, B>> {
    protected LinkedList<T> _list = new LinkedList<>();

    private CollectionBuilder() {
    }

    @SuppressWarnings("unchecked")
    protected B self() {
      return (B) this;
    }

    public B add(@Nonnull T value) {
      _list.add(Objects.requireNonNull(value));
      return self();
    }

    @SafeVarargs
    public final B add(T... values) {
      return addAll(Arrays.asList(values));
    }

    public B addAll(Collection<? extends T> values) {
      _list.addAll(values);
      return self();
    }

    public B addAll(Iterator<? extends T> values) {
      values.forEachRemaining(this::add);
      return self();
    }
  }

  public static final class SetBuilder<T> extends CollectionBuilder<T, SetBuilder<T>> {
    private SetBuilder() {
    }

    public Set<T> build() {
      return setOf(_list);
    }
  }

  public static final class ListBuilder<T> extends CollectionBuilder<T, ListBuilder<T>> {
    private ListBuilder() {
    }

    public List<T> build() {
      return listOf(_list);
    }
  }

  public static final class MapBuilder<K, V> extends CollectionBuilder<ImmutableMapEntry<K, V>, MapBuilder<K, V>> {
    private MapBuilder() {
    }

    public MapBuilder<K, V> put(K key, V value) {
      return super.add(ImmutableMapEntry.make(key, value));
    }

    public MapBuilder<K, V> putAll(Map<K, V> values) {
      return super.addAll(values.entrySet().stream().map(ImmutableMapEntry::make).iterator());
    }

    @SuppressWarnings("unchecked")
    public Map<K, V> build() {
      List<Map.Entry<K, V>> list = (List<Map.Entry<K, V>>) (List) this._list;
      return mapOfEntries(setOf(list));
    }
  }

  @SuppressWarnings("unchecked")
  public static <T> Stream<T> stream(Spliterator<? extends T> spliterator, boolean parallel) {
    return (Stream<T>) StreamSupport.stream(spliterator, parallel);
  }

  public static <T> Stream<T> stream(Iterable<? extends T> iterable) {
    return stream(iterable.spliterator(), false);
  }

  public static <T> Stream<T> stream(Iterator<? extends T> iterator) {
    return stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false);
  }

  @SafeVarargs
  public static <T> Stream<T> stream(T... source) {
    return source != null && source.length > 0 ? Arrays.stream(source) : Stream.empty();
  }

  @SuppressWarnings("unchecked")
  @SafeVarargs
  public static <T> Iterable<T> concat(Iterable<? extends T>... sources) {
    return sources != null && sources.length > 0
        ? () -> stream(sources).flatMap((Function<Iterable<? extends T>, Stream<T>>) CollectionUtil::stream).iterator()
        : Collections.emptySet();
  }

  @SafeVarargs
  public static <T> Iterator<T> concat(Iterator<? extends T>... sources) {
    return sources != null && sources.length > 0
        ? stream(sources).flatMap(CollectionUtil::<T>stream).iterator()
        : Collections.emptyIterator();
  }

  @SafeVarargs
  public static <T> Object deepToStringObject(T... array) {
    return new Object() {
      public final String toString() {
        return Arrays.deepToString(array);
      }
    };
  }

  public static <T> T applyFactoryModifiers(T pipelineFactory, List<Function<T, T>> modifiers) {
    return modifiers.stream()
        .collect(() -> pipelineFactory, (factory, modifier) -> modifier.apply(factory), (ignore1, ignore2) -> {});
  }

  /**
   * ComputeIfAbsent on a ConcurrentHashMap should be a very light weight operation if the key already exists.
   * Due to a bug in Java 8 (https://bugs.openjdk.java.net/browse/JDK-8161372), a lock is acquired regardless.
   * This Util rewrites the computeIfAbsent as get and putIfAbsent to avoid the lock if the key
   * already exists.
   *
   * @author Abhishek Andhavarapu &lt;aandhava@linkedin.com&gt;
   */
  public static <K, V> V computeIfAbsent(
      @Nonnull ConcurrentMap<K, V> map,
      @Nonnull K key,
      @Nonnull Function<? super K, ? extends V> mappingFunction) {
    V value = map.get(key);
    if (value == null) {
      value = mappingFunction.apply(key);
      if (value != null) {
        V existing = map.putIfAbsent(key, value);
        if (existing != null) {
          value = existing;
        }
      }
    }
    return value;
  }

  /**
   * Accepts a mapping function to compute the value, and only register when the old value is not set.
   */

  public static <K, V> V computeIfAbsent(
      @Nonnull ConcurrentMap<K, V> map,
      @Nonnull K key,
      @Nonnull Function<? super K, ? extends V> mappingFunction,
      BiConsumer<K, V> register) {
    V value = map.get(key);
    if (value == null) {
      value = mappingFunction.apply(key);
      if (value != null) {
        // Returns the old value
        V existing = map.putIfAbsent(key, value);
        if (existing != null) {
          value = existing;
        } else {
          // Run the register only when previous value is empty.
          register.accept(key, value);
        }
      }
    }
    return value;
  }

  /**
   * Clones an array with arbitrary levels of nesting.
   */
  public static <V> V[] deepCloneArray(@Nonnull V[] array) {
    // noinspection rawtypes
    return deepCloneArray(array, new IdentityHashMap());
  }

  @SuppressWarnings("rawtypes")
  private static <V> V[] deepCloneArray(@Nonnull V[] array, IdentityHashMap cloneMapRaw) {
    @SuppressWarnings("unchecked")
    IdentityHashMap<V[], V[]> cloneMap = (IdentityHashMap<V[], V[]>) cloneMapRaw;
    V[] clone = cloneMap.get(array);
    if (clone == null) {
      clone = array.clone();
      cloneMap.put(array, clone);
      for (int i = clone.length - 1; i >= 0; i--) {
        V value = clone[i];
        if (value == null || !value.getClass().isArray()) {
          continue;
        }
        if (value.getClass().getComponentType().isPrimitive()) {
          // noinspection unchecked
          clone[i] = (V) cloneMapRaw.computeIfAbsent(value, v -> {
            switch (v.getClass().getComponentType().getSimpleName()) {
              case "boolean":
                return ((boolean[]) v).clone();
              case "byte":
                return ((byte[]) v).clone();
              case "short":
                return ((short[]) v).clone();
              case "char":
                return ((char[]) v).clone();
              case "int":
                return ((int[]) v).clone();
              case "float":
                return ((float[]) v).clone();
              case "long":
                return ((long[]) v).clone();
              case "double":
                return ((double[]) v).clone();
              default:
                assert false;
                return null;
            }
          });
          continue;
        }
        // noinspection unchecked
        clone[i] = (V) deepCloneArray((Object[]) value, cloneMapRaw);
      }
    }
    return clone;
  }
}

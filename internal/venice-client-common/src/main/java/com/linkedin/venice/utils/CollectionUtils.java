package com.linkedin.venice.utils;

import com.linkedin.alpini.base.misc.ImmutableMapEntry;
import java.util.AbstractList;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.RandomAccess;
import java.util.Set;
import javax.annotation.Nonnull;


public class CollectionUtils {
  /**
   * A manual implementation of list equality.
   *
   * This is (unfortunately) useful with Avro lists since they do not work reliably.
   * There are cases where a {@link List<T>} coming out of an Avro record will be
   * implemented as a {@link org.apache.avro.generic.GenericData.Array} and other
   * times it will be a java {@link ArrayList}. When this happens, the equality check
   * fails...
   *
   * @return true if both lists have the same items in the same order
   */
  public static <T> boolean listEquals(List<T> list1, List<T> list2) {
    if (list1.size() != list2.size()) {
      return false;
    } else {
      for (int i = 0; i < list2.size(); i++) {
        if (!list1.get(i).equals(list2.get(i))) {
          return false;
        }
      }
    }
    return true;
  }

  public static List<Float> asUnmodifiableList(float[] array) {
    Objects.requireNonNull(array);
    class ResultList extends AbstractList<Float> implements RandomAccess {
      @Override
      public Float get(int index) {
        return array[index];
      }

      @Override
      public int size() {
        return array.length;
      }
    }
    ;
    return new ResultList();
  }

  /**
   * A reversed copy of the given collection
   *
   * @param <T> The type of the items in the list
   * @param collection The collection to reverse
   * @return The list, reversed
   */
  public static <T> List<T> reversed(Collection<T> collection) {
    List<T> copy = new ArrayList<>(collection);
    Collections.reverse(copy);
    return copy;
  }

  public static Map<CharSequence, CharSequence> getCharSequenceMapFromStringMap(Map<String, String> stringStringMap) {
    return new HashMap<>(stringStringMap);
  }

  public static Map<String, CharSequence> getStringKeyCharSequenceValueMapFromStringMap(Map<String, String> stringMap) {
    if (stringMap == null) {
      return null;
    }
    Map<String, CharSequence> scMap = new HashMap<>();
    scMap.putAll(stringMap);
    return scMap;
  }

  public static Map<String, String> getStringMapFromCharSequenceMap(Map<CharSequence, CharSequence> charSequenceMap) {
    if (charSequenceMap == null) {
      return null;
    }

    Map<String, String> ssMap = new HashMap<>();
    charSequenceMap.forEach((key, value) -> ssMap.put(key.toString(), value.toString()));
    return ssMap;
  }

  public static Map<CharSequence, CharSequence> convertStringMapToCharSequenceMap(Map<String, String> stringMap) {
    Map<CharSequence, CharSequence> res = new HashMap<>();
    stringMap.forEach((k, v) -> res.put(k, v));
    return res;
  }

  public static Map<String, String> convertCharSequenceMapToStringMap(Map<CharSequence, CharSequence> charSequenceMap) {
    Map<String, String> res = new HashMap<>();
    charSequenceMap.forEach((k, v) -> res.put(k.toString(), v.toString()));
    return res;
  }

  /**
   * This function can be useful when we want:
   * - To ensure a map is not null.
   * - To avoid hanging on to many references of empty maps (as opposed to the singleton {@link Collections#EMPTY_MAP}).
   *
   * @param map to be returned if populated, or substituted if null or empty
   * @return a non-null map with the same content (though not necessarily the same identity) as the input map
   */
  public static <K, V> Map<K, V> substituteEmptyMap(Map<K, V> map) {
    return map == null || map.isEmpty() ? Collections.emptyMap() : map;
  }

  public static <K, V> MapBuilder<K, V> mapBuilder() {
    return new MapBuilder<>();
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

  private static <K, V> Map<K, V> mapOfEntries(Set<Map.Entry<K, V>> entries) {
    return Collections.unmodifiableMap(new HashMap<>(new AbstractMap<K, V>() {
      @Override
      @Nonnull
      public Set<Entry<K, V>> entrySet() {
        return entries;
      }
    }));
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
}

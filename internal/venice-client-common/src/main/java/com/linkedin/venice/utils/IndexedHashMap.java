package com.linkedin.venice.utils;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;


/**
 * This is a fork of the standard {@link java.util.HashMap} which implements
 * {@link IndexedMap}. It is composed of an {@link ArrayList} to keep
 * track of the insertion & iteration order of the entries. The
 * performance of the various operations should be as follows:
 *
 * - The get and put performance should be constant-time, like HashMap.
 * - The iteration performance should be equal or slightly better than
 *   HashMap, since a regular map needs to weed through some empty nodes
 *   which are part of the map's internal structure.
 * - The delete performance should be worse than that of the HashMap,
 *   since it needs to do extra bookkeeping to keep the list and map
 *   nodes in sync, with a worse case of O(N).
 * - The indexOf(key) should be constant-time, like HashMap's get.
 * - The putByIndex should be O(N) in the worst case.
 * - The getByIndex should be O(1).
 * - The removeByIndex should be O(N) in the worst case.
 * - The moveElement should be O(N) in the worst case.
 *
 * Compared to the standard HashMap, this implementation is a bit
 * simplified since it doesn't attempt to provide support to the
 * {@link java.util.LinkedHashMap}, doesn't support cloning, nor Java
 * serialization.
 *
 * <p><strong>Note that this implementation is not synchronized.</strong>
 * If multiple threads access a hash map concurrently, and at least one of
 * the threads modifies the map structurally, it <i>must</i> be
 * synchronized externally.  (A structural modification is any operation
 * that adds or deletes one or more mappings; merely changing the value
 * associated with a key that an instance already contains is not a
 * structural modification.)  This is typically accomplished by
 * synchronizing on some object that naturally encapsulates the map.
 *
 * If no such object exists, the map should be "wrapped" using the
 * {@link Collections#synchronizedMap Collections.synchronizedMap}
 * method.  This is best done at creation time, to prevent accidental
 * unsynchronized access to the map:<pre>
 *   Map m = Collections.synchronizedMap(new IndexedHashMap(...));</pre>
 *
 * <p>The iterators returned by all of this class's "collection view methods"
 * are <i>fail-fast</i>: if the map is structurally modified at any time after
 * the iterator is created, in any way except through the iterator's own
 * <tt>remove</tt> method, the iterator will throw a
 * {@link ConcurrentModificationException}.  Thus, in the face of concurrent
 * modification, the iterator fails quickly and cleanly, rather than risking
 * arbitrary, non-deterministic behavior at an undetermined time in the
 * future.
 *
 * <p>Note that the fail-fast behavior of an iterator cannot be guaranteed
 * as it is, generally speaking, impossible to make any hard guarantees in the
 * presence of unsynchronized concurrent modification.  Fail-fast iterators
 * throw <tt>ConcurrentModificationException</tt> on a best-effort basis.
 * Therefore, it would be wrong to write a program that depended on this
 * exception for its correctness: <i>the fail-fast behavior of iterators
 * should be used only to detect bugs.</i>
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 */
public class IndexedHashMap<K, V> extends AbstractMap<K, V> implements IndexedMap<K, V> {
  /**
   * The default initial capacity - MUST be a power of two.
   */
  static final int DEFAULT_INITIAL_CAPACITY = 1 << 4; // aka 16

  /**
   * The maximum capacity, used if a higher value is implicitly specified
   * by either of the constructors with arguments.
   * MUST be a power of two <= 1<<30.
   */
  static final int MAXIMUM_CAPACITY = 1 << 30;

  /**
   * The load factor used when none specified in constructor.
   */
  static final float DEFAULT_LOAD_FACTOR = 0.75f;

  /**
   * The bin count threshold for using a tree rather than list for a
   * bin.  Bins are converted to trees when adding an element to a
   * bin with at least this many nodes. The value must be greater
   * than 2 and should be at least 8 to mesh with assumptions in
   * tree removal about conversion back to plain bins upon
   * shrinkage.
   */
  static final int TREEIFY_THRESHOLD = 8;

  /**
   * The bin count threshold for untreeifying a (split) bin during a
   * resize operation. Should be less than TREEIFY_THRESHOLD, and at
   * most 6 to mesh with shrinkage detection under removal.
   */
  static final int UNTREEIFY_THRESHOLD = 6;

  /**
   * The smallest table capacity for which bins may be treeified.
   * (Otherwise the table is resized if too many nodes in a bin.)
   * Should be at least 4 * TREEIFY_THRESHOLD to avoid conflicts
   * between resizing and treeification thresholds.
   */
  static final int MIN_TREEIFY_CAPACITY = 64;

  /**
   * Sentinel value used to indicate that we're not inserting an
   * entry at a specific index.
   */
  static final int APPEND_TO_END_OF_LIST = -1;

  /**
   * Basic hash bin node, used for most entries.  (See below for
   * TreeNode subclass.)
   */
  static class Node<K, V> implements Map.Entry<K, V> {
    final int hash;
    final K key;
    V value;
    Node<K, V> next;
    int index;

    Node(int hash, K key, V value, Node<K, V> next, int index) {
      this.hash = hash;
      this.key = key;
      this.value = value;
      this.next = next;
      this.index = index;
    }

    public final K getKey() {
      return key;
    }

    public final V getValue() {
      return value;
    }

    public final String toString() {
      return key + "=" + value;
    }

    public final int hashCode() {
      return Objects.hashCode(key) ^ Objects.hashCode(value);
    }

    public final V setValue(V newValue) {
      V oldValue = value;
      value = newValue;
      return oldValue;
    }

    public final boolean equals(Object o) {
      if (o == this)
        return true;
      if (o instanceof Map.Entry) {
        Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
        if (Objects.equals(key, e.getKey()) && Objects.equals(value, e.getValue()))
          return true;
      }
      return false;
    }
  }

  /* ---------------- Static utilities -------------- */

  /**
   * Computes key.hashCode() and spreads (XORs) higher bits of hash
   * to lower.  Because the table uses power-of-two masking, sets of
   * hashes that vary only in bits above the current mask will
   * always collide. (Among known examples are sets of Float keys
   * holding consecutive whole numbers in small tables.)  So we
   * apply a transform that spreads the impact of higher bits
   * downward. There is a tradeoff between speed, utility, and
   * quality of bit-spreading. Because many common sets of hashes
   * are already reasonably distributed (so don't benefit from
   * spreading), and because we use trees to handle large sets of
   * collisions in bins, we just XOR some shifted bits in the
   * cheapest possible way to reduce systematic lossage, as well as
   * to incorporate impact of the highest bits that would otherwise
   * never be used in index calculations because of table bounds.
   */
  static final int hash(Object key) {
    int h;
    return (key == null) ? 0 : (h = key.hashCode()) ^ (h >>> 16);
  }

  /**
   * Returns x's Class if it is of the form "class C implements
   * Comparable<C>", else null.
   */
  static Class<?> comparableClassFor(Object x) {
    if (x instanceof Comparable) {
      Class<?> c;
      Type[] ts, as;
      Type t;
      ParameterizedType p;
      if ((c = x.getClass()) == String.class) // bypass checks
        return c;
      if ((ts = c.getGenericInterfaces()) != null) {
        for (int i = 0; i < ts.length; ++i) {
          if (((t = ts[i]) instanceof ParameterizedType)
              && ((p = (ParameterizedType) t).getRawType() == Comparable.class)
              && (as = p.getActualTypeArguments()) != null && as.length == 1 && as[0] == c) // type arg is c
            return c;
        }
      }
    }
    return null;
  }

  /**
   * Returns k.compareTo(x) if x matches kc (k's screened comparable
   * class), else 0.
   */
  @SuppressWarnings({ "rawtypes", "unchecked" }) // for cast to Comparable
  static int compareComparables(Class<?> kc, Object k, Object x) {
    return (x == null || x.getClass() != kc ? 0 : ((Comparable) k).compareTo(x));
  }

  /**
   * Returns a power of two size for the given target capacity.
   */
  static final int tableSizeFor(int cap) {
    int n = cap - 1;
    n |= n >>> 1;
    n |= n >>> 2;
    n |= n >>> 4;
    n |= n >>> 8;
    n |= n >>> 16;
    return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
  }

  /* ---------------- Fields -------------- */

  /**
   * The table, initialized on first use, and resized as
   * necessary. When allocated, length is always a power of two.
   * (We also tolerate length zero in some operations to allow
   * bootstrapping mechanics that are currently not needed.)
   */
  transient Node<K, V>[] table;

  /**
   * Holds a pointer to each entries in the map, in the order they
   * were inserted (or potentially in the order they were arranged,
   * if {@link #putByIndex(Object, Object, int)} or {@link #moveElement(int, int)}
   * were used).
   */
  List<Map.Entry<K, V>> entryList;

  /**
   * Holds cached entrySet(). Note that AbstractMap fields are used
   * for keySet() and values().
   */
  transient Set<Entry<K, V>> entrySet;

  /**
   * The number of key-value mappings contained in this map.
   */
  transient int size;

  /**
   * The number of times this IndexedHashMap has been structurally modified
   * Structural modifications are those that change the number of mappings in
   * the IndexedHashMap or otherwise modify its internal structure (e.g.,
   * rehash).  This field is used to make iterators on Collection-views of
   * the IndexedHashMap fail-fast.  (See ConcurrentModificationException).
   */
  transient int modCount;

  /**
   * The next size value at which to resize (capacity * load factor).
   *
   * @serial
   */
  // (The javadoc description is true upon serialization.
  // Additionally, if the table array has not been allocated, this
  // field holds the initial array capacity, or zero signifying
  // DEFAULT_INITIAL_CAPACITY.)
  int threshold;

  /**
   * The load factor for the hash table.
   *
   * @serial
   */
  final float loadFactor;

  // Views

  /**
   * Each of these fields are initialized to contain an instance of the
   * appropriate view the first time this view is requested.  The views are
   * stateless, so there's no reason to create more than one of each.
   *
   * <p>Since there is no synchronization performed while accessing these fields,
   * it is expected that java.util.Map view classes using these fields have
   * no non-final fields (or any fields at all except for outer-this). Adhering
   * to this rule would make the races on these fields benign.
   *
   * <p>It is also imperative that implementations read the field only once,
   * as in:
   *
   * <pre> {@code
   * public Set<K> keySet() {
   *   Set<K> ks = keySet;  // single racy read
   *   if (ks == null) {
   *     ks = new KeySet();
   *     keySet = ks;
   *   }
   *   return ks;
   * }
   *}</pre>
   */
  transient Set<K> keySet;
  transient Collection<V> values;

  /* ---------------- Public operations -------------- */

  /**
   * Constructs an empty {@link IndexedHashMap} using the provided list implementation.
   *
   * @param  initialCapacity the initial capacity
   * @param  loadFactor      the load factor
   * @param  entryList       the list which backs the iteration functionality of the map
   * @throws IllegalArgumentException if the initial capacity is negative
   *         or the load factor is nonpositive
   */
  public IndexedHashMap(int initialCapacity, float loadFactor, List<Map.Entry<K, V>> entryList) {
    if (initialCapacity < 0)
      throw new IllegalArgumentException("Illegal initial capacity: " + initialCapacity);
    if (initialCapacity > MAXIMUM_CAPACITY)
      initialCapacity = MAXIMUM_CAPACITY;
    if (loadFactor <= 0 || Float.isNaN(loadFactor))
      throw new IllegalArgumentException("Illegal load factor: " + loadFactor);
    this.loadFactor = loadFactor;
    this.threshold = tableSizeFor(initialCapacity);
    this.entryList = entryList;
  }

  /**
   * Constructs an empty <tt>IndexedHashMap</tt> with the specified initial
   * capacity and load factor.
   *
   * @param  initialCapacity the initial capacity
   * @param  loadFactor      the load factor
   * @throws IllegalArgumentException if the initial capacity is negative
   *         or the load factor is nonpositive
   */
  public IndexedHashMap(int initialCapacity, float loadFactor) {
    this(initialCapacity, loadFactor, new ArrayList<>(initialCapacity));
  }

  /**
   * Constructs an empty <tt>IndexedHashMap</tt> with the specified initial
   * capacity and the default load factor (0.75).
   *
   * @param  initialCapacity the initial capacity.
   * @throws IllegalArgumentException if the initial capacity is negative.
   */
  public IndexedHashMap(int initialCapacity) {
    this(initialCapacity, DEFAULT_LOAD_FACTOR);
  }

  /**
   * Constructs an empty <tt>IndexedHashMap</tt> with the default initial capacity
   * (16) and the default load factor (0.75).
   */
  public IndexedHashMap() {
    this(DEFAULT_INITIAL_CAPACITY);
  }

  /**
   * Constructs a new <tt>IndexedHashMap</tt> with the same mappings as the
   * specified <tt>Map</tt>.
   *
   * @see     {@link #IndexedHashMap(Map, List)}
   * @param   m the map whose mappings are to be placed in this map
   * @throws  NullPointerException if the specified map is null
   */
  public IndexedHashMap(Map<? extends K, ? extends V> m) {
    this(m, new ArrayList<>(m.size()));
  }

  /**
   * Constructs a new <tt>IndexedHashMap</tt> with the same mappings as the
   * specified <tt>Map</tt>.  The <tt>IndexedHashMap</tt> is created with
   * default load factor (0.75) and an initial capacity sufficient to
   * hold the mappings in the specified <tt>Map</tt>.
   *
   * The ordering of the elements in the {@link IndexedHashMap} will be the
   * same as the one provided by the {@link Map#entrySet()} of the passed in
   * map. Depending on the implementation of the passed in map, that ordering
   * may or may not be deterministic. If the passed in map later undergoes
   * structural changes, the {@link IndexedHashMap} will still maintain the
   * same initial ordering.
   *
   * @param   m the map whose mappings are to be placed in this map
   * @throws  NullPointerException if the specified map is null
   */
  public IndexedHashMap(Map<? extends K, ? extends V> m, List<Map.Entry<K, V>> entryList) {
    this(m.size(), DEFAULT_LOAD_FACTOR, entryList);
    putMapEntries(m, false);
  }

  /**
   * Implements Map.putAll and Map constructor.
   *
   * @param m the map
   * @param evict false when initially constructing this map, else
   * true (relayed to method afterNodeInsertion).
   */
  final void putMapEntries(Map<? extends K, ? extends V> m, boolean evict) {
    int s = m.size();
    if (s > 0) {
      if (table == null) { // pre-size
        float ft = ((float) s / loadFactor) + 1.0F;
        int t = ((ft < (float) MAXIMUM_CAPACITY) ? (int) ft : MAXIMUM_CAPACITY);
        if (t > threshold)
          threshold = tableSizeFor(t);
      } else if (s > threshold)
        resize();
      for (Map.Entry<? extends K, ? extends V> e: m.entrySet()) {
        K key = e.getKey();
        V value = e.getValue();
        putVal(hash(key), key, value, false, APPEND_TO_END_OF_LIST);
      }
    }
  }

  /**
   * Returns the number of key-value mappings in this map.
   *
   * @return the number of key-value mappings in this map
   */
  public int size() {
    return size;
  }

  /**
   * Returns <tt>true</tt> if this map contains no key-value mappings.
   *
   * @return <tt>true</tt> if this map contains no key-value mappings
   */
  public boolean isEmpty() {
    return size == 0;
  }

  /**
   * Returns the value to which the specified key is mapped,
   * or {@code null} if this map contains no mapping for the key.
   *
   * <p>More formally, if this map contains a mapping from a key
   * {@code k} to a value {@code v} such that {@code (key==null ? k==null :
   * key.equals(k))}, then this method returns {@code v}; otherwise
   * it returns {@code null}.  (There can be at most one such mapping.)
   *
   * <p>A return value of {@code null} does not <i>necessarily</i>
   * indicate that the map contains no mapping for the key; it's also
   * possible that the map explicitly maps the key to {@code null}.
   * The {@link #containsKey containsKey} operation may be used to
   * distinguish these two cases.
   *
   * @see #put(Object, Object)
   */
  public V get(Object key) {
    Node<K, V> e;
    return (e = getNode(hash(key), key)) == null ? null : e.value;
  }

  /**
   * Implements Map.get and related methods.
   *
   * @param hash hash for key
   * @param key the key
   * @return the node, or null if none
   */
  final Node<K, V> getNode(int hash, Object key) {
    Node<K, V>[] tab;
    Node<K, V> first, e;
    int n;
    K k;
    if ((tab = table) != null && (n = tab.length) > 0 && (first = tab[(n - 1) & hash]) != null) {
      if (first.hash == hash && // always check first node
          ((k = first.key) == key || (key != null && key.equals(k))))
        return first;
      if ((e = first.next) != null) {
        if (first instanceof TreeNode)
          return ((TreeNode<K, V>) first).getTreeNode(hash, key);
        do {
          if (e.hash == hash && ((k = e.key) == key || (key != null && key.equals(k))))
            return e;
        } while ((e = e.next) != null);
      }
    }
    return null;
  }

  /**
   * Returns <tt>true</tt> if this map contains a mapping for the
   * specified key.
   *
   * @param   key   The key whose presence in this map is to be tested
   * @return <tt>true</tt> if this map contains a mapping for the specified
   * key.
   */
  public boolean containsKey(Object key) {
    return getNode(hash(key), key) != null;
  }

  /**
   * Associates the specified value with the specified key in this map.
   * If the map previously contained a mapping for the key, the old
   * value is replaced.
   *
   * @param key key with which the specified value is to be associated
   * @param value value to be associated with the specified key
   * @return the previous value associated with <tt>key</tt>, or
   *         <tt>null</tt> if there was no mapping for <tt>key</tt>.
   *         (A <tt>null</tt> return can also indicate that the map
   *         previously associated <tt>null</tt> with <tt>key</tt>.)
   */
  public V put(K key, V value) {
    return putVal(hash(key), key, value, false, APPEND_TO_END_OF_LIST);
  }

  /**
   * Constant-time, as {@link Map#get(Object)}
   *
   * @return Index of the key or -1 if the key does not exist.
   */
  @Override
  public int indexOf(K key) {
    Node nodeForKey = getNode(hash(key), key);
    return nodeForKey == null ? -1 : nodeForKey.index;
  }

  /**
   * O(N) in the worst case.
   */
  @Override
  public V putByIndex(K key, V value, int index) {
    return putVal(hash(key), key, value, false, index);
  }

  /**
   * O(1)
   */
  @Override
  public Entry<K, V> getByIndex(int index) {
    return entryList.get(index);
  }

  /**
   * O(N) in the worst case.
   */
  @Override
  public Entry<K, V> removeByIndex(int index) {
    K key = entryList.get(index).getKey();
    return removeNode(hash(key), key, null, false, true);
  }

  /**
   * O(N) in the worst case.
   */
  @Override
  public void moveElement(int originalIndex, int newIndex) {
    Map.Entry<K, V> entry = entryList.remove(originalIndex);
    if (entry != null) {
      entryList.add(newIndex, entry);
    }
    fixIndices(Math.min(originalIndex, newIndex), Math.max(originalIndex, newIndex));
    modCount++;
  }

  /**
   * Returns a string representation of this map.  The string representation
   * consists of a list of key-value mappings in the order returned by the
   * map's <tt>entrySet</tt> view's iterator, enclosed in braces
   * (<tt>"{}"</tt>).  Adjacent mappings are separated by the characters
   * <tt>", "</tt> (comma and space).  Each key-value mapping is rendered as
   * the key followed by an equals sign (<tt>"="</tt>) followed by the
   * associated value.  Keys and values are converted to strings as by
   * {@link String#valueOf(Object)}.
   *
   * @return a string representation of this map
   */
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(this.getClass().getSimpleName());

    Iterator<Entry<K, V>> i = entrySet().iterator();
    if (!i.hasNext()) {
      sb.append("{}");
      return sb.toString();
    }

    sb.append('{');
    for (;;) {
      Entry<K, V> e = i.next();
      K key = e.getKey();
      V value = e.getValue();
      sb.append(key == this ? "(this Map)" : key);
      sb.append('=');
      sb.append(value == this ? "(this Map)" : value);
      if (!i.hasNext())
        return sb.append('}').toString();
      sb.append(',').append(' ');
    }
  }

  /**
   * Implements Map.put and related methods.
   *
   * @param hash hash for key
   * @param key the key
   * @param value the value to put
   * @param onlyIfAbsent if true, don't change existing value
   * @param index where to insert, or negative to append
   * @return previous value, or null if none
   */
  final V putVal(int hash, K key, V value, boolean onlyIfAbsent, int index) {
    Node<K, V>[] tab;
    Node<K, V> p;
    int n, i;
    if ((tab = table) == null || (n = tab.length) == 0)
      n = (tab = resize()).length;
    if ((p = tab[i = (n - 1) & hash]) == null) {
      tab[i] = newNode(hash, key, value, null, index);
    } else {
      Node<K, V> e;
      K k;
      if (p.hash == hash && ((k = p.key) == key || (key != null && key.equals(k))))
        e = p;
      else if (p instanceof TreeNode)
        e = ((TreeNode<K, V>) p).putTreeVal(this, tab, hash, key, value);
      else {
        for (int binCount = 0;; ++binCount) {
          if ((e = p.next) == null) {
            p.next = newNode(hash, key, value, null, index);
            if (binCount >= TREEIFY_THRESHOLD - 1) // -1 for 1st
              treeifyBin(tab, hash);
            break;
          }
          if (e.hash == hash && ((k = e.key) == key || (key != null && key.equals(k))))
            break;
          p = e;
        }
      }
      if (e != null) { // existing mapping for key
        V oldValue = e.value;
        if (!onlyIfAbsent || oldValue == null)
          e.value = value;
        afterNodeAccess(e);
        return oldValue;
      }
    }
    ++modCount;
    if (++size > threshold)
      resize();
    return null;
  }

  /**
   * Initializes or doubles table size.  If null, allocates in
   * accord with initial capacity target held in field threshold.
   * Otherwise, because we are using power-of-two expansion, the
   * elements from each bin must either stay at same index, or move
   * with a power of two offset in the new table.
   *
   * @return the table
   */
  final Node<K, V>[] resize() {
    Node<K, V>[] oldTab = table;
    int oldCap = (oldTab == null) ? 0 : oldTab.length;
    int oldThr = threshold;
    int newCap, newThr = 0;
    if (oldCap > 0) {
      if (oldCap >= MAXIMUM_CAPACITY) {
        threshold = Integer.MAX_VALUE;
        return oldTab;
      } else if ((newCap = oldCap << 1) < MAXIMUM_CAPACITY && oldCap >= DEFAULT_INITIAL_CAPACITY)
        newThr = oldThr << 1; // double threshold
    } else if (oldThr > 0) // initial capacity was placed in threshold
      newCap = oldThr;
    else { // zero initial threshold signifies using defaults
      newCap = DEFAULT_INITIAL_CAPACITY;
      newThr = (int) (DEFAULT_LOAD_FACTOR * DEFAULT_INITIAL_CAPACITY);
    }
    if (newThr == 0) {
      float ft = (float) newCap * loadFactor;
      newThr = (newCap < MAXIMUM_CAPACITY && ft < (float) MAXIMUM_CAPACITY ? (int) ft : Integer.MAX_VALUE);
    }
    threshold = newThr;
    @SuppressWarnings({ "rawtypes", "unchecked" })
    Node<K, V>[] newTab = (Node<K, V>[]) new Node[newCap];
    table = newTab;
    if (oldTab != null) {
      for (int j = 0; j < oldCap; ++j) {
        Node<K, V> e;
        if ((e = oldTab[j]) != null) {
          oldTab[j] = null;
          if (e.next == null)
            newTab[e.hash & (newCap - 1)] = e;
          else if (e instanceof TreeNode)
            ((TreeNode<K, V>) e).split(this, newTab, j, oldCap);
          else { // preserve order
            Node<K, V> loHead = null, loTail = null;
            Node<K, V> hiHead = null, hiTail = null;
            Node<K, V> next;
            do {
              next = e.next;
              if ((e.hash & oldCap) == 0) {
                if (loTail == null)
                  loHead = e;
                else
                  loTail.next = e;
                loTail = e;
              } else {
                if (hiTail == null)
                  hiHead = e;
                else
                  hiTail.next = e;
                hiTail = e;
              }
            } while ((e = next) != null);
            if (loTail != null) {
              loTail.next = null;
              newTab[j] = loHead;
            }
            if (hiTail != null) {
              hiTail.next = null;
              newTab[j + oldCap] = hiHead;
            }
          }
        }
      }
    }
    return newTab;
  }

  /**
   * Replaces all linked nodes in bin at index for given hash unless
   * table is too small, in which case resizes instead.
   */
  final void treeifyBin(Node<K, V>[] tab, int hash) {
    int n, index;
    Node<K, V> e;
    if (tab == null || (n = tab.length) < MIN_TREEIFY_CAPACITY)
      resize();
    else if ((e = tab[index = (n - 1) & hash]) != null) {
      TreeNode<K, V> hd = null, tl = null;
      do {
        TreeNode<K, V> p = replacementTreeNode(e, null);
        if (tl == null)
          hd = p;
        else {
          p.prev = tl;
          tl.next = p;
        }
        tl = p;
      } while ((e = e.next) != null);
      if ((tab[index] = hd) != null)
        hd.treeify(tab);
    }
  }

  /**
   * Copies all of the mappings from the specified map to this map.
   * These mappings will replace any mappings that this map had for
   * any of the keys currently in the specified map.
   *
   * @param m mappings to be stored in this map
   * @throws NullPointerException if the specified map is null
   */
  public void putAll(Map<? extends K, ? extends V> m) {
    putMapEntries(m, true);
  }

  /**
   * Removes the mapping for the specified key from this map if present.
   *
   * @param  key key whose mapping is to be removed from the map
   * @return the previous value associated with <tt>key</tt>, or
   *         <tt>null</tt> if there was no mapping for <tt>key</tt>.
   *         (A <tt>null</tt> return can also indicate that the map
   *         previously associated <tt>null</tt> with <tt>key</tt>.)
   */
  public V remove(Object key) {
    Node<K, V> e;
    return (e = removeNode(hash(key), key, null, false, true)) == null ? null : e.value;
  }

  /**
   * Implements Map.remove and related methods.
   *
   * @param hash hash for key
   * @param key the key
   * @param value the value to match if matchValue, else ignored
   * @param matchValue if true only remove if value is equal
   * @param movable if false do not move other nodes while removing
   * @return the node, or null if none
   */
  final Node<K, V> removeNode(int hash, Object key, Object value, boolean matchValue, boolean movable) {
    Node<K, V>[] tab;
    Node<K, V> p;
    int n, index;
    if ((tab = table) != null && (n = tab.length) > 0 && (p = tab[index = (n - 1) & hash]) != null) {
      Node<K, V> node = null, e;
      K k;
      V v;
      if (p.hash == hash && ((k = p.key) == key || (key != null && key.equals(k))))
        node = p;
      else if ((e = p.next) != null) {
        if (p instanceof TreeNode)
          node = ((TreeNode<K, V>) p).getTreeNode(hash, key);
        else {
          do {
            if (e.hash == hash && ((k = e.key) == key || (key != null && key.equals(k)))) {
              node = e;
              break;
            }
            p = e;
          } while ((e = e.next) != null);
        }
      }
      if (node != null && (!matchValue || (v = node.value) == value || (value != null && value.equals(v)))) {
        if (node instanceof TreeNode)
          ((TreeNode<K, V>) node).removeTreeNode(this, tab, movable);
        else if (node == p)
          tab[index] = node.next;
        else
          p.next = node.next;
        ++modCount;
        --size;
        afterNodeRemoval(node);
        return node;
      }
    }
    return null;
  }

  /**
   * Removes all of the mappings from this map.
   * The map will be empty after this call returns.
   */
  public void clear() {
    Node<K, V>[] tab;
    modCount++;
    if ((tab = table) != null && size > 0) {
      size = 0;
      for (int i = 0; i < tab.length; ++i)
        tab[i] = null;
    }
    entryList.clear();
  }

  /**
   * Returns <tt>true</tt> if this map maps one or more keys to the
   * specified value.
   *
   * @param value value whose presence in this map is to be tested
   * @return <tt>true</tt> if this map maps one or more keys to the
   *         specified value
   */
  public boolean containsValue(Object value) {
    V v;
    if (size > 0) {
      for (int i = 0; i < entryList.size(); ++i) {
        Map.Entry<K, V> e = entryList.get(i);
        if ((v = e.getValue()) == value || (value != null && value.equals(v)))
          return true;
      }
    }
    return false;
  }

  /**
   * Returns a {@link Set} view of the keys contained in this map.
   * The set is backed by the map, so changes to the map are
   * reflected in the set, and vice-versa.  If the map is modified
   * while an iteration over the set is in progress (except through
   * the iterator's own <tt>remove</tt> operation), the results of
   * the iteration are undefined.  The set supports element removal,
   * which removes the corresponding mapping from the map, via the
   * <tt>Iterator.remove</tt>, <tt>Set.remove</tt>,
   * <tt>removeAll</tt>, <tt>retainAll</tt>, and <tt>clear</tt>
   * operations.  It does not support the <tt>add</tt> or <tt>addAll</tt>
   * operations.
   *
   * @return a set view of the keys contained in this map
   */
  public Set<K> keySet() {
    Set<K> ks = keySet;
    if (ks == null) {
      ks = new KeySet();
      keySet = ks;
    }
    return ks;
  }

  final class KeySet extends AbstractSet<K> {
    public final int size() {
      return size;
    }

    public final void clear() {
      IndexedHashMap.this.clear();
    }

    public final Iterator<K> iterator() {
      return new KeyIterator();
    }

    public final boolean contains(Object o) {
      return containsKey(o);
    }

    public final boolean remove(Object key) {
      return removeNode(hash(key), key, null, false, true) != null;
    }

    public final Spliterator<K> spliterator() {
      return entryList.stream().map(kvEntry -> kvEntry.getKey()).spliterator();
    }

    public final void forEach(Consumer<? super K> action) {
      iterator().forEachRemaining(action);
    }
  }

  /**
   * Returns a {@link Collection} view of the values contained in this map.
   * The collection is backed by the map, so changes to the map are
   * reflected in the collection, and vice-versa.  If the map is
   * modified while an iteration over the collection is in progress
   * (except through the iterator's own <tt>remove</tt> operation),
   * the results of the iteration are undefined.  The collection
   * supports element removal, which removes the corresponding
   * mapping from the map, via the <tt>Iterator.remove</tt>,
   * <tt>Collection.remove</tt>, <tt>removeAll</tt>,
   * <tt>retainAll</tt> and <tt>clear</tt> operations.  It does not
   * support the <tt>add</tt> or <tt>addAll</tt> operations.
   *
   * @return a view of the values contained in this map
   */
  public Collection<V> values() {
    Collection<V> vs = values;
    if (vs == null) {
      vs = new Values();
      values = vs;
    }
    return vs;
  }

  final class Values extends AbstractCollection<V> {
    public final int size() {
      return size;
    }

    public final void clear() {
      IndexedHashMap.this.clear();
    }

    public final Iterator<V> iterator() {
      return new ValueIterator();
    }

    public final boolean contains(Object o) {
      return containsValue(o);
    }

    public final Spliterator<V> spliterator() {
      return entryList.stream().map(kvEntry -> kvEntry.getValue()).spliterator();
    }

    public final void forEach(Consumer<? super V> action) {
      iterator().forEachRemaining(action);
    }
  }

  /**
   * Returns a {@link Set} view of the mappings contained in this map.
   * The set is backed by the map, so changes to the map are
   * reflected in the set, and vice-versa.  If the map is modified
   * while an iteration over the set is in progress (except through
   * the iterator's own <tt>remove</tt> operation, or through the
   * <tt>setValue</tt> operation on a map entry returned by the
   * iterator) the results of the iteration are undefined.  The set
   * supports element removal, which removes the corresponding
   * mapping from the map, via the <tt>Iterator.remove</tt>,
   * <tt>Set.remove</tt>, <tt>removeAll</tt>, <tt>retainAll</tt> and
   * <tt>clear</tt> operations.  It does not support the
   * <tt>add</tt> or <tt>addAll</tt> operations.
   *
   * @return a set view of the mappings contained in this map
   */
  public Set<Map.Entry<K, V>> entrySet() {
    Set<Map.Entry<K, V>> es;
    return (es = entrySet) == null ? (entrySet = new EntrySet()) : es;
  }

  final class EntrySet extends AbstractSet<Map.Entry<K, V>> {
    public final int size() {
      return size;
    }

    public final void clear() {
      IndexedHashMap.this.clear();
    }

    public final Iterator<Map.Entry<K, V>> iterator() {
      return new EntryIterator();
    }

    public final boolean contains(Object o) {
      if (!(o instanceof Map.Entry))
        return false;
      Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
      Object key = e.getKey();
      Node<K, V> candidate = getNode(hash(key), key);
      return candidate != null && candidate.equals(e);
    }

    public final boolean remove(Object o) {
      if (o instanceof Map.Entry) {
        Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
        Object key = e.getKey();
        Object value = e.getValue();
        return removeNode(hash(key), key, value, true, true) != null;
      }
      return false;
    }

    public final Spliterator<Map.Entry<K, V>> spliterator() {
      return entryList.spliterator();
    }

    public final void forEach(Consumer<? super Map.Entry<K, V>> action) {
      iterator().forEachRemaining(action);
    }
  }

  // Overrides of JDK8 Map extension methods

  @Override
  public V getOrDefault(Object key, V defaultValue) {
    Node<K, V> e;
    return (e = getNode(hash(key), key)) == null ? defaultValue : e.value;
  }

  @Override
  public V putIfAbsent(K key, V value) {
    return putVal(hash(key), key, value, true, APPEND_TO_END_OF_LIST);
  }

  @Override
  public boolean remove(Object key, Object value) {
    return removeNode(hash(key), key, value, true, true) != null;
  }

  @Override
  public boolean replace(K key, V oldValue, V newValue) {
    Node<K, V> e;
    V v;
    if ((e = getNode(hash(key), key)) != null && ((v = e.value) == oldValue || (v != null && v.equals(oldValue)))) {
      e.value = newValue;
      afterNodeAccess(e);
      return true;
    }
    return false;
  }

  @Override
  public V replace(K key, V value) {
    Node<K, V> e;
    if ((e = getNode(hash(key), key)) != null) {
      V oldValue = e.value;
      e.value = value;
      afterNodeAccess(e);
      return oldValue;
    }
    return null;
  }

  @Override
  public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
    if (mappingFunction == null)
      throw new NullPointerException();
    int hash = hash(key);
    Node<K, V>[] tab;
    Node<K, V> first;
    int n, i;
    int binCount = 0;
    TreeNode<K, V> t = null;
    Node<K, V> old = null;
    if (size > threshold || (tab = table) == null || (n = tab.length) == 0)
      n = (tab = resize()).length;
    if ((first = tab[i = (n - 1) & hash]) != null) {
      if (first instanceof TreeNode)
        old = (t = (TreeNode<K, V>) first).getTreeNode(hash, key);
      else {
        Node<K, V> e = first;
        K k;
        do {
          if (e.hash == hash && ((k = e.key) == key || (key != null && key.equals(k)))) {
            old = e;
            break;
          }
          ++binCount;
        } while ((e = e.next) != null);
      }
      V oldValue;
      if (old != null && (oldValue = old.value) != null) {
        afterNodeAccess(old);
        return oldValue;
      }
    }
    V v = mappingFunction.apply(key);
    if (v == null) {
      return null;
    } else if (old != null) {
      old.value = v;
      afterNodeAccess(old);
      return v;
    } else if (t != null)
      t.putTreeVal(this, tab, hash, key, v);
    else {
      tab[i] = newNode(hash, key, v, first, APPEND_TO_END_OF_LIST);
      if (binCount >= TREEIFY_THRESHOLD - 1)
        treeifyBin(tab, hash);
    }
    ++modCount;
    ++size;
    afterNodeInsertion(true);
    return v;
  }

  public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    if (remappingFunction == null)
      throw new NullPointerException();
    Node<K, V> e;
    V oldValue;
    int hash = hash(key);
    if ((e = getNode(hash, key)) != null && (oldValue = e.value) != null) {
      V v = remappingFunction.apply(key, oldValue);
      if (v != null) {
        e.value = v;
        afterNodeAccess(e);
        return v;
      } else
        removeNode(hash, key, null, false, true);
    }
    return null;
  }

  @Override
  public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    if (remappingFunction == null)
      throw new NullPointerException();
    int hash = hash(key);
    Node<K, V>[] tab;
    Node<K, V> first;
    int n, i;
    int binCount = 0;
    TreeNode<K, V> t = null;
    Node<K, V> old = null;
    if (size > threshold || (tab = table) == null || (n = tab.length) == 0)
      n = (tab = resize()).length;
    if ((first = tab[i = (n - 1) & hash]) != null) {
      if (first instanceof TreeNode)
        old = (t = (TreeNode<K, V>) first).getTreeNode(hash, key);
      else {
        Node<K, V> e = first;
        K k;
        do {
          if (e.hash == hash && ((k = e.key) == key || (key != null && key.equals(k)))) {
            old = e;
            break;
          }
          ++binCount;
        } while ((e = e.next) != null);
      }
    }
    V oldValue = (old == null) ? null : old.value;
    V v = remappingFunction.apply(key, oldValue);
    if (old != null) {
      if (v != null) {
        old.value = v;
        afterNodeAccess(old);
      } else
        removeNode(hash, key, null, false, true);
    } else if (v != null) {
      if (t != null)
        t.putTreeVal(this, tab, hash, key, v);
      else {
        tab[i] = newNode(hash, key, v, first, APPEND_TO_END_OF_LIST);
        if (binCount >= TREEIFY_THRESHOLD - 1)
          treeifyBin(tab, hash);
      }
      ++modCount;
      ++size;
      afterNodeInsertion(true);
    }
    return v;
  }

  @Override
  public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
    if (value == null)
      throw new NullPointerException();
    if (remappingFunction == null)
      throw new NullPointerException();
    int hash = hash(key);
    Node<K, V>[] tab;
    Node<K, V> first;
    int n, i;
    int binCount = 0;
    TreeNode<K, V> t = null;
    Node<K, V> old = null;
    if (size > threshold || (tab = table) == null || (n = tab.length) == 0)
      n = (tab = resize()).length;
    if ((first = tab[i = (n - 1) & hash]) != null) {
      if (first instanceof TreeNode)
        old = (t = (TreeNode<K, V>) first).getTreeNode(hash, key);
      else {
        Node<K, V> e = first;
        K k;
        do {
          if (e.hash == hash && ((k = e.key) == key || (key != null && key.equals(k)))) {
            old = e;
            break;
          }
          ++binCount;
        } while ((e = e.next) != null);
      }
    }
    if (old != null) {
      V v;
      if (old.value != null)
        v = remappingFunction.apply(old.value, value);
      else
        v = value;
      if (v != null) {
        old.value = v;
        afterNodeAccess(old);
      } else
        removeNode(hash, key, null, false, true);
      return v;
    }
    if (value != null) {
      if (t != null)
        t.putTreeVal(this, tab, hash, key, value);
      else {
        tab[i] = newNode(hash, key, value, first, APPEND_TO_END_OF_LIST);
        if (binCount >= TREEIFY_THRESHOLD - 1)
          treeifyBin(tab, hash);
      }
      ++modCount;
      ++size;
      afterNodeInsertion(true);
    }
    return value;
  }

  @Override
  public void forEach(BiConsumer<? super K, ? super V> action) {
    entryList.forEach(kvEntry -> action.accept(kvEntry.getKey(), kvEntry.getValue()));
  }

  @Override
  public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
    entryList.forEach(kvEntry -> kvEntry.setValue(function.apply(kvEntry.getKey(), kvEntry.getValue())));
  }

  /* ------------------------------------------------------------ */
  // iterators

  abstract class HashIterator {
    Map.Entry<K, V> next; // next entry to return
    Map.Entry<K, V> current; // current entry
    int expectedModCount; // for fast-fail
    int index; // current slot

    HashIterator() {
      expectedModCount = modCount;
      List<Map.Entry<K, V>> entries = entryList;
      current = next = null;
      index = 0;
      if (index < entries.size()) {
        next = entries.get(index++);
      }
    }

    public final boolean hasNext() {
      return next != null;
    }

    final Map.Entry<K, V> nextNode() {
      List<Map.Entry<K, V>> entries = entryList;
      if (modCount != expectedModCount)
        throw new ConcurrentModificationException();
      if (next == null)
        throw new NoSuchElementException();
      current = next;
      if (index == entries.size()) {
        next = null;
      } else {
        next = entries.get(index++);
      }
      return current;
    }

    public final void remove() {
      Map.Entry<K, V> p = current;
      if (p == null)
        throw new IllegalStateException();
      if (modCount != expectedModCount)
        throw new ConcurrentModificationException();
      current = null;
      K key = p.getKey();
      removeNode(hash(key), key, null, false, false);
      index--;
      expectedModCount = modCount;
    }
  }

  final class KeyIterator extends HashIterator implements Iterator<K> {
    public final K next() {
      return nextNode().getKey();
    }
  }

  final class ValueIterator extends HashIterator implements Iterator<V> {
    public final V next() {
      return nextNode().getValue();
    }
  }

  final class EntryIterator extends HashIterator implements Iterator<Map.Entry<K, V>> {
    public final Map.Entry<K, V> next() {
      return nextNode();
    }
  }

  /* ------------------------------------------------------------ */
  // LinkedHashMap support

  /*
   * The following package-protected methods are designed to be
   * overridden by LinkedHashMap, but not by any other subclass.
   * Nearly all other internal methods are also package-protected
   * but are declared final, so can be used by LinkedHashMap, view
   * classes, and HashSet.
   */

  // Create a regular (non-tree) node
  Node<K, V> newNode(int hash, K key, V value, Node<K, V> next, int index) {
    return newNode(index, i -> new Node<>(hash, key, value, next, i));
  }

  // For conversion from TreeNodes to plain nodes
  Node<K, V> replacementNode(Node<K, V> p, Node<K, V> next) {
    Node<K, V> entry = new Node<>(p.hash, p.key, p.value, next, p.index);
    entryList.set(p.index, entry);
    return entry;
  }

  // Create a tree bin node
  TreeNode<K, V> newTreeNode(int hash, K key, V value, Node<K, V> next, int index) {
    return newNode(index, i -> new TreeNode<>(hash, key, value, next, i));
  }

  <N extends Node> N newNode(int index, Function<Integer, N> nodeConstructor) {
    boolean append = index < 0;
    if (append) {
      index = entryList.size();
    }
    N entry = nodeConstructor.apply(index);
    entryList.add(index, entry);
    if (!append) {
      fixIndices(index, entryList.size());
    }
    return entry;
  }

  // For treeifyBin
  TreeNode<K, V> replacementTreeNode(Node<K, V> p, Node<K, V> next) {
    TreeNode<K, V> entry = new TreeNode<>(p.hash, p.key, p.value, next, p.index);
    entryList.set(p.index, entry);
    return entry;
  }

  // Callbacks to allow LinkedHashMap post-actions
  void afterNodeAccess(Node<K, V> p) {
  }

  void afterNodeInsertion(boolean evict) {
  }

  void afterNodeRemoval(Node<K, V> p) {
    entryList.remove(p.index);
    fixIndices(p.index, entryList.size());
  }

  private void fixIndices(int beginningIndex, int endingIndex) {
    for (int i = beginningIndex; i < endingIndex; i++) {
      ((Node) entryList.get(i)).index = i;
    }
  }

  /* ------------------------------------------------------------ */
  // Tree bins

  /**
   * Entry for Tree bins.
   */
  static final class TreeNode<K, V> extends Node<K, V> {
    TreeNode<K, V> parent; // red-black tree links
    TreeNode<K, V> left;
    TreeNode<K, V> right;
    TreeNode<K, V> prev; // needed to unlink next upon deletion
    boolean red;

    TreeNode(int hash, K key, V val, Node<K, V> next, int index) {
      super(hash, key, val, next, index);
    }

    /**
     * Returns root of tree containing this node.
     */
    final TreeNode<K, V> root() {
      for (TreeNode<K, V> r = this, p;;) {
        if ((p = r.parent) == null)
          return r;
        r = p;
      }
    }

    /**
     * Ensures that the given root is the first node of its bin.
     */
    static <K, V> void moveRootToFront(Node<K, V>[] tab, TreeNode<K, V> root) {
      int n;
      if (root != null && tab != null && (n = tab.length) > 0) {
        int index = (n - 1) & root.hash;
        TreeNode<K, V> first = (TreeNode<K, V>) tab[index];
        if (root != first) {
          Node<K, V> rn;
          tab[index] = root;
          TreeNode<K, V> rp = root.prev;
          if ((rn = root.next) != null)
            ((TreeNode<K, V>) rn).prev = rp;
          if (rp != null)
            rp.next = rn;
          if (first != null)
            first.prev = root;
          root.next = first;
          root.prev = null;
        }
        assert checkInvariants(root);
      }
    }

    /**
     * Finds the node starting at root p with the given hash and key.
     * The kc argument caches comparableClassFor(key) upon first use
     * comparing keys.
     */
    final TreeNode<K, V> find(int h, Object k, Class<?> kc) {
      TreeNode<K, V> p = this;
      do {
        int ph, dir;
        K pk;
        TreeNode<K, V> pl = p.left, pr = p.right, q;
        if ((ph = p.hash) > h)
          p = pl;
        else if (ph < h)
          p = pr;
        else if ((pk = p.key) == k || (k != null && k.equals(pk)))
          return p;
        else if (pl == null)
          p = pr;
        else if (pr == null)
          p = pl;
        else if ((kc != null || (kc = comparableClassFor(k)) != null) && (dir = compareComparables(kc, k, pk)) != 0)
          p = (dir < 0) ? pl : pr;
        else if ((q = pr.find(h, k, kc)) != null)
          return q;
        else
          p = pl;
      } while (p != null);
      return null;
    }

    /**
     * Calls find for root node.
     */
    final TreeNode<K, V> getTreeNode(int h, Object k) {
      return ((parent != null) ? root() : this).find(h, k, null);
    }

    /**
     * Tie-breaking utility for ordering insertions when equal
     * hashCodes and non-comparable. We don't require a total
     * order, just a consistent insertion rule to maintain
     * equivalence across rebalancings. Tie-breaking further than
     * necessary simplifies testing a bit.
     */
    static int tieBreakOrder(Object a, Object b) {
      int d;
      if (a == null || b == null || (d = a.getClass().getName().compareTo(b.getClass().getName())) == 0)
        d = (System.identityHashCode(a) <= System.identityHashCode(b) ? -1 : 1);
      return d;
    }

    /**
     * Forms tree of the nodes linked from this node.
     */
    final void treeify(Node<K, V>[] tab) {
      TreeNode<K, V> root = null;
      for (TreeNode<K, V> x = this, next; x != null; x = next) {
        next = (TreeNode<K, V>) x.next;
        x.left = x.right = null;
        if (root == null) {
          x.parent = null;
          x.red = false;
          root = x;
        } else {
          K k = x.key;
          int h = x.hash;
          Class<?> kc = null;
          for (TreeNode<K, V> p = root;;) {
            int dir, ph;
            K pk = p.key;
            if ((ph = p.hash) > h)
              dir = -1;
            else if (ph < h)
              dir = 1;
            else if ((kc == null && (kc = comparableClassFor(k)) == null) || (dir = compareComparables(kc, k, pk)) == 0)
              dir = tieBreakOrder(k, pk);

            TreeNode<K, V> xp = p;
            if ((p = (dir <= 0) ? p.left : p.right) == null) {
              x.parent = xp;
              if (dir <= 0)
                xp.left = x;
              else
                xp.right = x;
              root = balanceInsertion(root, x);
              break;
            }
          }
        }
      }
      moveRootToFront(tab, root);
    }

    /**
     * Returns a list of non-TreeNodes replacing those linked from
     * this node.
     */
    final Node<K, V> untreeify(IndexedHashMap<K, V> map) {
      Node<K, V> hd = null, tl = null;
      for (Node<K, V> q = this; q != null; q = q.next) {
        Node<K, V> p = map.replacementNode(q, null);
        if (tl == null)
          hd = p;
        else
          tl.next = p;
        tl = p;
      }
      return hd;
    }

    /**
     * Tree version of putVal.
     */
    final TreeNode<K, V> putTreeVal(IndexedHashMap<K, V> map, Node<K, V>[] tab, int h, K k, V v) {
      Class<?> kc = null;
      boolean searched = false;
      TreeNode<K, V> root = (parent != null) ? root() : this;
      for (TreeNode<K, V> p = root;;) {
        int dir, ph;
        K pk;
        if ((ph = p.hash) > h)
          dir = -1;
        else if (ph < h)
          dir = 1;
        else if ((pk = p.key) == k || (k != null && k.equals(pk)))
          return p;
        else if ((kc == null && (kc = comparableClassFor(k)) == null) || (dir = compareComparables(kc, k, pk)) == 0) {
          if (!searched) {
            TreeNode<K, V> q, ch;
            searched = true;
            if (((ch = p.left) != null && (q = ch.find(h, k, kc)) != null)
                || ((ch = p.right) != null && (q = ch.find(h, k, kc)) != null))
              return q;
          }
          dir = tieBreakOrder(k, pk);
        }

        TreeNode<K, V> xp = p;
        if ((p = (dir <= 0) ? p.left : p.right) == null) {
          Node<K, V> xpn = xp.next;
          TreeNode<K, V> x = map.newTreeNode(h, k, v, xpn, APPEND_TO_END_OF_LIST);
          if (dir <= 0)
            xp.left = x;
          else
            xp.right = x;
          xp.next = x;
          x.parent = x.prev = xp;
          if (xpn != null)
            ((TreeNode<K, V>) xpn).prev = x;
          moveRootToFront(tab, balanceInsertion(root, x));
          return null;
        }
      }
    }

    /**
     * Removes the given node, that must be present before this call.
     * This is messier than typical red-black deletion code because we
     * cannot swap the contents of an interior node with a leaf
     * successor that is pinned by "next" pointers that are accessible
     * independently during traversal. So instead we swap the tree
     * linkages. If the current tree appears to have too few nodes,
     * the bin is converted back to a plain bin. (The test triggers
     * somewhere between 2 and 6 nodes, depending on tree structure).
     */
    final void removeTreeNode(IndexedHashMap<K, V> map, Node<K, V>[] tab, boolean movable) {
      int n;
      if (tab == null || (n = tab.length) == 0)
        return;
      int index = (n - 1) & hash;
      TreeNode<K, V> first = (TreeNode<K, V>) tab[index], root = first, rl;
      TreeNode<K, V> succ = (TreeNode<K, V>) next, pred = prev;
      if (pred == null)
        tab[index] = first = succ;
      else
        pred.next = succ;
      if (succ != null)
        succ.prev = pred;
      if (first == null)
        return;
      if (root.parent != null)
        root = root.root();
      if (root == null || (movable && (root.right == null || (rl = root.left) == null || rl.left == null))) {
        tab[index] = first.untreeify(map); // too small
        return;
      }
      TreeNode<K, V> p = this, pl = left, pr = right, replacement;
      if (pl != null && pr != null) {
        TreeNode<K, V> s = pr, sl;
        while ((sl = s.left) != null) // find successor
          s = sl;
        boolean c = s.red;
        s.red = p.red;
        p.red = c; // swap colors
        TreeNode<K, V> sr = s.right;
        TreeNode<K, V> pp = p.parent;
        if (s == pr) { // p was s's direct parent
          p.parent = s;
          s.right = p;
        } else {
          TreeNode<K, V> sp = s.parent;
          if ((p.parent = sp) != null) {
            if (s == sp.left)
              sp.left = p;
            else
              sp.right = p;
          }
          if ((s.right = pr) != null)
            pr.parent = s;
        }
        p.left = null;
        if ((p.right = sr) != null)
          sr.parent = p;
        if ((s.left = pl) != null)
          pl.parent = s;
        if ((s.parent = pp) == null)
          root = s;
        else if (p == pp.left)
          pp.left = s;
        else
          pp.right = s;
        if (sr != null)
          replacement = sr;
        else
          replacement = p;
      } else if (pl != null)
        replacement = pl;
      else if (pr != null)
        replacement = pr;
      else
        replacement = p;
      if (replacement != p) {
        TreeNode<K, V> pp = replacement.parent = p.parent;
        if (pp == null)
          root = replacement;
        else if (p == pp.left)
          pp.left = replacement;
        else
          pp.right = replacement;
        p.left = p.right = p.parent = null;
      }

      TreeNode<K, V> r = p.red ? root : balanceDeletion(root, replacement);

      if (replacement == p) { // detach
        TreeNode<K, V> pp = p.parent;
        p.parent = null;
        if (pp != null) {
          if (p == pp.left)
            pp.left = null;
          else if (p == pp.right)
            pp.right = null;
        }
      }
      if (movable)
        moveRootToFront(tab, r);
    }

    /**
     * Splits nodes in a tree bin into lower and upper tree bins,
     * or untreeifies if now too small. Called only from resize;
     * see above discussion about split bits and indices.
     *
     * @param map the map
     * @param tab the table for recording bin heads
     * @param index the index of the table being split
     * @param bit the bit of hash to split on
     */
    final void split(IndexedHashMap<K, V> map, Node<K, V>[] tab, int index, int bit) {
      TreeNode<K, V> b = this;
      // Relink into lo and hi lists, preserving order
      TreeNode<K, V> loHead = null, loTail = null;
      TreeNode<K, V> hiHead = null, hiTail = null;
      int lc = 0, hc = 0;
      for (TreeNode<K, V> e = b, next; e != null; e = next) {
        next = (TreeNode<K, V>) e.next;
        e.next = null;
        if ((e.hash & bit) == 0) {
          if ((e.prev = loTail) == null)
            loHead = e;
          else
            loTail.next = e;
          loTail = e;
          ++lc;
        } else {
          if ((e.prev = hiTail) == null)
            hiHead = e;
          else
            hiTail.next = e;
          hiTail = e;
          ++hc;
        }
      }

      if (loHead != null) {
        if (lc <= UNTREEIFY_THRESHOLD)
          tab[index] = loHead.untreeify(map);
        else {
          tab[index] = loHead;
          if (hiHead != null) // (else is already treeified)
            loHead.treeify(tab);
        }
      }
      if (hiHead != null) {
        if (hc <= UNTREEIFY_THRESHOLD)
          tab[index + bit] = hiHead.untreeify(map);
        else {
          tab[index + bit] = hiHead;
          if (loHead != null)
            hiHead.treeify(tab);
        }
      }
    }

    /* ------------------------------------------------------------ */
    // Red-black tree methods, all adapted from CLR

    static <K, V> TreeNode<K, V> rotateLeft(TreeNode<K, V> root, TreeNode<K, V> p) {
      TreeNode<K, V> r, pp, rl;
      if (p != null && (r = p.right) != null) {
        if ((rl = p.right = r.left) != null)
          rl.parent = p;
        if ((pp = r.parent = p.parent) == null)
          (root = r).red = false;
        else if (pp.left == p)
          pp.left = r;
        else
          pp.right = r;
        r.left = p;
        p.parent = r;
      }
      return root;
    }

    static <K, V> TreeNode<K, V> rotateRight(TreeNode<K, V> root, TreeNode<K, V> p) {
      TreeNode<K, V> l, pp, lr;
      if (p != null && (l = p.left) != null) {
        if ((lr = p.left = l.right) != null)
          lr.parent = p;
        if ((pp = l.parent = p.parent) == null)
          (root = l).red = false;
        else if (pp.right == p)
          pp.right = l;
        else
          pp.left = l;
        l.right = p;
        p.parent = l;
      }
      return root;
    }

    static <K, V> TreeNode<K, V> balanceInsertion(TreeNode<K, V> root, TreeNode<K, V> x) {
      x.red = true;
      for (TreeNode<K, V> xp, xpp, xppl, xppr;;) {
        if ((xp = x.parent) == null) {
          x.red = false;
          return x;
        } else if (!xp.red || (xpp = xp.parent) == null)
          return root;
        if (xp == (xppl = xpp.left)) {
          if ((xppr = xpp.right) != null && xppr.red) {
            xppr.red = false;
            xp.red = false;
            xpp.red = true;
            x = xpp;
          } else {
            if (x == xp.right) {
              root = rotateLeft(root, x = xp);
              xpp = (xp = x.parent) == null ? null : xp.parent;
            }
            if (xp != null) {
              xp.red = false;
              if (xpp != null) {
                xpp.red = true;
                root = rotateRight(root, xpp);
              }
            }
          }
        } else {
          if (xppl != null && xppl.red) {
            xppl.red = false;
            xp.red = false;
            xpp.red = true;
            x = xpp;
          } else {
            if (x == xp.left) {
              root = rotateRight(root, x = xp);
              xpp = (xp = x.parent) == null ? null : xp.parent;
            }
            if (xp != null) {
              xp.red = false;
              if (xpp != null) {
                xpp.red = true;
                root = rotateLeft(root, xpp);
              }
            }
          }
        }
      }
    }

    static <K, V> TreeNode<K, V> balanceDeletion(TreeNode<K, V> root, TreeNode<K, V> x) {
      for (TreeNode<K, V> xp, xpl, xpr;;) {
        if (x == null || x == root)
          return root;
        else if ((xp = x.parent) == null) {
          x.red = false;
          return x;
        } else if (x.red) {
          x.red = false;
          return root;
        } else if ((xpl = xp.left) == x) {
          if ((xpr = xp.right) != null && xpr.red) {
            xpr.red = false;
            xp.red = true;
            root = rotateLeft(root, xp);
            xpr = (xp = x.parent) == null ? null : xp.right;
          }
          if (xpr == null)
            x = xp;
          else {
            TreeNode<K, V> sl = xpr.left, sr = xpr.right;
            if ((sr == null || !sr.red) && (sl == null || !sl.red)) {
              xpr.red = true;
              x = xp;
            } else {
              if (sr == null || !sr.red) {
                if (sl != null)
                  sl.red = false;
                xpr.red = true;
                root = rotateRight(root, xpr);
                xpr = (xp = x.parent) == null ? null : xp.right;
              }
              if (xpr != null) {
                xpr.red = (xp == null) ? false : xp.red;
                if ((sr = xpr.right) != null)
                  sr.red = false;
              }
              if (xp != null) {
                xp.red = false;
                root = rotateLeft(root, xp);
              }
              x = root;
            }
          }
        } else { // symmetric
          if (xpl != null && xpl.red) {
            xpl.red = false;
            xp.red = true;
            root = rotateRight(root, xp);
            xpl = (xp = x.parent) == null ? null : xp.left;
          }
          if (xpl == null)
            x = xp;
          else {
            TreeNode<K, V> sl = xpl.left, sr = xpl.right;
            if ((sl == null || !sl.red) && (sr == null || !sr.red)) {
              xpl.red = true;
              x = xp;
            } else {
              if (sl == null || !sl.red) {
                if (sr != null)
                  sr.red = false;
                xpl.red = true;
                root = rotateLeft(root, xpl);
                xpl = (xp = x.parent) == null ? null : xp.left;
              }
              if (xpl != null) {
                xpl.red = (xp == null) ? false : xp.red;
                if ((sl = xpl.left) != null)
                  sl.red = false;
              }
              if (xp != null) {
                xp.red = false;
                root = rotateRight(root, xp);
              }
              x = root;
            }
          }
        }
      }
    }

    /**
     * Recursive invariant check
     */
    static <K, V> boolean checkInvariants(TreeNode<K, V> t) {
      TreeNode<K, V> tp = t.parent, tl = t.left, tr = t.right, tb = t.prev, tn = (TreeNode<K, V>) t.next;
      if (tb != null && tb.next != t)
        return false;
      if (tn != null && tn.prev != t)
        return false;
      if (tp != null && t != tp.left && t != tp.right)
        return false;
      if (tl != null && (tl.parent != t || tl.hash > t.hash))
        return false;
      if (tr != null && (tr.parent != t || tr.hash < t.hash))
        return false;
      if (t.red && tl != null && tl.red && tr != null && tr.red)
        return false;
      if (tl != null && !checkInvariants(tl))
        return false;
      if (tr != null && !checkInvariants(tr))
        return false;
      return true;
    }
  }

}

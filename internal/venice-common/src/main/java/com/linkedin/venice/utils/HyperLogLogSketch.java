package com.linkedin.venice.utils;

import java.nio.ByteBuffer;
import java.util.Arrays;


/**
 * A standalone HyperLogLog (HLL) sketch for estimating the cardinality (number of distinct elements)
 * of a multiset. This is a pure-Java implementation with no external dependencies.
 *
 * <h3>Usage</h3>
 * <pre>
 *   HyperLogLogSketch hll = new HyperLogLogSketch();      // p=14 by default
 *   hll.add(keyBytes);
 *   hll.add(otherKeyBytes);
 *   long estimate = hll.estimate();                        // ~unique count
 *
 *   // Merge two sketches (e.g., from different partitions / tasks)
 *   hll.merge(otherHll);
 *
 *   // Serialize / deserialize for transport or persistence
 *   byte[] bytes = hll.toBytes();
 *   HyperLogLogSketch restored = HyperLogLogSketch.fromBytes(bytes);
 * </pre>
 *
 * <h3>Parameters</h3>
 * <ul>
 *   <li><b>p (precision)</b> — number of bits used for register indexing. 2^p registers are allocated.
 *       Higher p = more memory but lower error. Default p=14 gives ~0.8% standard error with 16 KB.</li>
 * </ul>
 *
 * <h3>Serialization format</h3>
 * <pre>
 *   [1 byte: precision p] [2^p bytes: registers (unsigned, valid range 0..64-p+1)]
 * </pre>
 * Total size = 1 + 2^p bytes (e.g., 16385 bytes for p=14). Always dense — no sparse mode.
 *
 * <h3>Thread safety</h3>
 * Not thread-safe. Callers must synchronize externally if shared across threads.
 */
public class HyperLogLogSketch {
  /** Default precision. 2^14 = 16384 registers, ~0.8% standard error, 16 KB memory. */
  public static final int DEFAULT_PRECISION = 14;

  /** Minimum supported precision (64 registers, ~26% error). */
  public static final int MIN_PRECISION = 4;

  /** Maximum supported precision (2^18 = 262144 registers, ~0.1% error, 256 KB). */
  public static final int MAX_PRECISION = 18;

  private final int p;
  private final int m; // 2^p
  private final double alphaM;
  private final byte[] registers;

  /** Creates a new sketch with default precision (p=14). */
  public HyperLogLogSketch() {
    this(DEFAULT_PRECISION);
  }

  /**
   * Creates a new sketch with the given precision.
   *
   * @param precision number of bits for register indexing (4..18). 2^p registers are allocated.
   * @throws IllegalArgumentException if precision is out of range
   */
  public HyperLogLogSketch(int precision) {
    if (precision < MIN_PRECISION || precision > MAX_PRECISION) {
      throw new IllegalArgumentException(
          "Precision must be between " + MIN_PRECISION + " and " + MAX_PRECISION + ", got " + precision);
    }
    this.p = precision;
    this.m = 1 << p;
    this.alphaM = computeAlpha(m) * m * m;
    this.registers = new byte[m];
  }

  /** Internal constructor for deserialization — takes ownership of the provided registers array. */
  private HyperLogLogSketch(int precision, byte[] ownedRegisters) {
    this.p = precision;
    this.m = 1 << p;
    this.alphaM = computeAlpha(m) * m * m;
    this.registers = ownedRegisters;
  }

  /**
   * Adds a byte array element to the sketch.
   *
   * @param key the element to add (null or empty keys are ignored)
   */
  public void add(byte[] key) {
    if (key == null || key.length == 0) {
      return;
    }
    updateRegister(hash64(key));
  }

  /**
   * Adds a pre-computed 64-bit hash to the sketch. Useful when the caller already has
   * a hash (e.g., from a partitioner) and wants to avoid re-hashing.
   *
   * @param hash a well-distributed 64-bit hash value
   */
  public void addHash(long hash) {
    updateRegister(hash);
  }

  private void updateRegister(long hash) {
    int registerIndex = (int) (hash >>> (64 - p));
    long remainingBits = (hash << p) | (1L << (p - 1));
    int rho = Long.numberOfLeadingZeros(remainingBits) + 1;
    if (rho > registers[registerIndex]) {
      registers[registerIndex] = (byte) rho;
    }
  }

  /**
   * Merges another sketch into this one. Both sketches must have the same precision.
   * The merge operation is element-wise max of registers — associative, commutative, and idempotent.
   *
   * @param other the sketch to merge in (must not be null)
   * @throws IllegalArgumentException if other is null or precisions differ
   */
  public void merge(HyperLogLogSketch other) {
    if (other == null) {
      throw new IllegalArgumentException("Cannot merge null HLL sketch");
    }
    if (this.p != other.p) {
      throw new IllegalArgumentException(
          "Cannot merge HLL sketches with different precisions: " + p + " vs " + other.p);
    }
    for (int i = 0; i < m; i++) {
      if (other.registers[i] > registers[i]) {
        registers[i] = other.registers[i];
      }
    }
  }

  /**
   * Returns the estimated cardinality (number of distinct elements added).
   * Includes small-range (linear counting) and large-range corrections per the original
   * Flajolet et al. HLL paper.
   *
   * @return estimated cardinality, or 0 if no elements have been added
   */
  public long estimate() {
    double sum = 0.0;
    int zeroCount = 0;
    for (int i = 0; i < m; i++) {
      sum += 1.0 / (1L << (registers[i] & 0xFF));
      if (registers[i] == 0) {
        zeroCount++;
      }
    }

    double estimate = alphaM / sum;

    // Small range correction (linear counting)
    if (estimate <= 2.5 * m && zeroCount > 0) {
      estimate = m * Math.log((double) m / zeroCount);
    }

    // Large range correction (Flajolet et al.)
    double twoTo32 = 1L << 32;
    if (estimate > twoTo32 / 30.0) {
      estimate = -twoTo32 * Math.log(1.0 - estimate / twoTo32);
    }

    return Math.round(estimate);
  }

  /** Returns true if no elements have been added. */
  public boolean isEmpty() {
    for (byte b: registers) {
      if (b != 0) {
        return false;
      }
    }
    return true;
  }

  /** Resets the sketch to its initial empty state. */
  public void reset() {
    Arrays.fill(registers, (byte) 0);
  }

  /** Returns the precision (p) of this sketch. */
  public int getPrecision() {
    return p;
  }

  /** Returns the number of registers (2^p). */
  public int getRegisterCount() {
    return m;
  }

  /** Creates a deep copy of this sketch. */
  public HyperLogLogSketch copy() {
    HyperLogLogSketch copySketch = new HyperLogLogSketch(p);
    System.arraycopy(registers, 0, copySketch.registers, 0, m);
    return copySketch;
  }

  // ---- Serialization ----

  /**
   * Serializes this sketch to a byte array.
   *
   * Format: [1 byte: precision] [2^p bytes: registers]
   *
   * @return the serialized bytes (length = 1 + 2^p)
   */
  public byte[] toBytes() {
    byte[] bytes = new byte[1 + m];
    bytes[0] = (byte) p;
    System.arraycopy(registers, 0, bytes, 1, m);
    return bytes;
  }

  /**
   * Serializes this sketch into a ByteBuffer (for Avro bytes fields, PubSub headers, etc.).
   *
   * @return a ByteBuffer wrapping the serialized bytes
   */
  public ByteBuffer toByteBuffer() {
    return ByteBuffer.wrap(toBytes());
  }

  /**
   * Deserializes a sketch from a byte array.
   *
   * @param bytes the serialized bytes (produced by {@link #toBytes()})
   * @return the deserialized sketch
   * @throws IllegalArgumentException if the input is malformed
   */
  public static HyperLogLogSketch fromBytes(byte[] bytes) {
    if (bytes == null || bytes.length < 2) {
      throw new IllegalArgumentException("Invalid HLL bytes: null or too short");
    }
    int precision = bytes[0] & 0xFF;
    if (precision < MIN_PRECISION || precision > MAX_PRECISION) {
      throw new IllegalArgumentException("Invalid HLL precision in serialized data: " + precision);
    }
    int expectedLength = 1 + (1 << precision);
    if (bytes.length != expectedLength) {
      throw new IllegalArgumentException(
          "Invalid HLL bytes length: expected " + expectedLength + " for p=" + precision + ", got " + bytes.length);
    }
    byte[] registers = Arrays.copyOfRange(bytes, 1, bytes.length);
    return new HyperLogLogSketch(precision, registers);
  }

  /**
   * Deserializes a sketch from a ByteBuffer (for reading from Avro bytes fields, etc.).
   *
   * @param buffer the buffer containing serialized HLL data
   * @return the deserialized sketch
   * @throws IllegalArgumentException if the input is malformed
   */
  public static HyperLogLogSketch fromByteBuffer(ByteBuffer buffer) {
    if (buffer == null) {
      throw new IllegalArgumentException("Buffer cannot be null");
    }
    byte[] bytes = new byte[buffer.remaining()];
    buffer.duplicate().get(bytes);
    return fromBytes(bytes);
  }

  // ---- Hash function ----

  /**
   * Produces a well-distributed 64-bit hash from arbitrary byte arrays.
   * Uses FNV-1a to fold the input bytes into a single long, then applies the
   * MurmurHash3 fmix64 avalanche step for final bit mixing.
   *
   * @param key the byte array to hash (must not be null)
   * @return a 64-bit hash value
   * @throws IllegalArgumentException if key is null
   */
  public static long hash64(byte[] key) {
    if (key == null) {
      throw new IllegalArgumentException("Key must not be null");
    }
    // FNV-1a to fold variable-length input into 64 bits
    long h = 0xcbf29ce484222325L; // FNV offset basis
    for (byte b: key) {
      h ^= b;
      h *= 0x100000001b3L; // FNV prime
    }
    // MurmurHash3 fmix64 avalanche
    h ^= h >>> 33;
    h *= 0xff51afd7ed558ccdL;
    h ^= h >>> 33;
    h *= 0xc4ceb9fe1a85ec53L;
    h ^= h >>> 33;
    return h;
  }

  // ---- Internal ----

  private static double computeAlpha(int m) {
    if (m == 16) {
      return 0.673;
    } else if (m == 32) {
      return 0.697;
    } else if (m == 64) {
      return 0.709;
    } else {
      return 0.7213 / (1.0 + 1.079 / m);
    }
  }
}

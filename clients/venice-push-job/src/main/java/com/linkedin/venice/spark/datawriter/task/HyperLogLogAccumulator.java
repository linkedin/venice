package com.linkedin.venice.spark.datawriter.task;

import java.util.Arrays;
import org.apache.spark.util.AccumulatorV2;


/**
 * A Spark accumulator that maintains a HyperLogLog sketch for estimating the cardinality
 * (number of unique elements) of a stream of byte array keys.
 *
 * Used during repush to track the approximate number of unique keys read from the source
 * version topic, enabling comparison with the exact write-side count for pipeline integrity
 * verification.
 *
 * Implementation uses p=14 (16384 registers) giving ~0.8% standard error and 16KB memory per task.
 */
public class HyperLogLogAccumulator extends AccumulatorV2<byte[], Long> {
  private static final long serialVersionUID = 1L;

  /** Precision parameter. 2^P registers. p=14 gives ~0.8% standard error. */
  private static final int P = 14;
  private static final int M = 1 << P; // 16384
  private static final double ALPHA_M;

  static {
    // Bias correction constant for p=14
    ALPHA_M = 0.7213 / (1.0 + 1.079 / M) * M * M;
  }

  private byte[] registers;

  public HyperLogLogAccumulator() {
    this.registers = new byte[M];
  }

  @Override
  public boolean isZero() {
    for (byte b: registers) {
      if (b != 0) {
        return false;
      }
    }
    return true;
  }

  @Override
  public AccumulatorV2<byte[], Long> copy() {
    HyperLogLogAccumulator newAcc = new HyperLogLogAccumulator();
    System.arraycopy(this.registers, 0, newAcc.registers, 0, M);
    return newAcc;
  }

  @Override
  public void reset() {
    Arrays.fill(registers, (byte) 0);
  }

  @Override
  public void add(byte[] key) {
    if (key == null || key.length == 0) {
      return;
    }
    long hash = hash64(key);
    int registerIndex = (int) (hash >>> (64 - P));
    // Count leading zeros of the remaining bits + 1
    long remainingBits = (hash << P) | (1L << (P - 1)); // ensure at least 1 bit set
    int rho = Long.numberOfLeadingZeros(remainingBits) + 1;
    if (rho > registers[registerIndex]) {
      registers[registerIndex] = (byte) rho;
    }
  }

  @Override
  public void merge(AccumulatorV2<byte[], Long> other) {
    HyperLogLogAccumulator otherHll = (HyperLogLogAccumulator) other;
    for (int i = 0; i < M; i++) {
      if (otherHll.registers[i] > registers[i]) {
        registers[i] = otherHll.registers[i];
      }
    }
  }

  @Override
  public Long value() {
    return estimate();
  }

  /** Returns the estimated cardinality. */
  public long estimate() {
    double sum = 0.0;
    int zeroCount = 0;
    for (int i = 0; i < M; i++) {
      sum += 1.0 / (1L << registers[i]);
      if (registers[i] == 0) {
        zeroCount++;
      }
    }

    double estimate = ALPHA_M / sum;

    // Small range correction (linear counting)
    if (estimate <= 2.5 * M && zeroCount > 0) {
      estimate = M * Math.log((double) M / zeroCount);
    }

    return Math.round(estimate);
  }

  /**
   * Produces a well-distributed 64-bit hash from arbitrary byte arrays.
   * Uses FNV-1a to fold the input bytes into a single long, then applies the
   * MurmurHash3 fmix64 avalanche step for final bit mixing.
   */
  private static long hash64(byte[] key) {
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
}

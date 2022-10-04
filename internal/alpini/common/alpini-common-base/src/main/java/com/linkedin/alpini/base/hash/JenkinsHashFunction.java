package com.linkedin.alpini.base.hash;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;


/**
 * Hash algorithm by Bob Jenkins, 1996.
 *
 * You may use this code any way you wish, private, educational, or commercial.  It's free.
 * See: http://burtleburtle.net/bob/hash/doobs.html
 *
 * Use for hash table lookup, or anything where one collision in 2^^32
 * is acceptable.  Do NOT use for cryptographic purposes.
 *
 * Reimplementation from http://burtleburtle.net/bob/c/lookup3.c to match test vectors
 */
public final class JenkinsHashFunction implements HashFunction {

  // internal variables used in the various calculations
  int _a;
  int _b;
  int _c;

  private static int rot(int x, int k) {
    return (x << k) | (x >>> (32 - k));
  }

  private void mix() {
    _a -= _c;
    _a ^= rot(_c, 4);
    _c += _b; // SUPPRESS CHECKSTYLE OneStatementPerLine
    _b -= _a;
    _b ^= rot(_a, 6);
    _a += _c; // SUPPRESS CHECKSTYLE OneStatementPerLine
    _c -= _b;
    _c ^= rot(_b, 8);
    _b += _a; // SUPPRESS CHECKSTYLE OneStatementPerLine
    _a -= _c;
    _a ^= rot(_c, 16);
    _c += _b; // SUPPRESS CHECKSTYLE OneStatementPerLine
    _b -= _a;
    _b ^= rot(_a, 19);
    _a += _c; // SUPPRESS CHECKSTYLE OneStatementPerLine
    _c -= _b;
    _c ^= rot(_b, 4);
    _b += _a; // SUPPRESS CHECKSTYLE OneStatementPerLine
  }

  private void fin() {
    _c ^= _b;
    _c -= rot(_b, 14); // SUPPRESS CHECKSTYLE OneStatementPerLine
    _a ^= _c;
    _a -= rot(_c, 11); // SUPPRESS CHECKSTYLE OneStatementPerLine
    _b ^= _a;
    _b -= rot(_a, 25); // SUPPRESS CHECKSTYLE OneStatementPerLine
    _c ^= _b;
    _c -= rot(_b, 16); // SUPPRESS CHECKSTYLE OneStatementPerLine
    _a ^= _c;
    _a -= rot(_c, 4); // SUPPRESS CHECKSTYLE OneStatementPerLine
    _b ^= _a;
    _b -= rot(_a, 14); // SUPPRESS CHECKSTYLE OneStatementPerLine
    _c ^= _b;
    _c -= rot(_b, 24); // SUPPRESS CHECKSTYLE OneStatementPerLine
  }

  private void addAndMixIn(IntBuffer k) {
    _a += k.get();
    _b += k.get();
    _c += k.get();
    mix();
  }

  private static final int[] MASK = { 0xffffffff, 0xff, 0xffff, 0xffffff };

  @SuppressWarnings("SF_SWITCH_FALLTHROUGH")
  private void addAndMixInRemainder(IntBuffer k, int length) {
    if (k.order() == ByteOrder.LITTLE_ENDIAN) {
      switch (length) {
        case 12:
        case 11:
        case 10:
        case 9:
          _c += k.get(k.position() + 2) & MASK[length & 3];
          length = 0;
          // fall through
        case 8:
        case 7:
        case 6:
        case 5:
          _b += k.get(k.position() + 1) & MASK[length & 3];
          length = 0;
          // fall through
        case 4:
        case 3:
        case 2:
        case 1:
          _a += k.get(k.position()) & MASK[length & 3];
          fin();
          // fall through
        case 0:
          return;
        default:
      }
    }
    throw new UnsupportedOperationException();
  }

  private int init(int length, int initval) {
    _a = _b = _c = 0xdeadbeef + length + initval; // SUPPRESS CHECKSTYLE InnerAssignment
    return length;
  }

  private int init(int length, long initval) {
    init(length, (int) initval);
    _c += (int) (initval >>> 32);
    return length;
  }

  private int intValue() {
    return _c;
  }

  private long longValue() {
    return (0xffffffffL & _c) | (((long) _b) << 32);
  }

  private JenkinsHashFunction hashwords(IntBuffer k, int length) {
    while (length > 12) {
      addAndMixIn(k);
      length -= 12;
    }
    addAndMixInRemainder(k, length);
    return this;
  }

  private ByteBuffer pad(ByteBuffer k, int length) {
    int rem = length % 12;
    ByteBuffer buffer;
    if (rem == 0) {
      buffer = k;
    } else {
      // Copy and pad
      buffer = ByteBuffer.allocate(length + 12 - rem).put(k).put(new byte[12 - rem]);
      buffer.flip();
    }
    return buffer;
  }

  private JenkinsHashFunction hashwords(ByteBuffer k, int length, ByteOrder endian) {
    return hashwords(pad(k, length).duplicate().order(endian).asIntBuffer(), length);
  }

  public int hashword(IntBuffer k, int initval) {
    return hashwords(k.duplicate(), init(k.remaining() << 2, initval)).intValue();
  }

  public long hashword2(IntBuffer k, long initval) {
    return hashwords(k.duplicate(), init(k.remaining() << 2, initval)).longValue();
  }

  public int hashlittle(ByteBuffer k, int initval) {
    return hashwords(k, init(k.remaining(), initval), ByteOrder.LITTLE_ENDIAN).intValue();
  }

  public long hashlittle2(ByteBuffer k, long initval) {
    return hashwords(k, init(k.remaining(), initval), ByteOrder.LITTLE_ENDIAN).longValue();
  }

  @Override
  public long hash(ByteBuffer buf) {
    buf = buf.duplicate();
    buf.position(0);
    return hashlittle2(buf, 0);
  }

  @Override
  public long hash(ByteBuffer buf, int off, int len) {
    buf = buf.duplicate();
    buf.position(off).limit(len);
    return hashlittle2(buf, 0);
  }

  @Override
  public long hash(byte[] key, int numBuckets) {
    return modulus(hash(ByteBuffer.wrap(key)), numBuckets);
  }

  @Override
  public long hash(long key, int numBuckets) {
    ByteBuffer buffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(key);
    buffer.flip();
    return modulus(hashword2(buffer.asIntBuffer(), 0), numBuckets);
  }

  // Should always produce a positive correct result. see Math.floorMod
  private static long modulus(long value, int numBuckets) {
    return (Math.floorMod(value, numBuckets) + Math.abs(numBuckets)) % Math.abs(numBuckets);
  }
}

package com.linkedin.alpini.base.hash;

import java.nio.ByteBuffer;
import javax.annotation.Nonnull;


/**
 * Forked from com.linkedin.databus.core.util @ r293057
 * @author sdas
 *
 */
public final class FnvHashFunction implements HashFunction {
  private static final long FNV_BASIS = 0x811c9dc5;
  private static final long FNV_PRIME = (1 << 24) + 0x193;

  @Override
  public long hash(@Nonnull ByteBuffer buf) {
    int length = buf.position() + buf.remaining();
    return hash(buf, 0, length);
  }

  @Override
  public long hash(@Nonnull ByteBuffer buf, int off, int len) {
    long hash = FNV_BASIS;

    int last = Math.min(off + len, buf.position() + buf.remaining());

    for (int i = off; i < last; i++) {
      hash ^= 0xFF & buf.get(i);
      hash *= FNV_PRIME;
    }
    return hash;
  }

  public long hash(@Nonnull byte[] key) {
    long hash = FNV_BASIS;
    for (byte aKey: key) {
      hash ^= 0xFF & aKey;
      hash *= FNV_PRIME;
    }

    return hash;
  }

  @Override
  public long hash(@Nonnull byte[] key, int numBuckets) {
    return hash(key) % numBuckets;
  }

  private long hash(long val) {
    long hashval = FNV_BASIS;

    for (int i = 0; i < 8; i++) {
      long octet = val & 0x00ff;
      val = val >> 8;

      hashval = hashval ^ octet;
      hashval = hashval * FNV_PRIME;
    }
    return (int) hashval;
  }

  @Override
  public long hash(long val, int numBuckets) {
    return hash(val) % numBuckets;
  }

  /*
  public static void main(String[] args)
  {
    byte[] b = new byte[1024*1024*100];
    ByteBuffer buf = ByteBuffer.allocateDirect(1024*1024*100).order(DbusEvent.byteOrder);
    Random r = new Random();
    r.nextBytes(b);
    buf.put(b);
  
    FnvHashFunction fun = new FnvHashFunction();
    CRC32 chksum = new CRC32();
    JenkinsHashFunction jFun = new JenkinsHashFunction();
  
    long start = 0;
    long end = 0;
    long hash = 0;
    long diff = 0;
    long delayMicro = 0;
  
    chksum.reset();
    chksum.update(b);
    long prevhash = chksum.getValue();
    for (int i = 0; i < 10; i++)
    {
      start = System.nanoTime();
      chksum.reset();
      chksum.update(b);
      hash = chksum.getValue();
      end = System.nanoTime();
      assert(prevhash == hash);
      diff += (end - start);
    }
  
    delayMicro = (diff/1000)/10;
  
    System.out.println("Latency of System CRC32 (Micro Seconds) is: " + delayMicro);
  
    prevhash = fun.hash(b);
    for (int i = 0; i < 10; i++)
    {
      start = System.nanoTime();
      hash = fun.hash(b);
      end = System.nanoTime();
      assert(prevhash == hash);
      diff += (end - start);
    }
    delayMicro = (diff/1000)/10;
    System.out.println("Latency of FNV (Micro Seconds)  is: " + delayMicro);
  
    prevhash = jFun.hash(b);
    for (int i = 0; i < 10; i++)
    {
      start = System.nanoTime();
      hash = jFun.hash(b);
      end = System.nanoTime();
      assert(prevhash == hash);
      diff += (end - start);
    }
    delayMicro = (diff/1000)/10;
    System.out.println("Latency of Jenkins (Micro Seconds)  is: " + delayMicro);
  
    prevhash = ByteBufferCRC32.getChecksum(b);
    for (int i = 0; i < 10; i++)
    {
      start = System.nanoTime();
      hash = ByteBufferCRC32.getChecksum(b);
      end = System.nanoTime();
      assert(prevhash == hash);
      diff += (end - start);
    }
    delayMicro = (diff/1000)/10;
    System.out.println("Latency of ByteBufferCRC32 (Micro Seconds)  is: " + delayMicro);
  
    //System.out.println("Buffer position-Remaining :" + buf.position() + "-" + buf.remaining());
  
    prevhash = fun.hash(buf);
    for (int i = 0; i < 10; i++)
    {
      start = System.nanoTime();
      hash = fun.hash(buf);
      end = System.nanoTime();
      assert(prevhash == hash);
      diff += (end - start);
    }
    delayMicro = (diff/1000)/10;
    System.out.println("Latency of FNV (Micro Seconds) for ByteBuffer is: " + delayMicro);
    //System.out.println("Buffer position-Remaining :" + buf.position() + "-" + buf.remaining());
  
    prevhash = fun.hash(buf);
    for (int i = 0; i < 10; i++)
    {
      start = System.nanoTime();
      hash = fun.hash(buf);
      end = System.nanoTime();
      assert(prevhash == hash);
      diff += (end - start);
    }
    delayMicro = (diff/1000)/10;
    System.out.println("Latency of Jenkins (Micro Seconds) for ByteBuffer is: " + delayMicro);
    //System.out.println("Buffer position-Remaining :" + buf.position() + "-" + buf.remaining());
    prevhash = ByteBufferCRC32.getChecksum(buf);
    for (int i = 0; i < 10; i++)
    {
      start = System.nanoTime();
      hash = ByteBufferCRC32.getChecksum(buf);
      end = System.nanoTime();
      assert(prevhash == hash);
      diff += (end - start);
    }
    delayMicro = (diff/1000)/10;
    System.out.println("Latency of ByteBufferCRC32 (Micro Seconds)  for ByteBuffer is: " + delayMicro);
  
    //System.out.println("Buffer position-Remaining :" + buf.position() + "-" + buf.remaining());
  }
  */
}

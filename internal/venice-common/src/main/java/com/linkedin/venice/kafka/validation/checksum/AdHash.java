package com.linkedin.venice.kafka.validation.checksum;

import com.linkedin.venice.utils.ByteUtils;
import java.util.zip.CRC32;


/**
 * This class AdHash is used to keep track of the checksum of the data incrementally.
 * It adopts the idea of AdHash that:
 * " A message x' which I want to hash may be a simple modification of a message x which I previously
 *   hashed. If I have already computed the hash f(x) of x then, rather than re-computing f(x') from
 *   scratch, I would like to just quickly 'update' the old hash value f(x) to the new value f(x').
 *   An incremental hash function is one that permits this. "
 *
 * See the paper for details:
 *   A New Paradigm for Collision-free Hashing: Incrementality at Reduced Cost.
 *   (https://cseweb.ucsd.edu/~daniele/papers/IncHash.pdf)
 *
 * Its adoption in ZooKeeper:
 *   Verify, And Then Trust: Data Inconsistency Detection in ZooKeeper.
 *   (https://dl.acm.org/doi/abs/10.1145/3578358.3591328)
 */

public class AdHash extends CheckSum {
  private volatile long incrementalDigest;
  private final CRC32 crc32 = new CRC32();

  public AdHash() {
    incrementalDigest = 0;
  }

  public AdHash(byte[] encodedState) {
    incrementalDigest = ByteUtils.readLong(encodedState, 0);
  }

  @Override
  public byte[] getFinalCheckSum() {
    byte[] finalCheckSum = new byte[ByteUtils.SIZE_OF_LONG];
    ByteUtils.writeLong(finalCheckSum, incrementalDigest, 0);
    return finalCheckSum;
  }

  /**
   * Update the checksum with the input data. Notice that the internal CRC32 object is
   * reset everytime before updating the checksum. This is to ensure that the checksum
   * is always calculated from the beginning of the data and checkpoint-able.
   * This is not thread-safe, but the caller should ensure that the object is not shared.
   */
  @Override
  public void updateChecksum(byte[] input, int startIndex, int length) {
    crc32.reset(); // reset the digest.
    crc32.update(input, startIndex, length);

    // incrementingDigest overflow can happen, but that is fine, we can let the long integer to rotate.
    incrementalDigest += crc32.getValue();
  }

  @Override
  public void resetInternal() {
    crc32.reset();
    incrementalDigest = 0;
  }

  @Override
  public CheckSumType getType() {
    return CheckSumType.ADHASH;
  }

  @Override
  public byte[] getEncodedState() {
    return getFinalCheckSum();
  }
}

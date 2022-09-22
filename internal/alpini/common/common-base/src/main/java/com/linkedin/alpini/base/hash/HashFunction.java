package com.linkedin.alpini.base.hash;

import java.nio.ByteBuffer;


/**
 * Forked from com.linkedin.databus.core.util @ r293057
 * @author sdas
 *
 */
public interface HashFunction {

  /*
   * Generates Hash for entire byte buffer
   * @param buf : ByteBuffer for which hash needs to be computed
   * @return hash value of buffer
   */
  long hash(ByteBuffer buf);

  /*
   * Generates Hash for a section of byte buffer denoted by its
   * endpoints
   *
   * @param buf : ByteBuffer for which hash needs to be computed
   * @param off : Starting Offset
   * @param len : Length of the section for hash computation
   * @return the hash value for the section of the buffer
   */
  long hash(ByteBuffer buf, int off, int len);

  /*
   * Generates hash for the byte array and bucketize the value to
   * 0.. (numBuckets - 1)
   *
   * @param key : Array to apply hash and bucketize
   * @param numBuckets : Number of buckets for bucketization
   *
   * @return Returns the bucket in the range 0..(numBuckets - 1)
   */
  long hash(byte[] key, int numBuckets);

  /*
   * Generates hash for the key and bucketize the value to
   * 0.. (numBuckets - 1)
   *
   * @param key : Input key for which hash needs to be calculated
   * @param numBuckets : Number of buckets for bucketization
   *
   * @return Returns the bucket in the range 0..(numBuckets - 1)
   */
  long hash(long key, int numBuckets);

}

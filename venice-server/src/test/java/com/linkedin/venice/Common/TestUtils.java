package com.linkedin.venice.Common;

import java.util.Random;


/**
 * Helper utilities for tests
 *
 *
 */
public class TestUtils {

  public static final Long SEED = System.currentTimeMillis();
  public static final Random SEEDED_RANDOM = new Random(SEED);

  /**
   * Generate an array of random bytes
   *
   * @param length length of the byte array to be generated
   * @return
   */
  public static byte[] getRandomBytes(int length) {
    byte[] bytes = new byte[length];
    SEEDED_RANDOM.nextBytes(bytes);
    return bytes;
  }

  /**
   * Return a random integer between 0 and max
   *
   * @param max The upper bound for the range of numbers
   * @return
   */
  public static int getRandomIntInRange(int max){
    return SEEDED_RANDOM.nextInt(max);
  }
}

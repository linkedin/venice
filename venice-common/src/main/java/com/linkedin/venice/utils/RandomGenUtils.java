package com.linkedin.venice.utils;

import java.util.Random;


/**
 * Helper utilities for tests
 *
 *
 */
public class RandomGenUtils {

  public static final Long SEED = System.currentTimeMillis();
  public static final Random SEEDED_RANDOM = new Random(SEED);
  private static final CharSequence VENICE_TEST_DATA_DIRECTORY_NAME = "venice-test-data";

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
   * @return a random float
   */
  public static float getRandomFloat() {
    return SEEDED_RANDOM.nextFloat();
  }

  /**
   * Return a random integer between 0 and max
   *
   * @param max The upper bound for the range of numbers
   * @return
   */
  public static int getRandomIntWithIn(int max){
    return SEEDED_RANDOM.nextInt(max);
  }

  /**
   *
   * @param min Minimum Value
   * @param max Maximum value. max must be greater than min.
   * @return Integer between min and max, inclusive.
   */
  public static int getRandomIntInRange(int min, int max){
    return (SEEDED_RANDOM.nextInt((max - min) + 1) + min);
  }
}

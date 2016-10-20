/*
 * Copyright 2008-2009 LinkedIn, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.linkedin.venice.kafka.validation.checksum;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.ByteUtils;
import org.apache.log4j.Logger;

import java.util.Optional;


/**
 * Parent class for various running checksum implementations.
 *
 * N.B.: Class taken from Voldemort.
 */
public abstract class CheckSum {

  private static final Logger LOGGER = Logger.getLogger(CheckSum.class);

  /**
   * Update the checksum buffer to include input with startIndex and length.
   * Following calls to multiple 'update's you need to call 'getCheckSum'
   * which will reset the buffer as well
   *
   * @param input
   * @param startIndex
   * @param length
   */
  public abstract void update(byte[] input, int startIndex, int length);

  /**
   * Get the checkSum of the buffer till now, after which buffer is reset
   */
  public abstract byte[] getCheckSum();

  /**
   * Reset the checksum generator
   */
  public abstract void reset();

  public abstract CheckSumType getType();

  public byte[] getEncodedState() {
    throw new VeniceException(getType() + " does not support accessing the partially encoded state!");
  }

  /**
   * Update the underlying buffer using the integer
   *
   * @param number number to be stored in checksum buffer
   */
  public void update(int number) {
    byte[] numberInBytes = new byte[ByteUtils.SIZE_OF_INT];
    ByteUtils.writeInt(numberInBytes, number, 0);
    update(numberInBytes);
  }

  /**
   * Update the underlying buffer using the short
   *
   * @param number number to be stored in checksum buffer
   */
  public void update(short number) {
    byte[] numberInBytes = new byte[ByteUtils.SIZE_OF_SHORT];
    ByteUtils.writeShort(numberInBytes, number, 0);
    update(numberInBytes);
  }

  /**
   * Update the checksum buffer to include input
   *
   * @param input bytes added to the buffer
   */
  public void update(byte[] input) {
    update(input, 0, input.length);
  }

  public static Optional<CheckSum> getInstance(CheckSumType type) {
    switch (type) {
      case NONE: return Optional.empty();
      case ADLER32: return Optional.of(new Adler32CheckSum());
      case CRC32: return Optional.of(new CRC32CheckSum());
      case MD5: return Optional.of(new MD5CheckSum());
      default: return Optional.empty();
    }
  }

  public static Optional<CheckSum> getInstance(CheckSumType type, byte[] encodedState) {
    if (type.isCheckpointingSupported()) {
      switch (type) {
        case NONE: return Optional.empty();
        case MD5: return Optional.of(new MD5CheckSum(encodedState));
        default: return Optional.empty();
      }
    } else {
      // TODO: Consider throwing exception here, instead.
      LOGGER.warn("CheckSum.getInstance(type, encodedState) called for a type which does not support checkpointing: " + type);
      // Not a very big deal since the default checksumming strategy is MD5 anyway.
      return Optional.empty();
    }
  }

  @Override
  public String toString() {
    return getType().toString();
  }
}

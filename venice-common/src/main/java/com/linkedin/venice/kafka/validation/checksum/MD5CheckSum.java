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

import org.bouncycastle.crypto.digests.MD5Digest;


/**
 * Running checksum implementation based on BouncyCastle's implementation of MD5.
 */
public class MD5CheckSum extends CheckSum {

  private MD5Digest checksum = null;

  public MD5CheckSum() {
    checksum = new MD5Digest();
  }

  public MD5CheckSum(byte[] encodedState) {
    checksum = new MD5Digest(encodedState);
  }

  @Override
  public byte[] getCheckSum() {
    byte[] finalChecksum = new byte[checksum.getDigestSize()];
    checksum.doFinal(finalChecksum, 0);
    return finalChecksum;
  }

  @Override
  public void update(byte[] input, int startIndex, int length) {
    checksum.update(input, startIndex, length);
  }

  @Override
  public void reset() {
    checksum.reset();
  }

  @Override
  public CheckSumType getType() {
    return CheckSumType.MD5;
  }

  @Override
  public byte[] getEncodedState() {
    return checksum.getEncodedState();
  }
}

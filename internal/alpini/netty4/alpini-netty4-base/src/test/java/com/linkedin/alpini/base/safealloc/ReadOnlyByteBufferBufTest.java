/*
 * Copyright 2013 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.linkedin.alpini.base.safealloc;

import static org.testng.AssertJUnit.assertEquals;

import io.netty.buffer.ByteBuf;
import io.netty.util.internal.PlatformDependent;
import java.nio.ByteBuffer;
import org.testng.annotations.Test;


public class ReadOnlyByteBufferBufTest extends ReadOnlyDirectByteBufferBufTest {
  @Override
  protected ByteBuffer allocate(int size) {
    return ByteBuffer.allocate(size);
  }

  @Test(groups = "unit")
  public void testCopyDirect() {
    testCopy(true);
  }

  @Test(groups = "unit")
  public void testCopyHeap() {
    testCopy(false);
  }

  private void testCopy(boolean direct) {
    byte[] bytes = new byte[1024];
    PlatformDependent.threadLocalRandom().nextBytes(bytes);

    ByteBuffer nioBuffer = direct ? ByteBuffer.allocateDirect(bytes.length) : ByteBuffer.allocate(bytes.length);
    nioBuffer.put(bytes).flip();

    ByteBuf buf = buffer(nioBuffer.asReadOnlyBuffer());
    ByteBuf copy = buf.copy();

    assertEquals(buf, copy);
    assertEquals(buf.alloc(), copy.alloc());
    assertEquals(buf.isDirect(), copy.isDirect());

    copy.release();
    buf.release();
  }
}

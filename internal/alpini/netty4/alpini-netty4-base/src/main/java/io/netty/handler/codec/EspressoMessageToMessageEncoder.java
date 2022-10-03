/*
 * Copyright 2012 The Netty Project
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
package io.netty.handler.codec;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.PromiseCombiner;
import io.netty.util.internal.StringUtil;
import java.util.List;


/**
 * Forked from Netty 4.1.42.Final. Main change is to avoid creating {@link io.netty.util.internal.TypeParameterMatcher}.
 *
 * {@link ChannelOutboundHandlerAdapter} which encodes from one message to an other message
 *
 * For example here is an implementation which decodes an {@link Integer} to an {@link String}.
 *
 * <pre>
 *     public class IntegerToStringEncoder extends
 *             {@link MessageToMessageEncoder}&lt;{@link Integer}&gt; {
 *
 *         {@code @Override}
 *         public void encode({@link ChannelHandlerContext} ctx, {@link Integer} message, List&lt;Object&gt; out)
 *                 throws {@link Exception} {
 *             out.add(message.toString());
 *         }
 *     }
 * </pre>
 *
 * Be aware that you need to call {@link io.netty.util.ReferenceCounted#retain()} on messages that are just passed through if they
 * are of type {@link io.netty.util.ReferenceCounted}. This is needed as the {@link MessageToMessageEncoder} will call
 * {@link io.netty.util.ReferenceCounted#release()} on encoded messages.
 */
public abstract class EspressoMessageToMessageEncoder<I> extends ChannelOutboundHandlerAdapter {
  /**
   * Create a new instance which will try to detect the types to match out of the type parameter of the class.
   */
  protected EspressoMessageToMessageEncoder() {
  }

  /**
   * Create a new instance
   *
   * @param inboundMessageType    The type of messages to match and so decode
   */
  protected EspressoMessageToMessageEncoder(Class<? extends I> inboundMessageType) {
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    CodecOutputList out = null;
    try {
      if (acceptOutboundMessage(msg)) {
        out = CodecOutputList.newInstance();
        @SuppressWarnings("unchecked")
        I cast = (I) msg;
        try {
          encode(ctx, cast, out);
        } finally {
          ReferenceCountUtil.release(cast);
        }

        if (out.isEmpty()) {
          out.recycle();
          out = null;

          throw new EncoderException(StringUtil.simpleClassName(this) + " must produce at least one message.");
        }
      } else {
        ctx.write(msg, promise);
      }
    } catch (EncoderException e) {
      throw e;
    } catch (Throwable t) {
      throw new EncoderException(t);
    } finally {
      if (out != null) {
        final int sizeMinusOne = out.size() - 1;
        if (sizeMinusOne == 0) {
          ctx.write(out.getUnsafe(0), promise);
        } else if (sizeMinusOne > 0) {
          // Check if we can use a voidPromise for our extra writes to reduce GC-Pressure
          // See https://github.com/netty/netty/issues/2525
          if (promise == ctx.voidPromise()) {
            writeVoidPromise(ctx, out);
          } else {
            writePromiseCombiner(ctx, out, promise);
          }
        }
        out.recycle();
      }
    }
  }

  protected boolean acceptOutboundMessage(Object msg) throws Exception {
    // Return true directly. Don't call TypeParameterMatcher
    return true;
  }

  private static void writeVoidPromise(ChannelHandlerContext ctx, CodecOutputList out) {
    final ChannelPromise voidPromise = ctx.voidPromise();
    for (int i = 0; i < out.size(); i++) {
      ctx.write(out.getUnsafe(i), voidPromise);
    }
  }

  private static void writePromiseCombiner(ChannelHandlerContext ctx, CodecOutputList out, ChannelPromise promise) {
    final PromiseCombiner combiner = new PromiseCombiner(ctx.executor());
    for (int i = 0; i < out.size(); i++) {
      combiner.add(ctx.write(out.getUnsafe(i)));
    }
    combiner.finish(promise);
  }

  /**
   * Encode from one message to an other. This method will be called for each written message that can be handled
   * by this encoder.
   *
   * @param ctx           the {@link ChannelHandlerContext} which this {@link MessageToMessageEncoder} belongs to
   * @param msg           the message to encode to an other one
   * @param out           the {@link List} into which the encoded msg should be added
   *                      needs to do some kind of aggregation
   * @throws Exception    is thrown if an error occurs
   */
  protected abstract void encode(ChannelHandlerContext ctx, I msg, List<Object> out) throws Exception;
}

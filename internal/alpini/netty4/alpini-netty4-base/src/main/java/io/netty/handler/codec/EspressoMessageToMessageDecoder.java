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
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import java.util.List;


/**
 * Forked from Netty 4.1.42.Final. Main change is to avoid creating {@link io.netty.util.internal.TypeParameterMatcher}.
 *
 * {@link ChannelInboundHandlerAdapter} which decodes from one message to an other message.
 *
 *
 * For example here is an implementation which decodes a {@link String} to an {@link Integer} which represent
 * the length of the {@link String}.
 *
 * <pre>
 *     public class StringToIntegerDecoder extends
 *             {@link MessageToMessageDecoder}&lt;{@link String}&gt; {
 *
 *         {@code @Override}
 *         public void decode({@link ChannelHandlerContext} ctx, {@link String} message,
 *                            List&lt;Object&gt; out) throws {@link Exception} {
 *             out.add(message.length());
 *         }
 *     }
 * </pre>
 *
 * Be aware that you need to call {@link io.netty.util.ReferenceCounted#retain()} on messages that are just passed through if they
 * are of type {@link io.netty.util.ReferenceCounted}. This is needed as the {@link MessageToMessageDecoder} will call
 * {@link io.netty.util.ReferenceCounted#release()} on decoded messages.
 *
 */
public abstract class EspressoMessageToMessageDecoder<I> extends ChannelInboundHandlerAdapter {
  /**
   * Create a new instance which will try to detect the types to match out of the type parameter of the class.
   */
  protected EspressoMessageToMessageDecoder() {
  }

  /**
   * Create a new instance
   *
   * @param inboundMessageType    The type of messages to match and so decode
   */
  protected EspressoMessageToMessageDecoder(Class<? extends I> inboundMessageType) {
  }

  /**
   * Returns {@code true} if the given message should be handled. If {@code false} it will be passed to the next
   * {@link io.netty.channel.ChannelInboundHandler} in the {@link io.netty.channel.ChannelPipeline}.
   */
  public boolean acceptInboundMessage(Object msg) throws Exception {
    // Return true directly. Don't check with TypeParameterMatcher
    return true;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    CodecOutputList out = CodecOutputList.newInstance();
    try {
      if (acceptInboundMessage(msg)) {
        @SuppressWarnings("unchecked")
        I cast = (I) msg;
        try {
          decode(ctx, cast, out);
        } finally {
          ReferenceCountUtil.release(cast);
        }
      } else {
        out.add(msg);
      }
    } catch (DecoderException e) {
      throw e;
    } catch (Exception e) {
      throw new DecoderException(e);
    } finally {
      int size = out.size();
      for (int i = 0; i < size; i++) {
        ctx.fireChannelRead(out.getUnsafe(i));
      }
      out.recycle();
    }
  }

  /**
   * Decode from one message to an other. This method will be called for each written message that can be handled
   * by this decoder.
   *
   * @param ctx           the {@link ChannelHandlerContext} which this {@link MessageToMessageDecoder} belongs to
   * @param msg           the message to decode to an other one
   * @param out           the {@link List} to which decoded messages should be added
   * @throws Exception    is thrown if an error occurs
   */
  protected abstract void decode(ChannelHandlerContext ctx, I msg, List<Object> out) throws Exception;
}

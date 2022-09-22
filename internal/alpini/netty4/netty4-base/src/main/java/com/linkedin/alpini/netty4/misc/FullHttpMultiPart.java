package com.linkedin.alpini.netty4.misc;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpContent;


/**
 * Created by acurtis on 3/22/17.
 */
public interface FullHttpMultiPart extends HttpMultiPart, HttpContent {
  FullHttpMultiPart retain();

  FullHttpMultiPart retain(int increment);

  FullHttpMultiPart touch();

  FullHttpMultiPart touch(Object hint);

  FullHttpMultiPart copy();

  FullHttpMultiPart duplicate();

  FullHttpMultiPart retainedDuplicate();

  FullHttpMultiPart replace(ByteBuf content);

}

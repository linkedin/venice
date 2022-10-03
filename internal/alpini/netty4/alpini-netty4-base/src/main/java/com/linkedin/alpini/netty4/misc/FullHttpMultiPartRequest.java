package com.linkedin.alpini.netty4.misc;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.FullHttpRequest;
import java.util.Collection;


/**
 * Created by acurtis on 3/27/17.
 */
public interface FullHttpMultiPartRequest extends FullHttpRequest, Iterable<FullHttpMultiPart> {
  FullHttpMultiPartRequest copy();

  FullHttpMultiPartRequest duplicate();

  FullHttpMultiPartRequest retainedDuplicate();

  FullHttpMultiPartRequest replace(ByteBuf content);

  FullHttpMultiPartRequest replace(Collection<FullHttpMultiPart> parts);

  FullHttpMultiPartRequest retain(int increment);

  FullHttpMultiPartRequest retain();

  FullHttpMultiPartRequest touch();

  FullHttpMultiPartRequest touch(Object hint);

}

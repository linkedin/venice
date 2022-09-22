package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.base.misc.HeaderUtils;
import com.linkedin.alpini.netty4.misc.BasicHttpRequest;
import com.linkedin.alpini.netty4.misc.BasicHttpResponse;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.AsciiString;
import io.netty.util.ReferenceCountUtil;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


/**
 * Unwraps the content of messages which have multipart bodies as bare {@linkplain io.netty.buffer.ByteBuf} messages
 * which can then be further processed by {@link HttpContentMultiPartDecode}.
 *
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class HttpContentUnwrap extends MessageToMessageDecoder<HttpObject> {
  HttpMessage _httpMessage;
  AsciiString _boundary;
  HeaderUtils.ContentType _contentType;
  private boolean _unwrap;

  @Override
  public boolean acceptInboundMessage(Object msg) {
    return msg instanceof HttpObject;
  }

  public boolean acceptBoundary(String boundary) {
    return !boundary.isEmpty();
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, HttpObject obj, List<Object> out) {
    if (obj instanceof HttpMessage) {
      _httpMessage = (HttpMessage) obj;
      _contentType =
          HeaderUtils.parseContentType(_httpMessage.headers().get(HttpHeaderNames.CONTENT_TYPE, "text/plain"));
      _unwrap = false;

      if (_contentType.isMultipart()) {
        Iterator<Map.Entry<String, String>> it = _contentType.parameters();
        while (it.hasNext()) {
          Map.Entry<String, String> entry = it.next();
          if ("boundary".equals(entry.getKey())) {
            if (acceptBoundary(entry.getValue())) {
              _unwrap = true;
              _boundary = AsciiString.of(entry.getValue());
              break;
            }
          }
        }

        if (_unwrap) {
          if (obj instanceof HttpRequest) {
            out.add(new BasicHttpRequest((HttpRequest) obj));
            if (obj instanceof LastHttpContent) {
              obj = new DefaultLastHttpContent(((HttpContent) obj).content());
            } else {
              return;
            }
          } else if (obj instanceof HttpResponse) {
            out.add(new BasicHttpResponse((HttpResponse) obj));
            if (obj instanceof LastHttpContent) {
              obj = new DefaultLastHttpContent(((HttpContent) obj).content());
            } else {
              return;
            }
          }
        }
      }
    }

    if (_unwrap && obj instanceof HttpContent) {
      HttpContent msg = (HttpContent) obj;

      if (msg.content().isReadable()) {
        out.add(msg.content().retainedDuplicate());
      }

      if (msg instanceof LastHttpContent) {
        _unwrap = false;
        if (((LastHttpContent) msg).trailingHeaders().isEmpty()) {
          out.add(LastHttpContent.EMPTY_LAST_CONTENT);
        } else {
          out.add(msg.replace(Unpooled.EMPTY_BUFFER));
        }
      }
    } else {
      out.add(ReferenceCountUtil.retain(obj));
    }
  }
}

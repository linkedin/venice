package com.linkedin.alpini.netty4.misc;

import com.linkedin.alpini.base.cache.ByteBufHashMap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.AsciiString;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nonnull;


/**
 * Created by acurtis on 8/15/17.
 */
public class BasicHttpRequestSerializer implements ByteBufHashMap.SerDes<BasicHttpRequest> {
  private final ByteBufAllocator _alloc;

  private enum Type {
    BASIC_REQUEST,

    FULL_REQUEST {
      BasicHttpRequest deserialize(ByteBufAllocator alloc, ObjectInputStream input) throws Exception {
        BasicHttpRequest request = super.deserialize(alloc, input);
        int contentLength = input.readInt();
        ByteBuf content;
        if (contentLength == 0) {
          content = Unpooled.EMPTY_BUFFER;
        } else {
          content = alloc.buffer(contentLength);
          try {
            content.writeBytes(input, contentLength);
            content.retain();
          } finally {
            content.release();
          }
        }
        HttpHeaders trailingHeaders = deserializeHeaders(input);
        return new BasicFullHttpRequest(request, request.headers(), trailingHeaders, content);
      }

      boolean serialize(ObjectOutputStream output, BasicHttpRequest value) throws Exception {
        if (super.serialize(output, value)) {
          BasicFullHttpRequest request = (BasicFullHttpRequest) value;
          ByteBuf content = request.content();
          int readableBytes = content.readableBytes();
          output.writeInt(readableBytes);
          content.slice().readBytes(output, readableBytes);
          serializeHeaders(output, request.trailingHeaders());
          return true;
        }
        return false;
      }
    },

    MULTIPART_REQUEST {
      BasicHttpRequest deserialize(ByteBufAllocator alloc, ObjectInputStream input) throws Exception {
        BasicHttpRequest request = super.deserialize(alloc, input);
        List<FullHttpMultiPart> parts = new LinkedList<>();
        try {
          HttpHeaders partHeader = deserializeHeaders(input);
          while (partHeader != null) {
            int readableBytes = input.readInt();
            ByteBuf content;
            if (readableBytes > 0) {
              content = alloc.buffer(readableBytes);
              try {
                content.writeBytes(input, readableBytes);
                content.retain();
              } finally {
                content.release();
              }
            } else {
              content = Unpooled.EMPTY_BUFFER;
            }
            parts.add(new BasicFullHttpMultiPart(content, partHeader));
            partHeader = deserializeHeaders(input);
          }
          HttpHeaders trailingHeader = deserializeHeaders(input);
          request = new BasicFullHttpMultiPartRequest(
              request.protocolVersion(),
              request.method(),
              request.uri(),
              parts,
              request.headers(),
              trailingHeader,
              request.getRequestId(),
              request.getRequestTimestamp(),
              request.getRequestNanos());
          parts.forEach(FullHttpMultiPart::retain);
          return request;
        } finally {
          parts.forEach(FullHttpMultiPart::release);
        }
      }

      boolean serialize(ObjectOutputStream output, BasicHttpRequest value) throws Exception {
        if (super.serialize(output, value)) {
          BasicFullHttpMultiPartRequest request = (BasicFullHttpMultiPartRequest) value;
          for (FullHttpMultiPart part: request) {
            serializeHeaders(output, part.headers());
            int readableBytes = part.content().readableBytes();
            output.writeInt(readableBytes);
            part.content().slice().readBytes(output, readableBytes);
          }
          output.writeInt(-1);
          serializeHeaders(output, request.trailingHeaders());
          return true;
        }
        return false;
      }
    };

    BasicHttpRequest deserialize(ByteBufAllocator alloc, ObjectInputStream input) throws Exception {
      HttpVersion protocol = HttpVersion.valueOf(input.readUTF());
      HttpMethod method = HttpMethod.valueOf(input.readUTF());
      String uri = input.readUTF();
      long requestTimestamp = input.readLong();
      long requestNanos = input.readLong();
      UUID uuid = new UUID(input.readLong(), input.readLong());
      HttpHeaders headers = deserializeHeaders(input);
      return new BasicHttpRequest(protocol, method, uri, headers, uuid, requestTimestamp, requestNanos);
    }

    boolean serialize(ObjectOutputStream output, BasicHttpRequest value) throws Exception {
      output.writeUTF(value.protocolVersion().text());
      output.writeUTF(value.getMethodName());
      output.writeUTF(value.uri());
      output.writeLong(value.getRequestTimestamp());
      output.writeLong(value.getRequestNanos());
      output.writeLong(value.getRequestId().getMostSignificantBits());
      output.writeLong(value.getRequestId().getLeastSignificantBits());
      serializeHeaders(output, value.headers());
      return true;
    }
  }

  public BasicHttpRequestSerializer(ByteBufAllocator alloc) {
    _alloc = alloc;
  }

  @Override
  public BasicHttpRequest deserialize(@Nonnull ByteBufInputStream inputStream) {
    try (ObjectInputStream input = new ObjectInputStream(inputStream)) {
      return ((Type) input.readUnshared()).deserialize(_alloc, input);
    } catch (Throwable ex) {
      return null;
    }
  }

  @Override
  public boolean serialize(@Nonnull ByteBufOutputStream outputStream, @Nonnull BasicHttpRequest value) {
    try (ObjectOutputStream output = new ObjectOutputStream(outputStream)) {
      Type type;
      if (value instanceof BasicFullHttpMultiPartRequest) {
        type = Type.MULTIPART_REQUEST;
      } else if (value instanceof BasicFullHttpRequest) {
        type = Type.FULL_REQUEST;
      } else {
        type = Type.BASIC_REQUEST;
      }
      output.writeUnshared(type);
      return type.serialize(output, value);
    } catch (Throwable ex) {
      return false;
    }
  }

  private static byte[] readBytes(ObjectInputStream input, byte[] bytes) throws IOException {
    if (input.read(bytes) != bytes.length) {
      throw new IllegalStateException();
    }
    return bytes;
  }

  private static HttpHeaders deserializeHeaders(ObjectInputStream input) throws Exception {
    int size = input.readInt();
    if (size < 0) {
      return null;
    }
    HttpHeaders headers = new DefaultHttpHeaders(false);
    while (size != 0) {
      AsciiString key = new AsciiString(readBytes(input, new byte[size]), false);
      size = input.readInt();
      AsciiString value = new AsciiString(readBytes(input, new byte[size]), false);
      headers.add(key, value);
      size = input.readInt();
    }
    return headers;
  }

  private static void serializeHeaders(ObjectOutputStream output, HttpHeaders headers) throws Exception {
    Iterator<Map.Entry<CharSequence, CharSequence>> it = headers.iteratorCharSequence();
    while (it.hasNext()) {
      Map.Entry<CharSequence, CharSequence> entry = it.next();
      byte[] bytes = AsciiString.of(entry.getKey()).toByteArray();
      output.writeInt(bytes.length);
      output.write(bytes);
      bytes = AsciiString.of(entry.getValue()).toByteArray();
      output.writeInt(bytes.length);
      output.write(bytes);
    }
    output.writeInt(0);
  }
}

package com.linkedin.alpini.netty4.misc;

import com.linkedin.alpini.io.IOUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.concurrent.Promise;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Objects;
import javax.annotation.Nonnull;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class DefaultFileChannelHttpResponse extends DefaultHttpResponse implements ChunkedHttpResponse {
  private static final int IO_CHUNK_SIZE = 8192;

  private final ByteBufAllocator _alloc;
  private final FileChannel _fileChannel;
  private final LastHttpContent _lastHttpContent;
  private long _position;

  public DefaultFileChannelHttpResponse(
      @Nonnull HttpVersion version,
      @Nonnull HttpResponseStatus status,
      boolean validateHeaders,
      boolean singleFieldHeaders,
      @Nonnull ByteBufAllocator alloc,
      @Nonnull FileChannel fileChannel) {
    this(
        Objects.requireNonNull(version, "version"),
        Objects.requireNonNull(status, "status"),
        validateHeaders,
        singleFieldHeaders,
        Objects.requireNonNull(alloc, "alloc"),
        Objects.requireNonNull(fileChannel, "fileChannel"),
        new DefaultLastHttpContent(Unpooled.EMPTY_BUFFER, validateHeaders));
  }

  private DefaultFileChannelHttpResponse(
      @Nonnull HttpVersion version,
      @Nonnull HttpResponseStatus status,
      boolean validateHeaders,
      boolean singleFieldHeaders,
      @Nonnull ByteBufAllocator alloc,
      @Nonnull FileChannel fileChannel,
      @Nonnull LastHttpContent lastHttpContent) {
    super(version, status, validateHeaders, singleFieldHeaders);
    _alloc = alloc;
    _fileChannel = fileChannel;
    _lastHttpContent = lastHttpContent;
  }

  @Override
  public void writeChunkedContent(ChannelHandlerContext ctx, Promise<LastHttpContent> writePromise) throws IOException {
    ByteBuf buffer = _alloc.ioBuffer();
    int bytes;

    try {
      bytes = buffer.writeBytes(_fileChannel, _position, IO_CHUNK_SIZE);
    } catch (IOException ex) {
      IOUtils.closeQuietly(_fileChannel);
      writePromise.setFailure(ex);
      return;
    }

    _position += bytes;

    if (bytes > 0) {
      ctx.writeAndFlush(new DefaultHttpContent(buffer)).addListener(future -> {
        if (future.isSuccess()) {
          writeChunkedContent(ctx, writePromise);
        } else {
          IOUtils.closeQuietly(_fileChannel);
          writePromise.setFailure(future.cause());
        }
      });
    } else {
      buffer.release();
      IOUtils.closeQuietly(_fileChannel);
      LastHttpContent lastHttpContent;
      if (trailingHeaders().isEmpty()) {
        lastHttpContent = LastHttpContent.EMPTY_LAST_CONTENT;
      } else {
        lastHttpContent = new DefaultLastHttpContent();
        lastHttpContent.trailingHeaders().set(trailingHeaders());
      }
      writePromise.setSuccess(lastHttpContent);
    }
  }

  @Override
  public HttpHeaders trailingHeaders() {
    return _lastHttpContent.trailingHeaders();
  }

  @Override
  public ByteBuf content() {
    return _lastHttpContent.content();
  }

  @Override
  public FullHttpResponse copy() {
    return this;
  }

  @Override
  public FullHttpResponse duplicate() {
    return this;
  }

  @Override
  public FullHttpResponse retainedDuplicate() {
    return this;
  }

  @Override
  public FullHttpResponse replace(ByteBuf content) {
    return this;
  }

  @Override
  public FullHttpResponse retain(int increment) {
    return this;
  }

  /**
   * Returns the reference count of this object.  If {@code 0}, it means this object has been deallocated.
   */
  @Override
  public int refCnt() {
    return _fileChannel.isOpen() ? 1 : 0;
  }

  @Override
  public FullHttpResponse retain() {
    return this;
  }

  @Override
  public FullHttpResponse touch() {
    return this;
  }

  @Override
  public FullHttpResponse touch(Object hint) {
    return this;
  }

  /**
   * Decreases the reference count by {@code 1} and deallocates this object if the reference count reaches at
   * {@code 0}.
   *
   * @return {@code true} if and only if the reference count became {@code 0} and this object has been deallocated
   */
  @Override
  public boolean release() {
    return false;
  }

  /**
   * Decreases the reference count by the specified {@code decrement} and deallocates this object if the reference
   * count reaches at {@code 0}.
   *
   * @param decrement
   * @return {@code true} if and only if the reference count became {@code 0} and this object has been deallocated
   */
  @Override
  public boolean release(int decrement) {
    return false;
  }

  @Override
  public FullHttpResponse setStatus(HttpResponseStatus status) {
    super.setStatus(status);
    return this;
  }

  @Override
  public FullHttpResponse setProtocolVersion(HttpVersion version) {
    super.setProtocolVersion(version);
    return this;
  }
}

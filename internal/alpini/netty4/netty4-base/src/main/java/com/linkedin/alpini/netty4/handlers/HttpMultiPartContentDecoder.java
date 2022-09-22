package com.linkedin.alpini.netty4.handlers;

import io.netty.channel.Channel;
import java.util.function.IntSupplier;
import javax.annotation.Nonnull;


/**
 * Decodes MIME multipart encoded content bodies by separating them into separate {@link io.netty.handler.codec.http.HttpMessage} objects.
 *
 * Created by acurtis on 3/22/17.
 */
public class HttpMultiPartContentDecoder extends SimpleChannelInitializer<Channel> {
  private final IntSupplier _maxHeaderSize;
  private final IntSupplier _maxChunkSize;
  private final IntSupplier _maxContentLength;
  private final boolean _validateHeaders;
  private final int _initialBufferSize;

  public HttpMultiPartContentDecoder(int maxHeaderSize, int maxChunkSize, int maxContentLength) {
    this(
        Integer.valueOf(maxHeaderSize)::intValue,
        Integer.valueOf(maxChunkSize)::intValue,
        Integer.valueOf(maxContentLength)::intValue);
  }

  public HttpMultiPartContentDecoder(
      @Nonnull IntSupplier maxHeaderSize,
      @Nonnull IntSupplier maxChunkSize,
      @Nonnull IntSupplier maxContentLength) {
    this(maxHeaderSize, maxChunkSize, maxContentLength, false, 128);
  }

  public HttpMultiPartContentDecoder(
      @Nonnull IntSupplier maxHeaderSize,
      @Nonnull IntSupplier maxChunkSize,
      @Nonnull IntSupplier maxContentLength,
      boolean validateHeaders,
      int initialBufferSize) {
    _maxHeaderSize = maxHeaderSize;
    _maxChunkSize = maxChunkSize;
    _maxContentLength = maxContentLength;
    _validateHeaders = validateHeaders;
    _initialBufferSize = initialBufferSize;
  }

  protected boolean checkUnwrapBoundary(Channel ch, String boundary) {
    assert ch != null && boundary != null;
    return true;
  }

  @Override
  protected void initChannel(Channel ch) throws Exception {
    HttpContentUnwrap unwrap = new HttpContentUnwrap() {
      @Override
      public boolean acceptBoundary(String boundary) {
        return super.acceptBoundary(boundary) && checkUnwrapBoundary(ch, boundary);
      }
    };
    HttpContentMultiPartDecode decode = new HttpContentMultiPartDecode(
        unwrap,
        _maxHeaderSize.getAsInt(),
        _maxChunkSize.getAsInt(),
        _validateHeaders,
        _initialBufferSize);
    HttpContentMultiPartAggregator aggregator = new HttpContentMultiPartAggregator(_maxContentLength.getAsInt());

    addAfter(ch, unwrap, decode, aggregator);
  }
}

package com.linkedin.alpini.netty4.handlers;

import io.netty.channel.Channel;
import java.util.function.IntSupplier;
import javax.annotation.Nonnull;


/**
 * Created by acurtis on 3/22/17.
 */
public class HttpMultiPartContentCodec extends SimpleChannelInitializer<Channel> {
  private final IntSupplier _maxChunkSize;
  private final SimpleChannelInitializer<Channel> _decoderInitializer;

  public HttpMultiPartContentCodec(int maxContentLength) {
    this(Integer.valueOf(8192)::intValue, Integer.valueOf(8192)::intValue, Integer.valueOf(maxContentLength)::intValue);
  }

  public HttpMultiPartContentCodec(
      @Nonnull IntSupplier maxHeaderSize,
      @Nonnull IntSupplier maxChunkSize,
      @Nonnull IntSupplier maxContentLength) {
    _maxChunkSize = maxChunkSize;
    _decoderInitializer = new HttpMultiPartContentDecoder(maxHeaderSize, maxChunkSize, maxContentLength) {
      @Override
      protected boolean checkUnwrapBoundary(Channel ch, String boundary) {
        return HttpMultiPartContentCodec.this.checkUnwrapBoundary(ch, boundary);
      }
    };
  }

  protected boolean checkUnwrapBoundary(Channel ch, String boundary) {
    assert ch != null && boundary != null;
    return true;
  }

  @Override
  protected void initChannel(Channel ch) throws Exception {
    int chunkSize = _maxChunkSize.getAsInt();
    addAfter(ch, new HttpMultiPartContentEncoder(chunkSize, chunkSize), _decoderInitializer);
  }
}

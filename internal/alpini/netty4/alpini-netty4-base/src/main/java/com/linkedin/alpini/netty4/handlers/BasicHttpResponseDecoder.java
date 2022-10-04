package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.netty4.misc.BasicFullHttpResponse;
import com.linkedin.alpini.netty4.misc.BasicHttpResponse;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpObjectDecoder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;


/**
 * Decodes {@link io.netty.buffer.ByteBuf}s into {@link HttpResponse}s and
 * {@link io.netty.handler.codec.http.HttpContent}s.
 *
 * <h3>Parameters that prevents excessive memory consumption</h3>
 * <table border="1">
 * <tr>
 * <th>Name</th><th>Meaning</th>
 * </tr>
 * <tr>
 * <td>{@code maxInitialLineLength}</td>
 * <td>The maximum length of the initial line (e.g. {@code "HTTP/1.0 200 OK"})
 *     If the length of the initial line exceeds this value, a
 *     {@link io.netty.handler.codec.TooLongFrameException} will be raised.</td>
 * </tr>
 * <tr>
 * <td>{@code maxHeaderSize}</td>
 * <td>The maximum length of all headers.  If the sum of the length of each
 *     header exceeds this value, a {@link io.netty.handler.codec.TooLongFrameException} will be raised.</td>
 * </tr>
 * <tr>
 * <td>{@code maxChunkSize}</td>
 * <td>The maximum length of the content or each chunk.  If the content length
 *     exceeds this value, the transfer encoding of the decoded response will be
 *     converted to 'chunked' and the content will be split into multiple
 *     {@link io.netty.handler.codec.http.HttpContent}s.
 *     If the transfer encoding of the HTTP response is
 *     'chunked' already, each chunk will be split into smaller chunks if the
 *     length of the chunk exceeds this value.  If you prefer not to handle
 *     {@link io.netty.handler.codec.http.HttpContent}s in your handler,
 *     insert {@link io.netty.handler.codec.http.HttpObjectAggregator}
 *     after this decoder in the {@link io.netty.channel.ChannelPipeline}.</td>
 * </tr>
 * </table>
 *
 * <h3>Decoding a response for a <tt>HEAD</tt> request</h3>
 * <p>
 * Unlike other HTTP requests, the successful response of a <tt>HEAD</tt>
 * request does not have any content even if there is <tt>Content-Length</tt>
 * header.  Because {@link BasicHttpResponseDecoder} is not able to determine if the
 * response currently being decoded is associated with a <tt>HEAD</tt> request,
 * you must override {@link #isContentAlwaysEmpty(HttpMessage)} to return
 * <tt>true</tt> for the response of the <tt>HEAD</tt> request.
 * </p><p>
 * If you are writing an HTTP client that issues a <tt>HEAD</tt> request,
 * please use {@link BasicHttpClientCodec} instead of this decoder.  It will perform
 * additional state management to handle the responses for <tt>HEAD</tt>
 * requests correctly.
 * </p>
 *
 * <h3>Decoding a response for a <tt>CONNECT</tt> request</h3>
 * <p>
 * You also need to do additional state management to handle the response of a
 * <tt>CONNECT</tt> request properly, like you did for <tt>HEAD</tt>.  One
 * difference is that the decoder should stop decoding completely after decoding
 * the successful 200 response since the connection is not an HTTP connection
 * anymore.
 * </p><p>
 * {@link BasicHttpClientCodec} also handles this edge case correctly, so you have to
 * use {@link BasicHttpClientCodec} if you are writing an HTTP client that issues a
 * <tt>CONNECT</tt> request.
 * </p>
 */
public abstract class BasicHttpResponseDecoder extends HttpObjectDecoder {
  private static final HttpResponseStatus UNKNOWN_STATUS = new HttpResponseStatus(999, "Unknown");

  /**
   * Creates a new instance with the default
   * {@code maxInitialLineLength (4096)}, {@code maxHeaderSize (8192)}, and
   * {@code maxChunkSize (8192)}.
   */
  public BasicHttpResponseDecoder() {
  }

  /**
   * Creates a new instance with the specified parameters.
   */
  public BasicHttpResponseDecoder(int maxInitialLineLength, int maxHeaderSize, int maxChunkSize) {
    super(maxInitialLineLength, maxHeaderSize, maxChunkSize, true);
  }

  public BasicHttpResponseDecoder(
      int maxInitialLineLength,
      int maxHeaderSize,
      int maxChunkSize,
      boolean validateHeaders) {
    super(maxInitialLineLength, maxHeaderSize, maxChunkSize, true, validateHeaders);
  }

  public BasicHttpResponseDecoder(
      int maxInitialLineLength,
      int maxHeaderSize,
      int maxChunkSize,
      boolean validateHeaders,
      int initialBufferSize) {
    super(maxInitialLineLength, maxHeaderSize, maxChunkSize, true, validateHeaders, initialBufferSize);
  }

  @Override
  protected HttpMessage createMessage(String[] initialLine) {
    return checkLastResponse(
        new BasicHttpResponse(
            lastRequest(),
            HttpResponseStatus.valueOf(Integer.parseInt(initialLine[1]), initialLine[2]),
            validateHeaders).setProtocolVersion(HttpVersion.valueOf(initialLine[0])));
  }

  @Override
  protected HttpMessage createInvalidMessage() {
    return new BasicFullHttpResponse(lastRequest(), UNKNOWN_STATUS, validateHeaders);
  }

  @Override
  protected boolean isDecodingRequest() {
    return false;
  }

  protected abstract HttpRequest lastRequest();

  protected abstract HttpResponse checkLastResponse(HttpResponse response);
}

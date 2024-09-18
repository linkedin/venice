package com.linkedin.venice.listener;

import com.linkedin.davinci.listener.response.ReadResponse;
import com.linkedin.venice.listener.response.HttpShortcutResponse;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import io.netty.channel.ChannelHandlerContext;


/**
 * This is used in REST/HTTP Netty handlers to handle the response from the {@link StorageReadRequestHandler#queueIoRequestForAsyncProcessing} method.
 */
public class HttpStorageResponseHandlerCallback implements StorageResponseHandlerCallback {
  private final ChannelHandlerContext context;

  private HttpStorageResponseHandlerCallback(ChannelHandlerContext context) {
    this.context = context;
  }

  // Factory method for creating an instance of this class.
  public static HttpStorageResponseHandlerCallback create(ChannelHandlerContext context) {
    return new HttpStorageResponseHandlerCallback(context);
  }

  @Override
  public void onReadResponse(ReadResponse readResponse) {
    context.writeAndFlush(readResponse);
  }

  @Override
  public void onError(VeniceReadResponseStatus readResponseStatus, String message) {
    HttpShortcutResponse response = new HttpShortcutResponse(message, readResponseStatus.getHttpResponseStatus());
    context.writeAndFlush(response);
  }
}

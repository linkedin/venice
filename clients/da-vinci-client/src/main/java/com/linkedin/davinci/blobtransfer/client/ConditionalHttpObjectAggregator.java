package com.linkedin.davinci.blobtransfer.client;

import com.linkedin.davinci.blobtransfer.BlobTransferUtils;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponse;


/**
 * ConditionalHttpObjectAggregator is a custom HttpObjectAggregator that
 * only aggregated HttpResponse messages for metadata.
 */
public class ConditionalHttpObjectAggregator extends HttpObjectAggregator {
  public ConditionalHttpObjectAggregator(int maxContentLength) {
    super(maxContentLength);
  }

  @Override
  public boolean acceptInboundMessage(Object msg) throws Exception {
    if (msg instanceof HttpResponse) {
      HttpResponse httpMessage = (HttpResponse) msg;
      // only accept metadata messages to be aggregated
      if (isMetadataMessage(httpMessage)) {
        return super.acceptInboundMessage(msg);
      } else {
        return false;
      }
    }
    return super.acceptInboundMessage(msg);
  }

  private boolean isMetadataMessage(HttpResponse msg) {
    String metadataHeader = msg.headers().get(BlobTransferUtils.BLOB_TRANSFER_TYPE);
    if (metadataHeader == null) {
      return false;
    }
    return metadataHeader.equals(BlobTransferUtils.BlobTransferType.METADATA.name());
  }
}

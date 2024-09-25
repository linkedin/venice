package com.linkedin.davinci.blobtransfer.client;

import com.linkedin.davinci.blobtransfer.BlobTransferUtils;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponse;


/**
 * MetadataAggregator is a custom HttpObjectAggregator that
 * only aggregated HttpResponse messages for metadata.
 */
public class MetadataAggregator extends HttpObjectAggregator {
  public MetadataAggregator(int maxContentLength) {
    super(maxContentLength);
  }

  @Override
  public boolean acceptInboundMessage(Object msg) throws Exception {
    if (msg instanceof HttpResponse) {
      HttpResponse httpMessage = (HttpResponse) msg;
      // only accept metadata messages to be aggregated
      if (BlobTransferUtils.isMetadataMessage(httpMessage)) {
        return super.acceptInboundMessage(msg);
      } else {
        return false;
      }
    }
    return super.acceptInboundMessage(msg);
  }
}

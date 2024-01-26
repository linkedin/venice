package com.linkedin.alpini.netty4.misc;

import com.linkedin.alpini.base.misc.HeaderNames;
import com.linkedin.alpini.base.misc.HeaderUtils;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Created by acurtis on 3/22/17.
 */
public interface HttpMultiPart extends HttpMessage {
  Logger MULTIPART_LOG = LogManager.getLogger(HttpMultiPart.class);

  default HttpResponseStatus status() {
    try {
      String multiPartContentStatus = headers().get(HeaderNames.X_MULTIPART_CONTENT_STATUS);
      return multiPartContentStatus == null
          ? HttpResponseStatus.OK
          : HttpResponseStatus.parseLine(multiPartContentStatus);
    } catch (Throwable ex) {
      MULTIPART_LOG.debug("Unparseable status", ex);
      return HttpResponseStatus.OK;
    }
  }

  default HttpMultiPart setStatus(HttpResponseStatus status) {
    headers().set(HeaderNames.X_MULTIPART_CONTENT_STATUS, HeaderUtils.cleanHeaderValue(status.toString()));
    return this;
  }
}

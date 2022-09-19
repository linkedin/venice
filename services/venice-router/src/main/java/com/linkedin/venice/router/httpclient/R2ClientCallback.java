package com.linkedin.venice.router.httpclient;

import com.linkedin.common.callback.Callback;
import com.linkedin.r2.message.rest.RestException;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class R2ClientCallback implements Callback<RestResponse> {
  private static final Logger LOGGER = LogManager.getLogger(R2ClientCallback.class);
  private final Consumer<PortableHttpResponse> responseConsumer;
  private final Consumer<Throwable> failedCallback;
  private final BooleanSupplier cancelledCallBack;
  private static final StackTraceElement[] emptyStackTrace = new StackTraceElement[0];

  protected static final RedundantExceptionFilter REDUNDANT_LOGGING_FILTER =
      RedundantExceptionFilter.getRedundantExceptionFilter();

  public R2ClientCallback(
      Consumer<PortableHttpResponse> responseConsumer,
      Consumer<Throwable> failedCallback,
      BooleanSupplier cancelledCallBack) {
    this.responseConsumer = responseConsumer;
    this.failedCallback = failedCallback;
    this.cancelledCallBack = cancelledCallBack;
  }

  @Override
  public void onError(Throwable e) {
    if (e instanceof RestException) {
      // Get the RestResponse for status codes other than 200
      RestResponse result = ((RestException) e).getResponse();
      onSuccess(result);
    } else {
      String msg = e.getMessage();
      if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg != null ? msg : "")) {
        e.setStackTrace(emptyStackTrace);
        LOGGER.error("Received error from R2 client ", e);
      }
      failedCallback.accept(e);
      cancelledCallBack.getAsBoolean();
    }
  }

  @Override
  public void onSuccess(RestResponse result) {
    int statusCode = result.getStatus();
    // Not found returns onErrror with Throwable, fail only for server errors
    if (statusCode >= 500) {
      failedCallback.accept(new VeniceException("R2 client failed with http status code " + statusCode));
      return;
    }
    R2ClientPortableHttpResponse response = new R2ClientPortableHttpResponse(result);
    responseConsumer.accept(response);
  }

  private static class R2ClientPortableHttpResponse implements PortableHttpResponse {
    private final RestResponse response;

    private R2ClientPortableHttpResponse(RestResponse response) {
      this.response = response;
    }

    @Override
    public int getStatusCode() {
      return response.getStatus();
    }

    @Override
    public ByteBuf getContentInByteBuf() {
      return Unpooled.wrappedBuffer(response.getEntity().asByteBuffer());
    }

    @Override
    public boolean containsHeader(String headerName) {
      return response.getHeaders().containsKey(headerName);
    }

    @Override
    public String getFirstHeader(String headerName) {
      return response.getHeaders().get(headerName);
    }
  }
}

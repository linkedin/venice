package com.linkedin.alpini.netty4.misc;

import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.alpini.base.misc.Time;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.IllegalReferenceCountException;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * @author Song Lu solu@linkedin.com
 * Date: 1/24/19
 */
public class TestBasicFullHttpResponse {
  @Test(groups = "unit")
  public void testReleaseWithoutAnyException() {
    ByteBuf content = mock(ByteBuf.class);
    when(content.release(anyInt())).thenThrow(new IllegalReferenceCountException("-1"));

    BasicFullHttpResponse response = new BasicFullHttpResponse(
        new BasicFullHttpRequest(
            HttpVersion.HTTP_1_1,
            HttpMethod.GET,
            "http://blah",
            Time.currentTimeMillis(),
            Time.nanoTime()),
        HttpResponseStatus.OK,
        content,
        false);
    try {
      response.release();
      response.release(100);
    } catch (IllegalReferenceCountException e) {
      Assert.fail();
    }

  }
}

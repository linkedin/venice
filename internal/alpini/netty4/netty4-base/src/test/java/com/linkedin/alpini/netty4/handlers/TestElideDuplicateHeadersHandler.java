package com.linkedin.alpini.netty4.handlers;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.util.Iterator;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestElideDuplicateHeadersHandler {
  @Test(groups = "unit")
  public void testBasicFunction() {
    HttpHeaders requestHeaders = new DefaultHttpHeaders();
    String key = "Hello-World";
    String value = "This is a test";
    requestHeaders.add(key, value);

    EmbeddedChannel ch = new EmbeddedChannel(ElideDuplicateHeadersHandler.INSTANCE);

    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/", requestHeaders);
    ch.writeOneInbound(request);
    Assert.assertSame(ch.readInbound(), request);

    HttpHeaders responseHeaders = new DefaultHttpHeaders();
    responseHeaders.add(key, value);
    HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, responseHeaders);
    ch.writeOneOutbound(response);
    ch.flushOutbound();
    Assert.assertSame(ch.readOutbound(), response);

    Iterator<Map.Entry<CharSequence, CharSequence>> requestIterator = request.headers().iteratorCharSequence();
    Iterator<Map.Entry<CharSequence, CharSequence>> responseIterator = response.headers().iteratorCharSequence();

    Map.Entry<CharSequence, CharSequence> requestEntry = requestIterator.next();
    Map.Entry<CharSequence, CharSequence> responseEntry = responseIterator.next();

    Assert.assertSame(requestEntry.getKey(), responseEntry.getKey());
    Assert.assertSame(requestEntry.getValue(), responseEntry.getValue());

    Assert.assertNotSame(requestEntry.getKey(), key);
    Assert.assertNotSame(requestEntry.getValue(), value);

    Assert.assertFalse(requestIterator.hasNext());
    Assert.assertFalse(responseIterator.hasNext());

    ch.finishAndReleaseAll();
  }
}

package com.linkedin.alpini.netty4.misc;

import com.linkedin.alpini.base.cache.ByteBufHashMap;
import com.linkedin.alpini.base.misc.Time;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.ReferenceCountUtil;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by acurtis on 8/16/17.
 */
public class TestBasicHttpRequestSerializer {
  private final ByteBufHashMap.SerDes<BasicHttpRequest> _serDes =
      new BasicHttpRequestSerializer(PooledByteBufAllocator.DEFAULT);

  @Test(groups = "unit")
  public void testSerializeBasicHttpRequest() {
    BasicHttpRequest request = new BasicHttpRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.GET,
        "/hello/world",
        Time.currentTimeMillis(),
        Time.nanoTime());
    request.headers().set(HttpHeaderNames.USER_AGENT, "Hello World Agent");

    ByteBuf buffer = PooledByteBufAllocator.DEFAULT.buffer();

    Assert.assertTrue(_serDes.serialize(new ByteBufOutputStream(buffer), request));

    BasicHttpRequest result = _serDes.deserialize(new ByteBufInputStream(buffer));

    Assert.assertNotSame(result, request);
    Assert.assertNotSame(result.headers(), request.headers());

    Assert.assertEquals(result, request);
    Assert.assertEquals(result.hashCode(), request.hashCode());

    buffer.release();
  }

  @Test(groups = "unit")
  public void testSerializeBasicFullHttpRequest() {
    BasicFullHttpRequest request = new BasicFullHttpRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.GET,
        "/hello/world",
        Time.currentTimeMillis(),
        Time.nanoTime());
    request.headers().set(HttpHeaderNames.USER_AGENT, "Hello World Agent");
    request.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");
    request.content().writeBytes("Hello world content".getBytes(StandardCharsets.US_ASCII));

    ByteBuf buffer = PooledByteBufAllocator.DEFAULT.buffer();

    Assert.assertTrue(_serDes.serialize(new ByteBufOutputStream(buffer), request));

    BasicHttpRequest result = _serDes.deserialize(new ByteBufInputStream(buffer));

    Assert.assertNotSame(result, request);
    Assert.assertNotSame(result.headers(), request.headers());

    Assert.assertEquals(result, request);
    Assert.assertEquals(result.hashCode(), request.hashCode());

    Assert.assertTrue(ReferenceCountUtil.release(result));

    buffer.release();
  }

  @Test(groups = "unit")
  public void testSerializeBasicFullHttpMultipartRequest() {
    List<FullHttpMultiPart> content = new LinkedList<>();

    HttpHeaders partHeader = new DefaultHttpHeaders(false);
    partHeader.add(HttpHeaderNames.CONTENT_TYPE, "text/plain");
    content.add(
        new BasicFullHttpMultiPart(
            Unpooled.copiedBuffer("This is test content", StandardCharsets.US_ASCII),
            partHeader));

    BasicFullHttpMultiPartRequest request = new BasicFullHttpMultiPartRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.GET,
        "/hello/world",
        content,
        Time.currentTimeMillis(),
        Time.nanoTime());
    request.headers().set(HttpHeaderNames.USER_AGENT, "Hello World Agent");
    request.headers().set(HttpHeaderNames.CONTENT_TYPE, "multipart/mixed");

    ByteBuf buffer = PooledByteBufAllocator.DEFAULT.buffer();

    Assert.assertTrue(_serDes.serialize(new ByteBufOutputStream(buffer), request));

    BasicHttpRequest result = _serDes.deserialize(new ByteBufInputStream(buffer));

    Assert.assertNotSame(result, request);
    Assert.assertNotSame(result.headers(), request.headers());

    Assert.assertEquals(result, request);
    Assert.assertEquals(result.hashCode(), request.hashCode());

    Assert.assertTrue(ReferenceCountUtil.release(result));

    buffer.release();
  }

  @Test(groups = "unit")
  public void testCopyBasicFullHttpMultipartRequest() {
    List<FullHttpMultiPart> content = new LinkedList<>();

    HttpHeaders partHeader = new DefaultHttpHeaders(false);
    partHeader.add(HttpHeaderNames.CONTENT_TYPE, "text/plain");
    content.add(
        new BasicFullHttpMultiPart(
            Unpooled.copiedBuffer("This is test content", StandardCharsets.US_ASCII),
            partHeader));

    BasicFullHttpMultiPartRequest request = new BasicFullHttpMultiPartRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.GET,
        "/hello/world",
        content,
        Time.currentTimeMillis(),
        Time.nanoTime());
    request.headers().set(HttpHeaderNames.USER_AGENT, "Hello World Agent");
    request.headers().set(HttpHeaderNames.CONTENT_TYPE, "multipart/mixed");

    BasicHttpRequest result = request.copy(); // doesn't change ref count

    Assert.assertNotSame(result, request);
    Assert.assertSame(result.headers(), request.headers());

    Assert.assertEquals(result, request);
    Assert.assertEquals(result.hashCode(), request.hashCode());

    Assert.assertTrue(ReferenceCountUtil.release(result));
  }

  @Test(groups = "unit")
  public void testDuplicateBasicFullHttpMultipartRequest() {
    List<FullHttpMultiPart> content = new LinkedList<>();

    HttpHeaders partHeader = new DefaultHttpHeaders(false);
    partHeader.add(HttpHeaderNames.CONTENT_TYPE, "text/plain");
    content.add(
        new BasicFullHttpMultiPart(
            Unpooled.copiedBuffer("This is test content", StandardCharsets.US_ASCII),
            partHeader));

    BasicFullHttpMultiPartRequest request = new BasicFullHttpMultiPartRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.GET,
        "/hello/world",
        content,
        Time.currentTimeMillis(),
        Time.nanoTime());
    request.headers().set(HttpHeaderNames.USER_AGENT, "Hello World Agent");
    request.headers().set(HttpHeaderNames.CONTENT_TYPE, "multipart/mixed");

    BasicHttpRequest result = request.duplicate(); // doesn't change ref count

    Assert.assertNotSame(result, request);
    Assert.assertSame(result.headers(), request.headers());

    Assert.assertEquals(result, request);
    Assert.assertEquals(result.hashCode(), request.hashCode());

    Assert.assertTrue(ReferenceCountUtil.release(result));
  }

  @Test(groups = "unit")
  public void testRetainedDuplicateBasicFullHttpMultipartRequest() {
    List<FullHttpMultiPart> content = new LinkedList<>();

    HttpHeaders partHeader = new DefaultHttpHeaders(false);
    partHeader.add(HttpHeaderNames.CONTENT_TYPE, "text/plain");
    content.add(
        new BasicFullHttpMultiPart(
            Unpooled.copiedBuffer("This is test content", StandardCharsets.US_ASCII),
            partHeader));

    BasicFullHttpMultiPartRequest request = new BasicFullHttpMultiPartRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.GET,
        "/hello/world",
        content,
        Time.currentTimeMillis(),
        Time.nanoTime());
    request.headers().set(HttpHeaderNames.USER_AGENT, "Hello World Agent");
    request.headers().set(HttpHeaderNames.CONTENT_TYPE, "multipart/mixed");

    BasicHttpRequest result = request.retainedDuplicate(); // increments ref count

    Assert.assertNotSame(result, request);
    Assert.assertSame(result.headers(), request.headers());

    Assert.assertEquals(result, request);
    Assert.assertEquals(result.hashCode(), request.hashCode());

    Assert.assertFalse(ReferenceCountUtil.release(result, 1));
  }

  @Test(groups = "unit")
  public void testSerializeEmptyFullHttpMultipartRequest() {
    BasicFullHttpMultiPartRequest request = new BasicFullHttpMultiPartRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.GET,
        "/hello/world",
        Time.currentTimeMillis(),
        Time.nanoTime());
    request.headers().set(HttpHeaderNames.USER_AGENT, "Hello World Agent");
    request.headers().set(HttpHeaderNames.CONTENT_TYPE, "multipart/mixed");

    ByteBuf buffer = PooledByteBufAllocator.DEFAULT.buffer();

    Assert.assertTrue(_serDes.serialize(new ByteBufOutputStream(buffer), request));

    BasicHttpRequest result = _serDes.deserialize(new ByteBufInputStream(buffer));

    Assert.assertNotSame(result, request);
    Assert.assertNotSame(result.headers(), request.headers());

    Assert.assertEquals(result, request);
    Assert.assertEquals(result.hashCode(), request.hashCode());

    Assert.assertFalse(ReferenceCountUtil.release(result));

    buffer.release();
  }
}

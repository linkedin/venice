package com.linkedin.alpini.netty4.http2;

import com.linkedin.alpini.base.misc.CollectionUtil;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.util.AsciiString;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test(groups = "unit")
public class TestHttp1Headers {
  public void testGetHttp2Headers() {
    Http2Headers http2Headers = new DefaultHttp2Headers();
    Http1Headers http1Headers = new Http1Headers(http2Headers, false);
    Assert.assertSame(http1Headers.getHttp2Headers(), http2Headers);
  }

  public void testCs() {
    Assert.assertSame(Http1Headers.cs("Hello"), "Hello");
    Assert.assertNull(Http1Headers.cs(null));
  }

  public void testStr() {
    Assert.assertSame(Http1Headers.str("Hello"), "Hello");
    Assert.assertNull(Http1Headers.str(null));
    Assert.assertNotSame(Http1Headers.str(AsciiString.of("Hello")), "Hello");
    Assert.assertEquals(Http1Headers.str(AsciiString.of("Hello")), "Hello");
  }

  public void testStrNotNull() {
    Assert.assertSame(Http1Headers.strNotNull("Hello"), "Hello");
    Assert.assertThrows(NullPointerException.class, () -> Http1Headers.strNotNull(null));
    Assert.assertNotSame(Http1Headers.strNotNull(AsciiString.of("Hello")), "Hello");
    Assert.assertEquals(Http1Headers.strNotNull(AsciiString.of("Hello")), "Hello");
  }

  public void testGet() {
    Http2Headers http2Headers = new DefaultHttp2Headers();
    Http1Headers http1Headers = new Http1Headers(http2Headers, false);
    http2Headers.set(HttpHeaderNames.PRAGMA, HttpHeaderValues.NO_CACHE);
    Assert.assertEquals(http1Headers.get("Pragma"), "no-cache");
    Assert.assertEquals(http1Headers.get(AsciiString.of("Pragma")), "no-cache");
    Assert.assertNull(http1Headers.get("foo"));
  }

  public void testTestGet() {
    Http2Headers http2Headers = new DefaultHttp2Headers();
    Http1Headers http1Headers = new Http1Headers(http2Headers, false);
    http2Headers.set(HttpHeaderNames.PRAGMA, HttpHeaderValues.NO_CACHE);
    Assert.assertEquals(http1Headers.get("pragma", "cache"), "no-cache");
    Assert.assertSame(http1Headers.get("foo", "bar"), "bar");
  }

  public void testGetInt() {
    Http2Headers http2Headers = new DefaultHttp2Headers();
    Http1Headers http1Headers = new Http1Headers(http2Headers, false);
    http2Headers.setInt(HttpHeaderNames.CONTENT_LENGTH, 42);
    Assert.assertEquals(http1Headers.getInt("content-length", -1), 42);
    Assert.assertEquals(http1Headers.getInt("content-length"), (Integer) 42);
  }

  public void testGetShort() {
    Http2Headers http2Headers = new DefaultHttp2Headers();
    Http1Headers http1Headers = new Http1Headers(http2Headers, false);
    http2Headers.setShort(HttpHeaderNames.CONTENT_LENGTH, (short) 42);
    Assert.assertEquals(http1Headers.getShort("content-length", (short) -1), 42);
    Assert.assertEquals(http1Headers.getShort("content-length"), (Short) (short) 42);
  }

  public void testGetTimeMillis() {
    Http2Headers http2Headers = new DefaultHttp2Headers();
    Http1Headers http1Headers = new Http1Headers(http2Headers, false);
    Instant test = Instant.ofEpochSecond(1593125018L);
    http2Headers.setTimeMillis(HttpHeaderNames.DATE, test.toEpochMilli());
    Assert.assertEquals(
        http1Headers.getTimeMillis("date", Instant.now().toEpochMilli()) / 1000L,
        test.toEpochMilli() / 1000L);
    Assert.assertEquals(http1Headers.getTimeMillis("date") / 1000L, test.toEpochMilli() / 1000L);
    Assert.assertEquals(http1Headers.get("date"), "Thu, 25 Jun 2020 22:43:38 GMT");
  }

  public void testGetAll() {
    Http2Headers http2Headers = new DefaultHttp2Headers();
    Http1Headers http1Headers = new Http1Headers(http2Headers, false);
    http2Headers.setLong(HttpHeaderNames.CONTENT_LENGTH, 42L);
    http2Headers.method(HttpMethod.GET.asciiName());
    List<String> it = http1Headers.getAll("content-length");
    Assert.assertEquals(it, Collections.singletonList("42"));
  }

  public void testEntries() {
    Http2Headers http2Headers = new DefaultHttp2Headers();
    Http1Headers http1Headers = new Http1Headers(http2Headers, false);
    http2Headers.setLong(HttpHeaderNames.CONTENT_LENGTH, 42L);
    http2Headers.method(HttpMethod.GET.asciiName());
    List<Map.Entry<String, String>> list = http1Headers.entries();
    Assert.assertEquals(list.size(), 1);
    Assert.assertEquals(list.get(0).getKey(), "content-length");
    Assert.assertEquals(list.get(0).getValue(), "42");
  }

  public void testCastMapEntry() {
    Map.Entry<CharSequence, CharSequence> entry =
        CollectionUtil.mapOf((CharSequence) AsciiString.of("foo"), (CharSequence) AsciiString.of("bar"))
            .entrySet()
            .stream()
            .findFirst()
            .get();
    Map.Entry<String, String> casted = Http1Headers.castMapEntry(entry);
    Assert.assertNotSame(casted, entry);
    Assert.assertEquals(casted.getKey(), entry.getKey().toString());
    Assert.assertEquals(casted.getValue(), entry.getValue().toString());

    entry = CollectionUtil.mapOf((CharSequence) AsciiString.of("foo"), (CharSequence) "bar")
        .entrySet()
        .stream()
        .findFirst()
        .get();
    casted = Http1Headers.castMapEntry(entry);
    Assert.assertNotSame(casted, entry);
    Assert.assertEquals(casted.getKey(), entry.getKey().toString());
    Assert.assertSame(casted.getValue(), entry.getValue());

    entry = CollectionUtil.mapOf((CharSequence) "foo", (CharSequence) AsciiString.of("bar"))
        .entrySet()
        .stream()
        .findFirst()
        .get();
    casted = Http1Headers.castMapEntry(entry);
    Assert.assertNotSame(casted, entry);
    Assert.assertSame(casted.getKey(), entry.getKey());
    Assert.assertEquals(casted.getValue(), entry.getValue().toString());

    entry = CollectionUtil.mapOf((CharSequence) "foo", (CharSequence) "bar").entrySet().stream().findFirst().get();
    casted = Http1Headers.castMapEntry(entry);
    Assert.assertSame(casted, entry);
  }

  public void testIterator() {
    Http2Headers http2Headers = new DefaultHttp2Headers();
    Http1Headers http1Headers = new Http1Headers(http2Headers, false);
    http2Headers.setLong(HttpHeaderNames.CONTENT_LENGTH, 42L);
    http2Headers.set("x-foo", "123");
    http2Headers.method(HttpMethod.GET.asciiName());
    Iterator<Map.Entry<String, String>> it = http1Headers.iterator();
    Map.Entry<String, String> entry = it.next();
    Assert.assertSame(entry.getKey(), entry.getKey());
    Assert.assertEquals(entry.getKey(), "content-length");
    Assert.assertEquals(entry.getValue(), "42");
    Assert.assertEquals(entry.setValue("foo"), "42");
    Map.Entry<String, String> entry2 = it.next();
    Assert.assertEquals(entry2.getKey(), "x-foo");
    Assert.assertEquals(entry2.getValue(), "123");
    Assert.assertFalse(it.hasNext());
    Assert.assertThrows(NoSuchElementException.class, it::next);
  }

  public void testValueStringIterator() {
    Http2Headers http2Headers = new DefaultHttp2Headers();
    Http1Headers http1Headers = new Http1Headers(http2Headers, false);
    http2Headers.setLong(HttpHeaderNames.CONTENT_LENGTH, 42L);
    Iterator<String> it = http1Headers.valueStringIterator("content-length");
    Assert.assertEquals(it.next(), "42");
    Assert.assertFalse(it.hasNext());
  }

  public void testIsEmpty() {
    Http2Headers http2Headers = new DefaultHttp2Headers();
    Http1Headers http1Headers = new Http1Headers(http2Headers, false);
    Assert.assertTrue(http1Headers.isEmpty());
    Assert.assertTrue(http2Headers.isEmpty());
    http2Headers.method(HttpMethod.GET.asciiName());
    Assert.assertFalse(http2Headers.isEmpty());
    Assert.assertTrue(http1Headers.isEmpty());
    http2Headers.setLong(HttpHeaderNames.CONTENT_LENGTH, 42L);
    Assert.assertFalse(http1Headers.isEmpty());
    Assert.assertFalse(http2Headers.isEmpty());
  }

  public void testSize() {
    Http2Headers http2Headers = new DefaultHttp2Headers();
    Http1Headers http1Headers = new Http1Headers(http2Headers, false);
    Assert.assertSame(http1Headers.getHttp2Headers(), http2Headers);
    http2Headers.method(HttpMethod.GET.asciiName());
    http2Headers.setLong(HttpHeaderNames.CONTENT_LENGTH, 42L);
    Assert.assertEquals(http2Headers.size(), 2);
    Assert.assertEquals(http1Headers.size(), 1);
  }

  public void testNames() {
    Http2Headers http2Headers = new DefaultHttp2Headers();
    Http1Headers http1Headers = new Http1Headers(http2Headers, false);
    Assert.assertSame(http1Headers.getHttp2Headers(), http2Headers);
    http2Headers.method(HttpMethod.GET.asciiName());
    http2Headers.setLong(HttpHeaderNames.CONTENT_LENGTH, 42L);
    Set<String> names = http1Headers.names();
    Assert.assertEquals(names, Collections.singleton("content-length"));
    Assert.assertSame(http1Headers.names(), names);
  }

  public void testAdd() {
    Http2Headers http2Headers = new DefaultHttp2Headers();
    Http1Headers http1Headers = new Http1Headers(http2Headers, false);
    Assert.assertSame(http1Headers.add("CONTENT-LENGTH", "42"), http1Headers);
    Assert.assertEquals(http2Headers.getLong(HttpHeaderNames.CONTENT_LENGTH, -1), 42L);

    Assert.assertSame(http1Headers.add("ACCEPT", Arrays.asList("text/html", "*")), http1Headers);
    List<CharSequence> accepts = http2Headers.getAll(HttpHeaderNames.ACCEPT);
    Assert.assertEquals(accepts.size(), 2);
    Assert.assertEquals(accepts.get(0), "text/html");
    Assert.assertEquals(accepts.get(1), "*");
  }

  public void testAddCS() {
    Http2Headers http2Headers = new DefaultHttp2Headers();
    Http1Headers http1Headers = new Http1Headers(http2Headers, false);
    Assert.assertSame(http1Headers.add(AsciiString.of("CONTENT-LENGTH"), "42"), http1Headers);
    Assert.assertEquals(http2Headers.getLong(HttpHeaderNames.CONTENT_LENGTH, -1), 42L);

    Assert.assertSame(http1Headers.add(AsciiString.of("ACCEPT"), Arrays.asList("text/html", "*")), http1Headers);
    List<CharSequence> accepts = http2Headers.getAll(HttpHeaderNames.ACCEPT);
    Assert.assertEquals(accepts.size(), 2);
    Assert.assertEquals(accepts.get(0), "text/html");
    Assert.assertEquals(accepts.get(1), "*");
  }

  public void testTestAdd() {
    Http2Headers http2Header1 = new DefaultHttp2Headers();
    Http1Headers http1Header1 = new Http1Headers(http2Header1, false);

    Http2Headers http2Header2 = new DefaultHttp2Headers();
    Http1Headers http1Header2 = new Http1Headers(http2Header2, false);

    http1Header1.setInt("content-length", 999);
    http1Header1.set("x-foo", "bar");
    http2Header2.method(HttpMethod.GET.asciiName());
    http1Header2.setInt("content-length", 42);

    Assert.assertTrue(http1Header1.contains("x-foo"));
    Assert.assertTrue(http1Header1.contains(AsciiString.of("x-foo")));
    Assert.assertSame(http1Header1.add(http1Header2), http1Header1);
    Assert.assertTrue(http1Header1.contains("x-foo"));

    Assert.assertSame(http1Header1.getHttp2Headers(), http2Header1);
    Assert.assertEquals(http1Header1.getAll("Content-Length"), Arrays.asList("999", "42"));
    Assert.assertEquals(http1Header1.getAll(AsciiString.of("Content-Length")), Arrays.asList("999", "42"));
    Assert.assertSame(http2Header1.method(), HttpMethod.GET.asciiName());

    HttpHeaders headers = new DefaultHttpHeaders();
    headers.setInt("content-length", 123);

    Assert.assertSame(http1Header1.add(headers), http1Header1);
    Assert.assertEquals(http1Header1.getAll("Content-Length"), Arrays.asList("999", "42", "123"));
    Assert.assertEquals(http1Header1.getAll(AsciiString.of("Content-Length")), Arrays.asList("999", "42", "123"));
    Assert.assertSame(http2Header1.method(), HttpMethod.GET.asciiName());
  }

  public void testAddInt() {
    Http2Headers http2Headers = new DefaultHttp2Headers();
    Http1Headers http1Headers = new Http1Headers(http2Headers, false);
    Assert.assertSame(http1Headers.addInt("Content-Length", 42), http1Headers);
    Assert.assertEquals(http2Headers.getInt(HttpHeaderNames.CONTENT_LENGTH, -1), 42);
  }

  public void testAddShort() {
    Http2Headers http2Headers = new DefaultHttp2Headers();
    Http1Headers http1Headers = new Http1Headers(http2Headers, false);
    Assert.assertSame(http1Headers.addShort("Content-Length", (short) 42), http1Headers);
    Assert.assertEquals(http2Headers.getShort(HttpHeaderNames.CONTENT_LENGTH), (Short) (short) 42);
  }

  public void testSet() {
    Http2Headers http2Headers = new DefaultHttp2Headers();
    Http1Headers http1Headers = new Http1Headers(http2Headers, false);
    Assert.assertSame(http1Headers.set("CONTENT-LENGTH", "42"), http1Headers);
    Assert.assertEquals(http2Headers.getLong(HttpHeaderNames.CONTENT_LENGTH, -1), 42L);

    Assert.assertSame(http1Headers.set("ACCEPT", Arrays.asList("text/html", "*")), http1Headers);
    List<CharSequence> accepts = http2Headers.getAll(HttpHeaderNames.ACCEPT);
    Assert.assertEquals(accepts.size(), 2);
    Assert.assertEquals(accepts.get(0), "text/html");
    Assert.assertEquals(accepts.get(1), "*");
  }

  public void testSetCS() {
    Http2Headers http2Headers = new DefaultHttp2Headers();
    Http1Headers http1Headers = new Http1Headers(http2Headers, false);
    Assert.assertSame(http1Headers.set(AsciiString.of("CONTENT-LENGTH"), AsciiString.of("42")), http1Headers);
    Assert.assertEquals(http2Headers.getLong(HttpHeaderNames.CONTENT_LENGTH, -1), 42L);

    Assert.assertSame(
        http1Headers.set(AsciiString.of("ACCEPT"), Arrays.asList(AsciiString.of("text/html"), AsciiString.of("*"))),
        http1Headers);
    List<CharSequence> accepts = http2Headers.getAll(HttpHeaderNames.ACCEPT);
    Assert.assertEquals(accepts.size(), 2);
    Assert.assertEquals(accepts.get(0), AsciiString.of("text/html"));
    Assert.assertEquals(accepts.get(1), AsciiString.of("*"));
  }

  public void testTestSet() {
    Http2Headers http2Header1 = new DefaultHttp2Headers();
    Http1Headers http1Header1 = new Http1Headers(http2Header1, false);

    Http2Headers http2Header2 = new DefaultHttp2Headers();
    Http1Headers http1Header2 = new Http1Headers(http2Header2, false);

    http1Header1.setInt("content-length", 999);
    http1Header1.set("x-foo", "bar");
    http2Header2.method(HttpMethod.GET.asciiName());
    http1Header2.setInt("content-length", 42);

    Assert.assertTrue(http1Header1.contains("x-foo"));
    Assert.assertSame(http1Header1.set(http1Header2), http1Header1);
    Assert.assertFalse(http1Header1.contains("x-foo"));

    Assert.assertSame(http1Header1.getHttp2Headers(), http2Header1);
    Assert.assertSame(http1Header1.getInt("content-length"), 42);
    Assert.assertSame(http2Header1.method(), HttpMethod.GET.asciiName());

    HttpHeaders headers = new DefaultHttpHeaders();
    headers.setInt("content-length", 123);

    Assert.assertSame(http1Header1.set(headers), http1Header1);
    Assert.assertSame(http1Header1.getInt("content-length"), 123);
    Assert.assertNull(http2Header1.method());
  }

  public void testSetAll() {
    Http2Headers http2Header1 = new DefaultHttp2Headers();
    Http1Headers http1Header1 = new Http1Headers(http2Header1, false);

    Http2Headers http2Header2 = new DefaultHttp2Headers();
    Http1Headers http1Header2 = new Http1Headers(http2Header2, false);

    http1Header1.setInt("content-length", 999);
    http1Header1.set("x-foo", "bar");
    http2Header2.method(HttpMethod.GET.asciiName());
    http1Header2.setInt("content-length", 42);

    Assert.assertTrue(http1Header1.contains("x-foo"));
    Assert.assertSame(http1Header1.setAll(http1Header2), http1Header1);
    Assert.assertTrue(http1Header1.contains("x-foo"));

    Assert.assertSame(http1Header1.getHttp2Headers(), http2Header1);
    Assert.assertEquals(http1Header1.get("content-length"), "42");
    Assert.assertSame(http2Header1.method(), HttpMethod.GET.asciiName());

    HttpHeaders headers = new DefaultHttpHeaders();
    headers.setInt("content-length", 123);

    Assert.assertSame(http1Header1.setAll(headers), http1Header1);
    Assert.assertEquals(http1Header1.get("content-length"), "123");
    Assert.assertSame(http2Header1.method(), HttpMethod.GET.asciiName());
  }

  public void testSetInt() {
    Http2Headers http2Headers = new DefaultHttp2Headers();
    Http1Headers http1Headers = new Http1Headers(http2Headers, false);
    Assert.assertSame(http1Headers.addInt(HttpHeaderNames.CONTENT_LENGTH, 88), http1Headers);
    Assert.assertSame(http1Headers.setInt("content-length", 42), http1Headers);
    Assert.assertEquals(http2Headers.getInt(HttpHeaderNames.CONTENT_LENGTH, -1), 42);
  }

  public void testSetShort() {
    Http2Headers http2Headers = new DefaultHttp2Headers();
    Http1Headers http1Headers = new Http1Headers(http2Headers, false);
    Assert.assertSame(http1Headers.addShort(HttpHeaderNames.CONTENT_LENGTH, (short) 88), http1Headers);
    Assert.assertSame(http1Headers.setShort("content-length", (short) 42), http1Headers);
    Assert.assertEquals(http2Headers.getShort(HttpHeaderNames.CONTENT_LENGTH), (Short) (short) 42);
  }

  public void testRemove() {
    Http2Headers http2Headers = new DefaultHttp2Headers();
    Http1Headers http1Headers = new Http1Headers(http2Headers, false);
    http2Headers.set(HttpHeaderNames.CONTENT_ENCODING, HttpHeaderValues.CHUNKED);
    Assert.assertTrue(http2Headers.contains(HttpHeaderNames.CONTENT_ENCODING));
    Assert.assertSame(http1Headers.remove("Content-Encoding"), http1Headers);
    Assert.assertFalse(http2Headers.contains(HttpHeaderNames.CONTENT_ENCODING));
  }

  public void testRemoveCS() {
    Http2Headers http2Headers = new DefaultHttp2Headers();
    Http1Headers http1Headers = new Http1Headers(http2Headers, false);
    http2Headers.set(HttpHeaderNames.CONTENT_ENCODING, HttpHeaderValues.CHUNKED);
    Assert.assertTrue(http2Headers.contains(HttpHeaderNames.CONTENT_ENCODING));
    Assert.assertSame(http1Headers.remove(AsciiString.of("Content-Encoding")), http1Headers);
    Assert.assertFalse(http2Headers.contains(HttpHeaderNames.CONTENT_ENCODING));
  }

  public void testClear() {
    Http2Headers http2Headers = new DefaultHttp2Headers();
    Http1Headers http1Headers = new Http1Headers(http2Headers, false);
    http2Headers.set("hello", "world");
    Assert.assertTrue(http1Headers.contains("hello"));
    Assert.assertSame(http1Headers.clear(), http1Headers);
    Assert.assertFalse(http1Headers.contains("hello"));
    Assert.assertTrue(http2Headers.isEmpty());
  }

  public void testTestContains1() {
    Http1Headers http1Headers = new Http1Headers(false);
    Assert.assertSame(http1Headers.set("ACCEPT", Arrays.asList("text/html", "*")), http1Headers);

    Assert.assertFalse(http1Headers.contains("ACCEPT", "foobarite", true));
    Assert.assertTrue(http1Headers.contains("ACCEPT", "text/html", true));
    Assert.assertTrue(http1Headers.contains("ACCEPT", "*", false));
  }

  public void testTestContains1CS() {
    Http1Headers http1Headers = new Http1Headers(false);
    Assert.assertSame(http1Headers.set(AsciiString.of("ACCEPT"), Arrays.asList("text/html", "*")), http1Headers);

    Assert.assertFalse(http1Headers.contains(AsciiString.of("Accept"), "foobarite", true));
    Assert.assertTrue(http1Headers.contains(AsciiString.of("accept"), "text/html", true));
    Assert.assertTrue(http1Headers.contains(AsciiString.of("ACCEPT"), "*", false));
  }

  public void testCopy() {
    Http2Headers http2Headers = new DefaultHttp2Headers();
    Http1Headers http1Headers = new Http1Headers(http2Headers, false);
    http2Headers.method(HttpMethod.GET.asciiName());
    http2Headers.setLong(HttpHeaderNames.CONTENT_LENGTH, 42L);

    Http1Headers copy = http1Headers.copy();
    Assert.assertNotSame(copy.getHttp2Headers(), http2Headers);
    Assert.assertSame(copy.getHttp2Headers().method(), http2Headers.method());
    Assert.assertSame(copy.getInt("content-length"), http1Headers.getInt("content-length"));
  }
}

package com.linkedin.alpini.base.misc;

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class TestHeaderUtils {
  @Test(groups = "unit")
  public void testCleanStatusMessageNull() {
    Assert.assertEquals(HeaderUtils.cleanStatusMessage(null), "null");
  }

  @Test(groups = "unit")
  public void testCleanStatusMessageSimple() {
    String text = "Hello World";
    Assert.assertSame(HeaderUtils.cleanStatusMessage(text), text);
  }

  @Test(groups = "unit")
  public void testCleanStatusMessageCRLF() {
    Assert.assertEquals(HeaderUtils.cleanStatusMessage("Hello World\r\nThis is a test").toString(), "Hello World");
  }

  @Test(groups = "unit")
  public void testCleanStatusMessageLF() {
    Assert.assertEquals(HeaderUtils.cleanStatusMessage("Hello World\nThis is a test").toString(), "Hello World");
  }

  @Test(groups = "unit")
  public void testCleanStatusMessage8Bit() {
    Assert.assertEquals(
        HeaderUtils.cleanStatusMessage("Hello World\u0089This is a test").toString(),
        "Hello World This is a test");
  }

  @Test(groups = "unit")
  public void testCleanHeaderValueNull() {
    Assert.assertEquals(HeaderUtils.cleanHeaderValue(null), "");
  }

  @Test(groups = "unit")
  public void testCleanHeaderValueSimple() {
    String text = "Hello World";
    Assert.assertSame(HeaderUtils.cleanHeaderValue(text), text);
    text = "Hello World\n This is a test";
    Assert.assertSame(HeaderUtils.cleanHeaderValue(text), text);
    text = "Hello World\r\n This is a test";
    Assert.assertSame(HeaderUtils.cleanHeaderValue(text), text);
    text = "Hello World\r\n\tThis is a test";
    Assert.assertSame(HeaderUtils.cleanHeaderValue(text), text);
  }

  @Test(groups = "unit")
  public void testCleanHeaderNUL() {
    String text = "Hello World\u0000This is a test";
    Assert.assertEquals(HeaderUtils.cleanHeaderValue(text).toString(), "Hello World%00This is a test");
  }

  @Test(groups = "unit")
  public void testCleanHeaderValueReplaceCRLF() {
    String text = "Hello World\nThis is a test\n Foo!";
    Assert.assertEquals(HeaderUtils.cleanHeaderValue(text).toString(), "Hello World%0aThis is a test%0a Foo!");
    text = "Hello World\r\nThis is a test";
    Assert.assertEquals(HeaderUtils.cleanHeaderValue(text).toString(), "Hello World%0d%0aThis is a test");
    text = "Hello World\rThis is a test";
    Assert.assertEquals(HeaderUtils.cleanHeaderValue(text).toString(), "Hello World%0dThis is a test");
  }

  @Test(groups = "unit")
  public void testCleanHeaderCRLFAtEnd() {
    Assert.assertEquals(HeaderUtils.cleanHeaderValue("Hello World\r\n").toString(), "Hello World%0d%0a");
    Assert.assertEquals(HeaderUtils.cleanHeaderValue("Hello World\r").toString(), "Hello World%0d");
    Assert.assertEquals(HeaderUtils.cleanHeaderValue("Hello World\n").toString(), "Hello World%0a");
  }

  @Test(groups = "unit")
  public void testCleanHeaderValue8Bit() {
    Assert.assertEquals(
        HeaderUtils.cleanHeaderValue("Hello World\u0089This is a test").toString(),
        "Hello World%c2%89This is a test");
  }

  @Test(groups = "unit")
  public void testCleanHeaderValueHT() {
    Assert.assertEquals(
        HeaderUtils.cleanHeaderValue("Hello World\u000bThis is a test").toString(),
        "Hello World%0bThis is a test");
    Assert.assertEquals(
        HeaderUtils.cleanHeaderValue("Hello World\fThis is a test").toString(),
        "Hello World%0cThis is a test");
    Assert.assertEquals(
        HeaderUtils.cleanHeaderValue("Hello World\fThis is a test%").toString(),
        "Hello World%0cThis is a test%25");
  }

  @Test(groups = "unit")
  public void testSanitizeHeaderValue8Bit() {
    Assert.assertEquals(
        HeaderUtils.sanitizeHeaderValue("Hello World\u0089This is a test").toString(),
        "Hello World%c2%89This is a test");
  }

  @Test(groups = "unit")
  public void testEscapeDelimiters() {
    StringBuilder sb = new StringBuilder();
    sb = HeaderUtils.escapeDelimiters(sb.append(">[>"), "test(me)now[or]else{barf}").append("<]<");
    Assert.assertEquals(sb.toString(), ">[>test%28me%29now%5bor%5delse%7bbarf%7d<]<");
  }

  @Test(groups = "unit")
  public void testContentTypeParser() {
    HeaderUtils.ContentType contentType;
    Iterator<Map.Entry<String, String>> iterator;
    Map.Entry<String, String> parameter;

    try {
      HeaderUtils.parseContentType("text");
      Assert.fail();
    } catch (IllegalArgumentException ex) {
      Assert.assertEquals(ex.getMessage(), "Invalid content type: text");
    }

    contentType = HeaderUtils.parseContentType("text/html");
    Assert.assertFalse(contentType.isMultipart());
    Assert.assertEquals(contentType.type(), "text");
    Assert.assertEquals(contentType.subType(), "html");
    Assert.assertFalse(contentType.parameters().hasNext());
    Assert.assertEquals(contentType.toString(), "text/html");

    contentType = HeaderUtils.parseContentType("multipart/mixed");
    Assert.assertTrue(contentType.isMultipart());
    Assert.assertEquals(contentType.type(), "multipart");
    Assert.assertEquals(contentType.subType(), "mixed");
    Assert.assertFalse(contentType.parameters().hasNext());
    Assert.assertEquals(contentType.toString(), "multipart/mixed");

    contentType = HeaderUtils.parseContentType("text/html; charset=iso8859-1");
    Assert.assertFalse(contentType.isMultipart());
    Assert.assertEquals(contentType.type(), "text");
    Assert.assertEquals(contentType.subType(), "html");
    iterator = contentType.parameters();
    Assert.assertTrue(iterator.hasNext());
    parameter = iterator.next();
    Assert.assertEquals(parameter.getKey(), "charset");
    Assert.assertEquals(parameter.getValue(), "iso8859-1");
    Assert.assertFalse(iterator.hasNext());
    Assert.assertFalse(iterator.hasNext());

    contentType = HeaderUtils.parseContentType("multipart/mixed; boundary=\"1234567890;test=bar\"; schema=foo");
    Assert.assertTrue(contentType.isMultipart());
    Assert.assertEquals(contentType.type(), "multipart");
    Assert.assertEquals(contentType.subType(), "mixed");
    iterator = contentType.parameters();
    Assert.assertTrue(iterator.hasNext());
    parameter = iterator.next();
    Assert.assertEquals(parameter.getKey(), "boundary");
    Assert.assertEquals(parameter.getValue(), "1234567890;test=bar");
    Assert.assertTrue(iterator.hasNext());
    parameter = iterator.next();
    Assert.assertEquals(parameter.getKey(), "schema");
    Assert.assertEquals(parameter.getValue(), "foo");
    Assert.assertFalse(iterator.hasNext());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertEquals(contentType.toString(), "multipart/mixed; boundary=\"1234567890;test=bar\"; schema=foo");

  }

  @Test(groups = "unit")
  public void testBuildContentType() {
    Assert.assertEquals(HeaderUtils.buildContentType("text", "html", Collections.emptyList()), "text/html");
    Assert.assertEquals(
        HeaderUtils.buildContentType(
            "multipart",
            "mixed",
            Collections.singleton(ImmutableMapEntry.make("boundary", "123456"))),
        "multipart/mixed; boundary=123456");
    Assert.assertEquals(
        HeaderUtils.buildContentType(
            "multipart",
            "mixed",
            Collections.singleton(ImmutableMapEntry.make("boundary", "123456;foo"))),
        "multipart/mixed; boundary=\"123456;foo\"");

    try {
      HeaderUtils.buildContentType("", "", (Collection<Map.Entry<String, String>>) null);
      Assert.fail();
    } catch (IllegalArgumentException ignored) {
      // OK
    }

    try {
      HeaderUtils.buildContentType("text", "", (Collection<Map.Entry<String, String>>) null);
      Assert.fail();
    } catch (IllegalArgumentException ignored) {
      // OK
    }

    try {
      HeaderUtils.buildContentType("text/html", "foo", (Collection<Map.Entry<String, String>>) null);
      Assert.fail();
    } catch (IllegalArgumentException ignored) {
      // OK
    }

    try {
      HeaderUtils.buildContentType("text", "html/foo", (Collection<Map.Entry<String, String>>) null);
      Assert.fail();
    } catch (IllegalArgumentException ignored) {
      // OK
    }

    try {
      HeaderUtils
          .buildContentType("text", "html", Collections.singleton(ImmutableMapEntry.make("charset/foo", "iso8859-1")));
      Assert.fail();
    } catch (IllegalArgumentException ignored) {
      // OK
    }

    try {
      HeaderUtils
          .buildContentType("text", "html", Collections.singleton(ImmutableMapEntry.make("charset", "iso8859-1\"bad")));
      Assert.fail();
    } catch (IllegalArgumentException ignored) {
      // OK
    }
  }

  @Test(groups = "unit")
  public void testFormatDate() {
    Assert.assertEquals(
        HeaderUtils.formatDate(Date.from(Instant.ofEpochSecond(784111777L))),
        "Sun, 06 Nov 1994 08:49:37 GMT");
    Assert.assertNull(HeaderUtils.formatDate(null));
  }

  @Test(groups = "unit")
  public void testRandomWeakUUID() throws Exception {
    CompletableFuture<UUID> uuid = new CompletableFuture<>();

    Thread thd = new Thread(() -> {
      try {
        Thread current = Thread.currentThread();

        Field probe = current.getClass().getDeclaredField("threadLocalRandomProbe");
        probe.setAccessible(true);
        probe.setInt(current, 1);

        Field seed = current.getClass().getDeclaredField("threadLocalRandomSeed");
        seed.setAccessible(true);
        seed.setLong(current, 784111777L);

        uuid.complete(HeaderUtils.randomWeakUUID());
      } catch (Exception ex) {
        uuid.completeExceptionally(ex);
      }
    });

    try {
      thd.start();
      Assert.assertEquals(uuid.get().toString(), "0048ee7a-8c6c-33bb-8e63-0e4b63d0fba5");
    } finally {
      thd.join();
    }
  }
}

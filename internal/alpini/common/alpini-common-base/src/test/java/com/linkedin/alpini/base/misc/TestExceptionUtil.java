package com.linkedin.alpini.base.misc;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test(groups = "unit")
public class TestExceptionUtil {
  @Test
  public void testGetStackTrace() {
    String stackTrace = ExceptionUtil.getStackTrace(new RuntimeException("Hello World", innerException()));

    Assert.assertTrue(
        Pattern.compile(
            "java\\.lang\\.RuntimeException: Hello World\n"
                + "\tat com\\.linkedin\\.alpini\\.base\\.misc\\.TestExceptionUtil\\.testGetStackTrace\\(TestExceptionUtil\\.java:\\d+\\)\n"
                + "(\tat [^\\n]+\n)+" + "Caused by: java\\.lang\\.IllegalArgumentException\n"
                + "\tat com\\.linkedin\\.alpini\\.base\\.misc\\.TestExceptionUtil\\.innerException\\(TestExceptionUtil\\.java:\\d+\\)\n"
                + "\t\\.\\.\\. \\d+ more\n" + "\tSuppressed: java\\.lang\\.NullPointerException\n"
                + "\t\tat com\\.linkedin\\.alpini\\.base\\.misc\\.TestExceptionUtil\\.suppressedException\\(TestExceptionUtil\\.java:\\d+\\)\n"
                + "\t\tat com\\.linkedin\\.alpini\\.base\\.misc\\.TestExceptionUtil\\.innerException\\(TestExceptionUtil\\.java:\\d+\\)\n"
                + "\t\t\\.\\.\\. \\d+ more\n",
            Pattern.MULTILINE | Pattern.DOTALL).matcher(stackTrace).matches(),
        stackTrace);
  }

  private Throwable innerException() {
    Throwable ex = new IllegalArgumentException();
    ex.addSuppressed(suppressedException());
    return ex;
  }

  private Throwable suppressedException() {
    return new NullPointerException();
  }

  @Test
  public void testGetCause() {
    IndexOutOfBoundsException ex1 = new IndexOutOfBoundsException();
    IllegalStateException ex2 = new IllegalStateException(ex1);
    RuntimeException ex3 = new RuntimeException(ex2);

    // Should get the IndexOutOfBoundsException, since that's what we ask for
    Assert.assertSame(ExceptionUtil.getCause(ex3, IndexOutOfBoundsException.class), ex1);
    // Should get the IndexOutOfBoundsException, since it is deepest in the stack out of the two classes we ask for
    Assert.assertSame(ExceptionUtil.getCause(ex3, IndexOutOfBoundsException.class, IllegalStateException.class), ex1);
    Assert.assertSame(ExceptionUtil.getCause(ex3, IllegalStateException.class, IndexOutOfBoundsException.class), ex1);
    // Should get the IllegalStateException, since that's what we ask for
    Assert.assertSame(ExceptionUtil.getCause(ex3, IllegalStateException.class), ex2);
    // Should get the IllegalStateException, since that's what we ask for
    Assert.assertSame(ExceptionUtil.getCause(ex3, RuntimeException.class), ex3);
    // Should get the original exception, since there is no IllegalArgumentException cause
    Assert.assertSame(ExceptionUtil.getCause(ex3, IllegalArgumentException.class), ex3);
  }

  @Test
  public void testCause() {
    IndexOutOfBoundsException ex1 = new IndexOutOfBoundsException();
    IllegalStateException ex2 = new IllegalStateException(ex1);
    RuntimeException ex3 = new RuntimeException(ex2);

    // Should get the IndexOutOfBoundsException, since that's what we ask for
    Assert.assertSame(ExceptionUtil.cause(ex3, IndexOutOfBoundsException.class), ex1);
    // Should get the IllegalStateException, since that's what we ask for
    Assert.assertSame(ExceptionUtil.cause(ex3, IllegalStateException.class), ex2);
    // Should get the IllegalStateException, since that's what we ask for
    Assert.assertSame(ExceptionUtil.cause(ex3, RuntimeException.class), ex3);
    // Should get the original exception, since there is no IllegalArgumentException cause
    Assert.assertNull(ExceptionUtil.cause(ex3, IllegalArgumentException.class));
  }

  @Test
  public void testUnwrap() {
    IndexOutOfBoundsException ex1 = new IndexOutOfBoundsException();
    IllegalStateException ex2 = new IllegalStateException(ex1);
    RuntimeException ex3 = new RuntimeException(ex2);

    // Should get the IndexOutOfBoundsException, since that's what we ask for
    Assert.assertSame(ExceptionUtil.unwrap(ex3, IndexOutOfBoundsException.class), ex1);
    // Should get the IllegalStateException, since that's what we ask for
    Assert.assertSame(ExceptionUtil.unwrap(ex3, IllegalStateException.class), ex2);
    // Should get the IndexOutOfBoundsException, since that's a RuntimeException
    Assert.assertSame(ExceptionUtil.unwrap(ex3, RuntimeException.class), ex1);
    // Should get null, since there is no IllegalArgumentException cause
    Assert.assertNull(ExceptionUtil.unwrap(ex3, IllegalArgumentException.class));
  }

  @Test
  public void testUnwrapCompletion() {
    IndexOutOfBoundsException ex1 = new IndexOutOfBoundsException();
    CompletableFuture<?> future = CompletableFuture.supplyAsync(() -> {
      throw ex1;
    }, Runnable::run);

    Assert.assertTrue(future.isDone());

    try {
      future.join();
      Assert.fail();
    } catch (Throwable ex) {
      Assert.assertNotSame(ex, ex1);

      // Should get the IndexOutOfBoundsException, since that's what we ask for
      Assert.assertSame(ExceptionUtil.unwrapCompletion(ex), ex1);
    }
  }

  public void testCheckException() {
    Assert.assertTrue(ExceptionUtil.checkException(Boolean.TRUE::booleanValue, "Unused"));
    Assert.assertThrows(RuntimeException.class, () -> ExceptionUtil.checkException(() -> {
      throw new IOException();
    }, "Used"));
  }

  @Test(expectedExceptions = IOException.class, expectedExceptionsMessageRegExp = "Test Successful")
  public void simpleThrowTest() {
    ExceptionUtil.throwException(new IOException("Test Successful"));
  }
}

package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.base.misc.Time;
import com.linkedin.alpini.netty4.TestLogAppender;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Log4J2LoggerFactory;
import java.lang.reflect.Modifier;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;


public abstract class AbstractLeakDetect {
  static {
    InternalLoggerFactory.setDefaultFactory(Log4J2LoggerFactory.INSTANCE);
  }

  private final ResourceLeakDetector.Level _oldLevel = ResourceLeakDetector.getLevel();
  protected static final ByteBufAllocator POOLED_ALLOCATOR = PooledByteBufAllocator.DEFAULT;

  protected static TestLogAppender RESOURCE_LEAK_LOG =
      LoggerContext.getContext().getConfiguration().getAppender("resourceLeakLog");

  private static AbstractLeakDetect TEST_CURRENT;

  private static final Logger LOG = LogManager.getLogger(AbstractLeakDetect.class);

  private final Logger _log = LogManager.getLogger(getClass());

  private boolean _finished;

  @BeforeClass
  public final void beforeClassLeakDetect() throws InterruptedException, NoSuchMethodException {
    if (RESOURCE_LEAK_LOG == null) {
      LOG.error("logging config is not set correctly for leak detection");
      throw new SkipException("logging config is not set correctly for leak detection");
    }
    Assert.assertTrue(
        TEST_CURRENT == null || TEST_CURRENT._finished,
        "Test cleanup for "
            + Optional.ofNullable(TEST_CURRENT).map(Object::getClass).map(Class::getSimpleName).orElse("null")
            + " not yet run");

    Assert.assertTrue(Modifier.isFinal(getClass().getModifiers()), "test class is not final");
    Assert.assertNotNull(getClass().getDeclaredMethod("zz9PluralZAlpha"));

    refLeakDetect(this);
    Time.sleep(1000);
    gc();
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    int leaks = checkForLeaks();
    if (leaks != 0) {
      LOG.warn("{} leaks occurred in earlier tests!", leaks);
    }
    RESOURCE_LEAK_LOG.clearMessages();
  }

  @BeforeMethod(alwaysRun = true)
  public final void beforeMethodLeakDetect() {
    Assert.assertFalse(_finished);
  }

  @AfterMethod(alwaysRun = true)
  public final void afterMethodLeakDetect() throws InterruptedException {
    if (_finished) {
      return;
    }
    int leaks = checkForLeaks();
    Assert.assertEquals(leaks, 0, "Resource leak occurred");
  }

  @AfterClass(alwaysRun = true)
  public final void afterClassLeakDetect() throws InterruptedException {
    try {
      if (RESOURCE_LEAK_LOG != null) {
        finallyLeakDetect();
      }
    } finally {
      derefLeakDetect(this);
    }
  }

  /**
   * since TestNG tends to sort by method name, this tries to be the last test
   * in the class. We do this because the AfterClass annotated methods may
   * execute after other tests classes have run and doesn't execute immediately
   * after the methods in this test class.
   */
  // @Test(groups = "unit", alwaysRun = true)
  // public final void zz9PluralZAlpha() throws InterruptedException {
  // finallyLeakDetect();
  // }

  protected final void finallyLeakDetect() throws InterruptedException {
    if (_finished) {
      return;
    }
    try {
      int leaks = checkForLeaks();
      if (leaks != 0) {
        _log.warn("{} leaks occurred during this test!", leaks);
      }
    } finally {
      _finished = true;
      RESOURCE_LEAK_LOG.clearMessages();
      ResourceLeakDetector.setLevel(_oldLevel);
    }
  }

  private static synchronized void refLeakDetect(AbstractLeakDetect test) {
    TEST_CURRENT = test;
  }

  private static synchronized void derefLeakDetect(AbstractLeakDetect test) {
    if (TEST_CURRENT == test) {
      TEST_CURRENT = null;
    }
  }

  protected int checkForLeaks() throws InterruptedException {
    int size = 0;
    for (;;) {
      gc();
      int leaks = RESOURCE_LEAK_LOG.size();
      if (size == leaks) {
        for (int i = 0; i < leaks; i++) {
          _log.warn("Leak detected : {}", RESOURCE_LEAK_LOG.getMessage(i));
        }
        return leaks;
      }
      size = leaks;
    }
  }

  private void gc() throws InterruptedException {
    System.gc();
    Thread.sleep(200);
    System.gc();
    Thread.sleep(200);
  }

  public static ByteBuf encodeString(CharSequence string, Charset charset) {
    return ByteBufUtil.encodeString(POOLED_ALLOCATOR, CharBuffer.wrap(string), charset);
  }

  static ByteBuf copyOf(byte[] buffer) {
    ByteBuf buf = POOLED_ALLOCATOR.buffer(buffer.length);
    buf.writeBytes(buffer);
    return buf;
  }

}

package com.linkedin.alpini.base;

import com.linkedin.alpini.base.misc.ByteBufAsciiString;
import com.linkedin.alpini.base.misc.HeaderStringCache;
import com.linkedin.alpini.base.misc.Time;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.util.AsciiString;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestHeaderStringCache {
  @BeforeClass(groups = "unit")
  public void freezeTime() {
    Time.freeze();
  }

  @AfterClass(groups = "unit", alwaysRun = true)
  public void restoreTime() {
    Time.restore();
    ;
  }

  @Test(groups = "unit")
  public void testHeaders() {
    HeaderStringCache.Cache cache = HeaderStringCache.getAndExpireOld();

    CharSequence s1 = cache.lookupName("Hello World");
    Assert.assertTrue(s1 instanceof String);

    CharSequence s2 = cache.lookupName(AsciiString.of("hello world"));
    Assert.assertTrue(s2 instanceof String);

    Assert.assertSame(s1, s2);

    Time.advance(1, TimeUnit.SECONDS);
    cache = HeaderStringCache.getAndExpireOld();

    CharSequence s3 = cache.lookupName(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "hello world"));
    Assert.assertTrue(s3 instanceof String);

    Assert.assertSame(s3, s2);

    Time.advance(1, TimeUnit.SECONDS);
    HeaderStringCache.getAndExpireOld();

    Time.advance(1, TimeUnit.SECONDS); // after two seconds, the cache should forget
    cache = HeaderStringCache.getAndExpireOld();

    CharSequence s4 = cache.lookupName("hello world");
    Assert.assertTrue(s4 instanceof String);
    Assert.assertNotSame(s4, s3);
  }

  @Test(groups = "unit")
  public void testValues() {
    HeaderStringCache.Cache cache = HeaderStringCache.getAndExpireOld();

    CharSequence s1 = cache.lookupValue("Hello World");
    CharSequence t1 = cache.lookupValue("hello world");
    Assert.assertTrue(s1 instanceof AsciiString);
    Assert.assertTrue(t1 instanceof AsciiString);
    Assert.assertNotSame(s1, t1);

    CharSequence t2 = cache.lookupValue(AsciiString.of("hello world"));
    Assert.assertTrue(t2 instanceof AsciiString);

    Assert.assertSame(t1, t2);

    Time.advance(1, TimeUnit.SECONDS);
    cache = HeaderStringCache.getAndExpireOld();

    CharSequence t3 = cache.lookupValue(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "hello world"));
    Assert.assertTrue(t3 instanceof AsciiString);

    Assert.assertSame(t3, t2);

    Time.advance(1, TimeUnit.SECONDS);
    HeaderStringCache.getAndExpireOld();

    Time.advance(1, TimeUnit.SECONDS); // after two seconds, the cache should forget
    cache = HeaderStringCache.getAndExpireOld();

    CharSequence t4 = cache.lookupValue("hello world");
    Assert.assertTrue(t4 instanceof AsciiString);
    Assert.assertNotSame(t4, t3);
  }
}

package com.linkedin.alpini.netty4.compression;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Test CompressionUtils Class
 *
 * @author Eun-Gyu Kim <eukim@linkedin.com>
 */
public class TestCompressionUtils {
  @Test(groups = { "unit" })
  public void testIsSupportedEncoding() {
    Assert.assertFalse(CompressionUtils.isSupportedEncoding(null));

    final String f1 = "snappy";
    Assert.assertTrue(CompressionUtils.isSupportedEncoding(f1));

    final String f2 = "deflate";
    Assert.assertTrue(CompressionUtils.isSupportedEncoding(f2));

    final String f3 = "identity";
    Assert.assertTrue(CompressionUtils.isSupportedEncoding(f3));

    final String f4 = "gzip";
    Assert.assertTrue(CompressionUtils.isSupportedEncoding(f4));

    final String f5 = "something";
    Assert.assertFalse(CompressionUtils.isSupportedEncoding(f5));
  }

  @Test(groups = { "unit" })
  public void testIsSupportedEncodingWithQuality() {
    Assert.assertFalse(CompressionUtils.isSupportedEncoding(null));

    final String f1 = "snappy;q=1.0";
    Assert.assertTrue(CompressionUtils.isSupportedEncoding(f1));

    final String f1a = "snappy ; q = 1.0";
    Assert.assertTrue(CompressionUtils.isSupportedEncoding(f1a));

    final String f1b = "snappy ; q = 1.0 , foobar";
    Assert.assertTrue(CompressionUtils.isSupportedEncoding(f1b));

    final String f2 = "deflate;q=1.0";
    Assert.assertTrue(CompressionUtils.isSupportedEncoding(f2));

    final String f3 = "identity;q=1.0";
    Assert.assertTrue(CompressionUtils.isSupportedEncoding(f3));

    final String f4 = "gzip;q=1.0";
    Assert.assertTrue(CompressionUtils.isSupportedEncoding(f4));

    final String f5 = "something;q=1.0";
    Assert.assertFalse(CompressionUtils.isSupportedEncoding(f5));
  }

  @Test(groups = { "unit" })
  public void testIsSupportedEncodingWithQualityZero() {
    Assert.assertFalse(CompressionUtils.isSupportedEncoding(null));

    final String f1 = "snappy;q=0.0";
    Assert.assertFalse(CompressionUtils.isSupportedEncoding(f1));

    final String f1a = "snappy ; q = 0.0";
    Assert.assertFalse(CompressionUtils.isSupportedEncoding(f1a));

    final String f1b = "snappy ; q = 0.0 , foobar";
    Assert.assertFalse(CompressionUtils.isSupportedEncoding(f1b));

    final String f2 = "deflate;q=0.0";
    Assert.assertFalse(CompressionUtils.isSupportedEncoding(f2));

    final String f3 = "identity;q=0.0";
    Assert.assertFalse(CompressionUtils.isSupportedEncoding(f3));

    final String f4 = "gzip;q=0.0";
    Assert.assertFalse(CompressionUtils.isSupportedEncoding(f4));

    final String f5 = "something";
    Assert.assertFalse(CompressionUtils.isSupportedEncoding(f5));

    final String f6 = "snappy;q=0.0,deflate;q=0.0,identity;q=0.0,gzip;q=0.0";
    Assert.assertFalse(CompressionUtils.isSupportedEncoding(f6));
  }

  @Test(groups = { "unit" })
  public void testIsSupportedEncodingWithQualityMixed() {
    Assert.assertFalse(CompressionUtils.isSupportedEncoding(null));

    final String f1 = "deflate;q=0.0,snappy;q=1.0";
    Assert.assertTrue(CompressionUtils.isSupportedEncoding(f1));

    final String f2 = "identity;q=0.0,deflate;q=1.0";
    Assert.assertTrue(CompressionUtils.isSupportedEncoding(f2));

    final String f3 = "gzip;q=0.0,identity;q=1.0";
    Assert.assertTrue(CompressionUtils.isSupportedEncoding(f3));

    final String f4 = "deflate;q=0.0,gzip;q=1.0";
    Assert.assertTrue(CompressionUtils.isSupportedEncoding(f4));

    final String f5 = "deflate;q=0.0,gzip;q=yellow";
    Assert.assertFalse(CompressionUtils.isSupportedEncoding(f5));

    final String f6 = "something;q=1.0";
    Assert.assertFalse(CompressionUtils.isSupportedEncoding(f6));
  }

  @Test(groups = { "unit" })
  public void testIsCompatibleEncodings() {
    Assert.assertTrue(CompressionUtils.isCompatibleEncoding(null, "anything;at;all"));
    Assert.assertFalse(CompressionUtils.isCompatibleEncoding("gzip", null));
    Assert.assertTrue(CompressionUtils.isCompatibleEncoding("identity", null));
    Assert.assertFalse(
        CompressionUtils.isCompatibleEncoding("gzip", "snappy;q=0.0,deflate;q=0.0,identity;q=0.0,gzip;q=0.0"));
    Assert.assertFalse(
        CompressionUtils.isCompatibleEncoding("gzip", "snappy;q=1.0,deflate;q=0.0,identity;q=0.0,gzip;q=0.0"));
    Assert.assertTrue(
        CompressionUtils.isCompatibleEncoding("gzip", "snappy;q=0.0,deflate;q=0.0,identity;q=0.0,gzip;q=1.0"));
    Assert.assertTrue(CompressionUtils.isCompatibleEncoding("gzip", "snappy;q=0.0,deflate;q=0.0,identity;q=0.0,gzip"));
    Assert.assertFalse(
        CompressionUtils.isCompatibleEncoding("gzip", "snappy;q=0.0,deflate;q=0.0,identity;q=0.0,gzip;q=yellow"));
    Assert.assertFalse(
        CompressionUtils.isCompatibleEncoding("foobar", "snappy;q=0.0,deflate;q=0.0,identity;q=0.0,gzip;q=0.0"));
    Assert.assertFalse(
        CompressionUtils.isCompatibleEncoding("foobar", "snappy;q=1.0,deflate;q=0.0,identity;q=0.0,gzip;q=0.0"));
    Assert.assertTrue(
        CompressionUtils
            .isCompatibleEncoding("foobar", "snappy;q=0.0,deflate;q=0.0,foobar;q=0.1,identity;q=0.0,gzip;q=1.0"));

  }
}

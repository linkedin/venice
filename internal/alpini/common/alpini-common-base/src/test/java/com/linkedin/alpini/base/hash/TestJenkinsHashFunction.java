package com.linkedin.alpini.base.hash;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Test values are taken from the original Jenkins hash source code.
 *
 * @see {@literal http://burtleburtle.net/bob/c/lookup.c}
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class TestJenkinsHashFunction {
  @Test(groups = "unit")
  public void testDriver5() {

    JenkinsHashFunction jenkins = new JenkinsHashFunction();

    Assert.assertEquals(jenkins.hashlittle(ByteBuffer.allocate(0), 0), 0xdeadbeef);
    Assert.assertEquals(jenkins.hashlittle2(ByteBuffer.allocate(0), 0), 0xdeadbeefdeadbeefL);

    Assert.assertEquals(jenkins.hashlittle2(ByteBuffer.allocate(0), 0xdeadbeef00000000L), 0xdeadbeefbd5b7ddeL);

    Assert.assertEquals(jenkins.hashlittle2(ByteBuffer.allocate(0), 0xdeadbeefdeadbeefL), 0xbd5b7dde9c093ccdL);

    Assert.assertEquals(
        jenkins.hashlittle2(ByteBuffer.wrap("Four score and seven years ago".getBytes(StandardCharsets.US_ASCII)), 0),
        0xce7226e617770551L);

    Assert.assertEquals(
        jenkins.hashlittle2(
            ByteBuffer.wrap("Four score and seven years ago".getBytes(StandardCharsets.US_ASCII)),
            0x0000000100000000L),
        0xbd371de4e3607caeL);

    Assert.assertEquals(
        jenkins.hashlittle2(
            ByteBuffer.wrap("Four score and seven years ago".getBytes(StandardCharsets.US_ASCII)),
            0x0000000000000001L),
        0x6cbea4b3cd628161L);

    Assert.assertEquals(
        jenkins.hashlittle(ByteBuffer.wrap("Four score and seven years ago".getBytes(StandardCharsets.US_ASCII)), 0),
        0x17770551);

    Assert.assertEquals(
        jenkins.hashlittle(ByteBuffer.wrap("Four score and seven years ago".getBytes(StandardCharsets.US_ASCII)), 1),
        0xcd628161);
  }
}

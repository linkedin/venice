package io.netty.handler.codec.http2;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;


/**
 * Tests for {@link EspressoHttp2MultiplexHandler} for reuseChildChannels - true, offloadChildChannels - false
 *
 * @author Abhishek Andhavarapu
 */
@Test(groups = "unit")
public class TestHttp2MultiplexReuseChildChannels extends TestHttp2Multiplex<Http2FrameCodec> {
  public EspressoHttp2MultiplexHandler multiplexer;

  @Override
  protected Http2FrameCodec newCodec(TestChannelInitializer childChannelInitializer, Http2FrameWriter frameWriter) {
    return new Http2FrameCodecBuilder(true).frameWriter(frameWriter).build();
  }

  @Override
  protected EspressoHttp2MultiplexHandler newMultiplexer(TestChannelInitializer childChannelInitializer) {
    this.multiplexer = new EspressoHttp2MultiplexHandler(childChannelInitializer, true, false);
    return multiplexer;
  }

  @Override
  protected boolean useUserEventForResetFrame() {
    return true;
  }

  @Override
  protected boolean ignoreWindowUpdateFrames() {
    return true;
  }

  @AfterMethod(groups = "unit")
  @Override
  public void tearDown() throws Exception {
    // Child channel is closed and so not recycled
    assertEquals(0, ((EspressoHttp2MultiplexHandler) multiplexer).getChildChannelPool().size());
    super.tearDown();
  }
}

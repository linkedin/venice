package io.netty.handler.codec.http2;

import com.linkedin.alpini.netty4.handlers.Http2SettingsFrameLogger;
import com.linkedin.alpini.netty4.http2.Http2PipelineInitializer;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.logging.LogLevel;
import java.util.function.Consumer;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests ActiveStreamsCountHandler
 *
 * @author Abhishek Andhavarapu
 */
public class TestActiveStreamsCountHandler {
  @Test(groups = "unit")
  public void testActiveStreams() {
    ActiveStreamsCountHandler countHandler = new ActiveStreamsCountHandler();
    Http2SettingsFrameLogger http2SettingsFrameLogger = new Http2SettingsFrameLogger(LogLevel.INFO);

    Consumer<ChannelPipeline> existingHttpPipelineInitializer = new Consumer<ChannelPipeline>() {
      @Override
      public void accept(ChannelPipeline pipeline) {
        pipeline.addLast(new ChannelInboundHandlerAdapter() {
          @Override
          public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            // This handler is after the ActiveStreamsCountHandler. At this stage, the number of active streams should
            // be 1.
            Assert.assertEquals(countHandler.getActiveStreamsCount(), 1);
            // Write the response back
            ctx.writeAndFlush(new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK));
          }
        });
      }
    };

    EmbeddedChannel channel = new EmbeddedChannel(
        Http2PipelineInitializer.DEFAULT_BUILDER.get()
            .http2Settings(new Http2Settings())
            .activeStreamsCountHandler(countHandler)
            .http2SettingsFrameLogger(http2SettingsFrameLogger)
            .existingHttpPipelineInitializer(existingHttpPipelineInitializer)
            .maxInitialLineLength(8192)
            .maxHeaderSize(8192)
            .maxChunkSize(256)
            .validateHeaders(false)
            .build());

    // Write an empty request
    channel.writeInbound(new DefaultHttp2HeadersFrame(EmptyHttp2Headers.INSTANCE, true));
    channel.flushInbound();

    // Assert the stream is closed and the number of active stream is decremented to zero.
    Assert.assertEquals(countHandler.getActiveStreamsCount(), 0);
  }

}

package com.linkedin.venice.listener;

import com.linkedin.alpini.netty4.handlers.Http2SettingsFrameLogger;
import com.linkedin.alpini.netty4.http2.Http2PipelineInitializer;
import com.linkedin.davinci.config.VeniceServerConfig;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http2.ActiveStreamsCountHandler;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.logging.LogLevel;
import java.util.function.Consumer;


/**
 * This utility is used to manage all the HTTP/2 related configs and offer a way to spin up a pipeline to support
 * both HTTP/1.1 and HTTP/2.
 */
public class VeniceHttp2PipelineInitializerBuilder {
  /**
   * The following constants are expected to be changed and since they are for codec handler.
   */
  private static final int DEFAULT_MAX_INITIAL_LINE_LENGTH = 4096;
  private static final int DEFAULT_MAX_HEADER_SIZE = 8192;
  private static final int DEFAULT_MAX_CHUNK_SIZE = 8192;

  private final VeniceServerConfig serverConfig;

  private static final ActiveStreamsCountHandler ACTIVE_STREAMS_COUNT_HANDLER = new ActiveStreamsCountHandler();
  private static final Http2SettingsFrameLogger HTTP2_SETTINGS_FRAME_LOGGER =
      new Http2SettingsFrameLogger(LogLevel.INFO);

  public VeniceHttp2PipelineInitializerBuilder(VeniceServerConfig serverConfig) {
    this.serverConfig = serverConfig;
  }

  private Http2Settings getServerHttpSettings() {
    return new Http2Settings().maxConcurrentStreams(serverConfig.getHttp2MaxConcurrentStreams())
        .maxFrameSize(serverConfig.getHttp2MaxFrameSize())
        .initialWindowSize(serverConfig.getHttp2InitialWindowSize())
        .headerTableSize(serverConfig.getHttp2HeaderTableSize())
        .maxHeaderListSize(serverConfig.getHttp2MaxHeaderListSize());
  }

  /**
   * This function will leverage the existing HTTP/1.1 pipeline for both HTTP/1.1 and HTTP/2.
   *
   * @param existingHttpPipelineInitializer
   * @return
   */
  public Http2PipelineInitializer createHttp2PipelineInitializer(
      Consumer<ChannelPipeline> existingHttpPipelineInitializer) {
    return Http2PipelineInitializer.DEFAULT_BUILDER.get()
        .http2Settings(getServerHttpSettings())
        .activeStreamsCountHandler(ACTIVE_STREAMS_COUNT_HANDLER)
        .http2SettingsFrameLogger(HTTP2_SETTINGS_FRAME_LOGGER)
        .existingHttpPipelineInitializer(existingHttpPipelineInitializer)
        .maxInitialLineLength(DEFAULT_MAX_INITIAL_LINE_LENGTH)
        .maxHeaderSize(DEFAULT_MAX_HEADER_SIZE)
        .maxChunkSize(DEFAULT_MAX_CHUNK_SIZE)
        .validateHeaders(false)
        .build();
  }
}

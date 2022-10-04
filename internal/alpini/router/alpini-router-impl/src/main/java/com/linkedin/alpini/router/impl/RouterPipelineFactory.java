package com.linkedin.alpini.router.impl;

import java.util.function.Supplier;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public interface RouterPipelineFactory<CHANNEL_HANDLER> {
  RouterPipelineFactory addBeforeHttpServerCodec(String name, Supplier<? extends CHANNEL_HANDLER> supplier);

  RouterPipelineFactory addBeforeChunkAggregator(String name, Supplier<? extends CHANNEL_HANDLER> supplier);

  RouterPipelineFactory addBeforeIdleStateHandler(String name, Supplier<? extends CHANNEL_HANDLER> supplier);

  RouterPipelineFactory addBeforeHttpRequestHandler(String name, Supplier<? extends CHANNEL_HANDLER> supplier);

}

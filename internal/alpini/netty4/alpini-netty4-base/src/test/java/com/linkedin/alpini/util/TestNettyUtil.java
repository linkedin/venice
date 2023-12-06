package com.linkedin.alpini.util;

import java.util.function.Supplier;
import org.testng.SkipException;


public class TestNettyUtil {
  public static <T> T skipEpollIfNotFound(Supplier<T> eventLoopGroupSupplier) {
    try {
      return eventLoopGroupSupplier.get();
    } catch (NoClassDefFoundError e) {
      if (e.getMessage().equals("Could not initialize class io.netty.channel.epoll.EpollEventLoop")) {
        throw new SkipException("EPoll not available.");
      }
      throw e;
    } catch (UnsatisfiedLinkError e) {
      if (e.getMessage().equals("failed to load the required native library")) {
        throw new SkipException("EPoll not available.");
      }
      throw e;
    }
  }
}

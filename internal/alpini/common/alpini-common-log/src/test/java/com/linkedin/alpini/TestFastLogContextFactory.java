package com.linkedin.alpini;

import com.linkedin.alpini.log.FastLogContextFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.MessageFactory;
import org.apache.logging.log4j.spi.ExtendedLogger;
import org.apache.logging.log4j.spi.LoggerContext;
import org.apache.logging.log4j.spi.LoggerContextFactory;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test(groups = "unit")
public class TestFastLogContextFactory {
  @Test
  public void testConstruct() {
    LoggerContextFactory oldFactory = LogManager.getFactory();
    try {
      LogManager.setFactory(new FastLogContextFactory());

      LogManager.getLogger(getClass()).error("Hello World"); // this should never reach the underlying logger

    } finally {
      LogManager.setFactory(oldFactory);
    }
  }

  @Test
  public void testLog() {

    LoggerContextFactory oldFactory = LogManager.getFactory();
    try {
      LoggerContextFactory mockFactory = Mockito.mock(LoggerContextFactory.class);
      LogManager.setFactory(new FastLogContextFactory(mockFactory));

      LoggerContext mockContext = Mockito.mock(LoggerContext.class);

      Mockito
          .when(
              mockFactory
                  .getContext(Mockito.anyString(), Mockito.any(ClassLoader.class), Mockito.any(), Mockito.anyBoolean()))
          .thenReturn(mockContext);

      ExtendedLogger mockLogger = Mockito.mock(ExtendedLogger.class);
      Mockito.when(mockContext.getLogger(Mockito.anyString())).thenReturn(mockLogger);

      Logger logger = LogManager.getLogger(TestFastLogContextFactory.class);

      Mockito.verify(mockFactory)
          .getContext(
              Mockito.eq("org.apache.logging.log4j.LogManager"),
              Mockito.any(ClassLoader.class),
              Mockito.isNull(),
              Mockito.eq(false));

      Mockito.verify(mockContext).getLogger(Mockito.eq(getClass().getName()));

      logger.debug("Hello World"); // this should never reach the underlying logger

      Mockito.verifyNoMoreInteractions(mockFactory, mockContext, mockLogger);

    } finally {
      LogManager.setFactory(oldFactory);
    }
  }

  @Test
  public void testLogContext() {

    LoggerContextFactory oldFactory = LogManager.getFactory();
    try {
      LoggerContextFactory mockFactory = Mockito.mock(LoggerContextFactory.class);
      LogManager.setFactory(new FastLogContextFactory(mockFactory));

      LoggerContext mockContext = Mockito.mock(LoggerContext.class);

      Mockito.when(mockFactory.getContext(Mockito.anyString(), Mockito.any(), Mockito.any(), Mockito.anyBoolean()))
          .thenReturn(mockContext);

      ExtendedLogger mockLogger = Mockito.mock(ExtendedLogger.class);
      Mockito.when(mockContext.getLogger(Mockito.anyString())).thenReturn(mockLogger);

      LoggerContext context = LogManager.getContext();

      Assert.assertFalse(context.hasLogger("foo"));

      Mockito.verify(mockFactory)
          .getContext(
              Mockito.eq("org.apache.logging.log4j.LogManager"),
              Mockito.isNull(ClassLoader.class),
              Mockito.isNull(),
              Mockito.eq(true));

      Mockito.verify(mockContext).hasLogger(Mockito.eq("foo"));

      Mockito.verifyNoMoreInteractions(mockFactory, mockContext, mockLogger);

      MessageFactory factory = Mockito.mock(MessageFactory.class);

      Assert.assertFalse(context.hasLogger("bar", factory));

      Mockito.verify(mockContext).hasLogger(Mockito.eq("bar"), Mockito.same(factory));

      Mockito.verifyNoMoreInteractions(mockFactory, mockContext, mockLogger);

      Assert.assertFalse(context.hasLogger("xyz", factory.getClass()));

      Mockito.verify(mockContext).hasLogger(Mockito.eq("xyz"), Mockito.same(factory.getClass()));

      Mockito.verifyNoMoreInteractions(mockFactory, mockContext, mockLogger);

    } finally {
      LogManager.setFactory(oldFactory);
    }
  }

}

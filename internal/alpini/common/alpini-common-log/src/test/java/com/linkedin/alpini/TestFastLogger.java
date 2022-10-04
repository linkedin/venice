package com.linkedin.alpini;

import com.linkedin.alpini.log.FastLogContextFactory;
import com.linkedin.alpini.log.FastLogMBean;
import com.linkedin.alpini.log.FastLogger;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.message.EntryMessage;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.message.MessageFactory;
import org.apache.logging.log4j.spi.ExtendedLogger;
import org.apache.logging.log4j.spi.LoggerContext;
import org.apache.logging.log4j.spi.LoggerContextFactory;
import org.apache.logging.log4j.util.MessageSupplier;
import org.apache.logging.log4j.util.Supplier;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


@Test(groups = "unit")
public class TestFastLogger {
  static final LoggerContextFactory ORIGINAL_FACTORY = LogManager.getFactory();

  LoggerContextFactory _mockFactory;
  LoggerContext _mockContext;
  ExtendedLogger _mockLogger;
  FastLogMBean _mBean;

  @BeforeClass
  public void beforeClass() {
    _mockFactory = Mockito.mock(LoggerContextFactory.class);

    _mockContext = Mockito.mock(LoggerContext.class);

    Mockito
        .when(
            _mockFactory
                .getContext(Mockito.anyString(), Mockito.any(ClassLoader.class), Mockito.any(), Mockito.anyBoolean()))
        .thenReturn(_mockContext);

    _mockLogger = Mockito.mock(ExtendedLogger.class);
    Mockito.when(_mockContext.getLogger(Mockito.anyString())).thenReturn(_mockLogger);

    LogManager.setFactory(new FastLogContextFactory(_mockFactory));

    Logger logger = LogManager.getLogger(getClass());

    _mBean = ((FastLogger) logger).getManagementMBean();

    Mockito.verify(_mockFactory)
        .getContext(
            Mockito.eq("org.apache.logging.log4j.LogManager"),
            Mockito.any(ClassLoader.class),
            Mockito.isNull(),
            Mockito.eq(false));

    verifyMockContext();
  }

  @AfterClass(alwaysRun = true)
  public void afterClass() {
    LogManager.setFactory(ORIGINAL_FACTORY);
  }

  @BeforeMethod
  public void beforeMethod() {
    Mockito.reset(_mockLogger, _mockContext);
    Mockito.when(_mockContext.getLogger(Mockito.anyString())).thenReturn(_mockLogger);
  }

  private void verifyMockContext() {
    Mockito.verify(_mockContext).getLogger(Mockito.eq(getClass().getName()));
  }

  @Test
  public void testErrorMarkerMessage() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    Message message = Mockito.mock(Message.class);

    log.error(marker, message);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(Mockito.same(marker), Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorMarkerMessageThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    Message message = Mockito.mock(Message.class);
    Throwable ex = new Error();

    log.error(marker, message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(Mockito.same(marker), Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorMarkerMessageSupplier() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    MessageSupplier message = Mockito.mock(MessageSupplier.class);

    log.error(marker, message);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(Mockito.same(marker), Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorMarkerMessageSupplierThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    MessageSupplier message = Mockito.mock(MessageSupplier.class);
    Throwable ex = new Error();

    log.error(marker, message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(Mockito.same(marker), Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorMarkerSupplier() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    Supplier message = Mockito.mock(Supplier.class);

    log.error(marker, message);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(Mockito.same(marker), Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorMarkerSupplierThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    Supplier message = Mockito.mock(Supplier.class);
    Throwable ex = new Error();

    log.error(marker, message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(Mockito.same(marker), Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorMarkerCharSequence() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    CharSequence message = Mockito.mock(CharSequence.class);
    log.error(marker, message);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(Mockito.same(marker), Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorMarkerCharSequenceThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    CharSequence message = Mockito.mock(CharSequence.class);
    Throwable ex = new Error();
    log.error(marker, message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(Mockito.same(marker), Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorMarkerObject() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    Object message = new Object();
    log.error(marker, message);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(Mockito.same(marker), Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorMarkerObjectThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    Object message = new Object();
    Throwable ex = new Error();
    log.error(marker, message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(Mockito.same(marker), Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorMarkerString() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    log.error(marker, message);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(Mockito.same(marker), Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorMarkerStringThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Throwable ex = new Error();
    log.error(marker, message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(Mockito.same(marker), Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorMarkerStringSupplierVararg() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Supplier<?> p0 = Mockito.mock(Supplier.class);
    Supplier<?> p1 = Mockito.mock(Supplier.class);
    Supplier<?> p2 = Mockito.mock(Supplier.class);
    Supplier<?> p3 = Mockito.mock(Supplier.class);
    Supplier<?> p4 = Mockito.mock(Supplier.class);
    Supplier<?> p5 = Mockito.mock(Supplier.class);
    Supplier<?> p6 = Mockito.mock(Supplier.class);
    Supplier<?> p7 = Mockito.mock(Supplier.class);
    Supplier<?> p8 = Mockito.mock(Supplier.class);
    Supplier<?> p9 = Mockito.mock(Supplier.class);
    Supplier<?> pa = Mockito.mock(Supplier.class);
    log.error(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorMarkerStringVararg() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    Object p8 = new Object();
    Object p9 = new Object();
    Object pa = new Object();
    log.error(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorMarkerStringP9() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    Object p8 = new Object();
    Object p9 = new Object();
    log.error(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorMarkerStringP8() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    Object p8 = new Object();
    log.error(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorMarkerStringP7() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    log.error(marker, message, p0, p1, p2, p3, p4, p5, p6, p7);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(marker, message, p0, p1, p2, p3, p4, p5, p6, p7);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorMarkerStringP6() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    log.error(marker, message, p0, p1, p2, p3, p4, p5, p6);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(marker, message, p0, p1, p2, p3, p4, p5, p6);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorMarkerStringP5() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    log.error(marker, message, p0, p1, p2, p3, p4, p5);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(marker, message, p0, p1, p2, p3, p4, p5);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorMarkerStringP4() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    log.error(marker, message, p0, p1, p2, p3, p4);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(marker, message, p0, p1, p2, p3, p4);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorMarkerStringP3() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    log.error(marker, message, p0, p1, p2, p3);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(marker, message, p0, p1, p2, p3);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorMarkerStringP2() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    log.error(marker, message, p0, p1, p2);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(marker, message, p0, p1, p2);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorMarkerStringP1() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    log.error(marker, message, p0, p1);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(marker, message, p0, p1);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testMarkerErrorStringP0() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    log.error(marker, message, p0);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(marker, message, p0);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorMessage() {
    Logger log = LogManager.getLogger(getClass());

    Message message = Mockito.mock(Message.class);

    log.error(message);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorMessageThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Message message = Mockito.mock(Message.class);
    Throwable ex = new Error();

    log.error(message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorMessageSupplier() {
    Logger log = LogManager.getLogger(getClass());

    MessageSupplier message = Mockito.mock(MessageSupplier.class);

    log.error(message);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorMessageSupplierThrowable() {
    Logger log = LogManager.getLogger(getClass());

    MessageSupplier message = Mockito.mock(MessageSupplier.class);
    Throwable ex = new Error();

    log.error(message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorSupplier() {
    Logger log = LogManager.getLogger(getClass());

    Supplier<?> message = Mockito.mock(Supplier.class);

    log.error(message);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorSupplierThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Supplier message = Mockito.mock(Supplier.class);
    Throwable ex = new Error();

    log.error(message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorCharSequence() {
    Logger log = LogManager.getLogger(getClass());

    CharSequence message = Mockito.mock(CharSequence.class);
    log.error(message);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorCharSequenceThrowable() {
    Logger log = LogManager.getLogger(getClass());

    CharSequence message = Mockito.mock(CharSequence.class);
    Throwable ex = new Error();
    log.error(message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorObject() {
    Logger log = LogManager.getLogger(getClass());

    Object message = new Object();
    log.error(message);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorObjectThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Object message = new Object();
    Throwable ex = new Error();
    log.error(message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorString() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    log.error(message);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorStringThrowable() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Throwable ex = new Error();
    log.error(message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorStringSupplierVararg() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Supplier<?> p0 = Mockito.mock(Supplier.class);
    Supplier<?> p1 = Mockito.mock(Supplier.class);
    Supplier<?> p2 = Mockito.mock(Supplier.class);
    Supplier<?> p3 = Mockito.mock(Supplier.class);
    Supplier<?> p4 = Mockito.mock(Supplier.class);
    Supplier<?> p5 = Mockito.mock(Supplier.class);
    Supplier<?> p6 = Mockito.mock(Supplier.class);
    Supplier<?> p7 = Mockito.mock(Supplier.class);
    Supplier<?> p8 = Mockito.mock(Supplier.class);
    Supplier<?> p9 = Mockito.mock(Supplier.class);
    Supplier<?> pa = Mockito.mock(Supplier.class);
    log.error(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorStringVararg() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    Object p8 = new Object();
    Object p9 = new Object();
    Object pa = new Object();
    log.error(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorStringP9() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    Object p8 = new Object();
    Object p9 = new Object();
    log.error(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorStringP8() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    Object p8 = new Object();
    log.error(message, p0, p1, p2, p3, p4, p5, p6, p7, p8);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorStringP7() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    log.error(message, p0, p1, p2, p3, p4, p5, p6, p7);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(message, p0, p1, p2, p3, p4, p5, p6, p7);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorStringP6() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    log.error(message, p0, p1, p2, p3, p4, p5, p6);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(message, p0, p1, p2, p3, p4, p5, p6);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorStringP5() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    log.error(message, p0, p1, p2, p3, p4, p5);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(message, p0, p1, p2, p3, p4, p5);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorStringP4() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    log.error(message, p0, p1, p2, p3, p4);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(message, p0, p1, p2, p3, p4);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorStringP3() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    log.error(message, p0, p1, p2, p3);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(message, p0, p1, p2, p3);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorStringP2() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    log.error(message, p0, p1, p2);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(message, p0, p1, p2);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorStringP1() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    log.error(message, p0, p1);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(message, p0, p1);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testErrorStringP0() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    log.error(message, p0);

    verifyMockContext();
    Mockito.verify(_mockLogger).error(message, p0);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnMarkerMessage() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    Message message = Mockito.mock(Message.class);

    log.warn(marker, message);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(Mockito.same(marker), Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnMarkerMessageThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    Message message = Mockito.mock(Message.class);
    Throwable ex = new Error();

    log.warn(marker, message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(Mockito.same(marker), Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnMarkerMessageSupplier() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    MessageSupplier message = Mockito.mock(MessageSupplier.class);

    log.warn(marker, message);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(Mockito.same(marker), Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnMarkerMessageSupplierThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    MessageSupplier message = Mockito.mock(MessageSupplier.class);
    Throwable ex = new Error();

    log.warn(marker, message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(Mockito.same(marker), Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnMarkerSupplier() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    Supplier message = Mockito.mock(Supplier.class);

    log.warn(marker, message);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(Mockito.same(marker), Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnMarkerSupplierThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    Supplier message = Mockito.mock(Supplier.class);
    Throwable ex = new Error();

    log.warn(marker, message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(Mockito.same(marker), Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnMarkerCharSequence() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    CharSequence message = Mockito.mock(CharSequence.class);
    log.warn(marker, message);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(Mockito.same(marker), Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnMarkerCharSequenceThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    CharSequence message = Mockito.mock(CharSequence.class);
    Throwable ex = new Error();
    log.warn(marker, message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(Mockito.same(marker), Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnMarkerObject() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    Object message = new Object();
    log.warn(marker, message);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(Mockito.same(marker), Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnMarkerObjectThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    Object message = new Object();
    Throwable ex = new Error();
    log.warn(marker, message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(Mockito.same(marker), Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnMarkerString() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    log.warn(marker, message);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(Mockito.same(marker), Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnMarkerStringThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Throwable ex = new Error();
    log.warn(marker, message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(Mockito.same(marker), Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnMarkerStringSupplierVararg() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Supplier<?> p0 = Mockito.mock(Supplier.class);
    Supplier<?> p1 = Mockito.mock(Supplier.class);
    Supplier<?> p2 = Mockito.mock(Supplier.class);
    Supplier<?> p3 = Mockito.mock(Supplier.class);
    Supplier<?> p4 = Mockito.mock(Supplier.class);
    Supplier<?> p5 = Mockito.mock(Supplier.class);
    Supplier<?> p6 = Mockito.mock(Supplier.class);
    Supplier<?> p7 = Mockito.mock(Supplier.class);
    Supplier<?> p8 = Mockito.mock(Supplier.class);
    Supplier<?> p9 = Mockito.mock(Supplier.class);
    Supplier<?> pa = Mockito.mock(Supplier.class);
    log.warn(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnMarkerStringVararg() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    Object p8 = new Object();
    Object p9 = new Object();
    Object pa = new Object();
    log.warn(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnMarkerStringP9() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    Object p8 = new Object();
    Object p9 = new Object();
    log.warn(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnMarkerStringP8() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    Object p8 = new Object();
    log.warn(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnMarkerStringP7() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    log.warn(marker, message, p0, p1, p2, p3, p4, p5, p6, p7);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(marker, message, p0, p1, p2, p3, p4, p5, p6, p7);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnMarkerStringP6() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    log.warn(marker, message, p0, p1, p2, p3, p4, p5, p6);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(marker, message, p0, p1, p2, p3, p4, p5, p6);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnMarkerStringP5() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    log.warn(marker, message, p0, p1, p2, p3, p4, p5);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(marker, message, p0, p1, p2, p3, p4, p5);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnMarkerStringP4() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    log.warn(marker, message, p0, p1, p2, p3, p4);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(marker, message, p0, p1, p2, p3, p4);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnMarkerStringP3() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    log.warn(marker, message, p0, p1, p2, p3);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(marker, message, p0, p1, p2, p3);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnMarkerStringP2() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    log.warn(marker, message, p0, p1, p2);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(marker, message, p0, p1, p2);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnMarkerStringP1() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    log.warn(marker, message, p0, p1);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(marker, message, p0, p1);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testMarkerWarnStringP0() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    log.warn(marker, message, p0);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(marker, message, p0);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnMessage() {
    Logger log = LogManager.getLogger(getClass());

    Message message = Mockito.mock(Message.class);

    log.warn(message);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnMessageThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Message message = Mockito.mock(Message.class);
    Throwable ex = new Error();

    log.warn(message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnMessageSupplier() {
    Logger log = LogManager.getLogger(getClass());

    MessageSupplier message = Mockito.mock(MessageSupplier.class);

    log.warn(message);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnMessageSupplierThrowable() {
    Logger log = LogManager.getLogger(getClass());

    MessageSupplier message = Mockito.mock(MessageSupplier.class);
    Throwable ex = new Error();

    log.warn(message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnSupplier() {
    Logger log = LogManager.getLogger(getClass());

    Supplier<?> message = Mockito.mock(Supplier.class);

    log.warn(message);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnSupplierThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Supplier message = Mockito.mock(Supplier.class);
    Throwable ex = new Error();

    log.warn(message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnCharSequence() {
    Logger log = LogManager.getLogger(getClass());

    CharSequence message = Mockito.mock(CharSequence.class);
    log.warn(message);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnCharSequenceThrowable() {
    Logger log = LogManager.getLogger(getClass());

    CharSequence message = Mockito.mock(CharSequence.class);
    Throwable ex = new Error();
    log.warn(message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnObject() {
    Logger log = LogManager.getLogger(getClass());

    Object message = new Object();
    log.warn(message);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnObjectThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Object message = new Object();
    Throwable ex = new Error();
    log.warn(message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnString() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    log.warn(message);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnStringThrowable() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Throwable ex = new Error();
    log.warn(message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnStringSupplierVararg() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Supplier<?> p0 = Mockito.mock(Supplier.class);
    Supplier<?> p1 = Mockito.mock(Supplier.class);
    Supplier<?> p2 = Mockito.mock(Supplier.class);
    Supplier<?> p3 = Mockito.mock(Supplier.class);
    Supplier<?> p4 = Mockito.mock(Supplier.class);
    Supplier<?> p5 = Mockito.mock(Supplier.class);
    Supplier<?> p6 = Mockito.mock(Supplier.class);
    Supplier<?> p7 = Mockito.mock(Supplier.class);
    Supplier<?> p8 = Mockito.mock(Supplier.class);
    Supplier<?> p9 = Mockito.mock(Supplier.class);
    Supplier<?> pa = Mockito.mock(Supplier.class);
    log.warn(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnStringVararg() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    Object p8 = new Object();
    Object p9 = new Object();
    Object pa = new Object();
    log.warn(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnStringP9() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    Object p8 = new Object();
    Object p9 = new Object();
    log.warn(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnStringP8() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    Object p8 = new Object();
    log.warn(message, p0, p1, p2, p3, p4, p5, p6, p7, p8);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnStringP7() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    log.warn(message, p0, p1, p2, p3, p4, p5, p6, p7);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(message, p0, p1, p2, p3, p4, p5, p6, p7);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnStringP6() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    log.warn(message, p0, p1, p2, p3, p4, p5, p6);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(message, p0, p1, p2, p3, p4, p5, p6);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnStringP5() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    log.warn(message, p0, p1, p2, p3, p4, p5);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(message, p0, p1, p2, p3, p4, p5);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnStringP4() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    log.warn(message, p0, p1, p2, p3, p4);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(message, p0, p1, p2, p3, p4);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnStringP3() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    log.warn(message, p0, p1, p2, p3);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(message, p0, p1, p2, p3);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnStringP2() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    log.warn(message, p0, p1, p2);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(message, p0, p1, p2);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnStringP1() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    log.warn(message, p0, p1);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(message, p0, p1);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testWarnStringP0() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    log.warn(message, p0);

    verifyMockContext();
    Mockito.verify(_mockLogger).warn(message, p0);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalMarkerMessage() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    Message message = Mockito.mock(Message.class);

    log.fatal(marker, message);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(Mockito.same(marker), Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalMarkerMessageThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    Message message = Mockito.mock(Message.class);
    Throwable ex = new Error();

    log.fatal(marker, message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(Mockito.same(marker), Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalMarkerMessageSupplier() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    MessageSupplier message = Mockito.mock(MessageSupplier.class);

    log.fatal(marker, message);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(Mockito.same(marker), Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalMarkerMessageSupplierThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    MessageSupplier message = Mockito.mock(MessageSupplier.class);
    Throwable ex = new Error();

    log.fatal(marker, message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(Mockito.same(marker), Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalMarkerSupplier() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    Supplier message = Mockito.mock(Supplier.class);

    log.fatal(marker, message);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(Mockito.same(marker), Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalMarkerSupplierThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    Supplier message = Mockito.mock(Supplier.class);
    Throwable ex = new Error();

    log.fatal(marker, message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(Mockito.same(marker), Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalMarkerCharSequence() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    CharSequence message = Mockito.mock(CharSequence.class);
    log.fatal(marker, message);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(Mockito.same(marker), Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalMarkerCharSequenceThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    CharSequence message = Mockito.mock(CharSequence.class);
    Throwable ex = new Error();
    log.fatal(marker, message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(Mockito.same(marker), Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalMarkerObject() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    Object message = new Object();
    log.fatal(marker, message);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(Mockito.same(marker), Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalMarkerObjectThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    Object message = new Object();
    Throwable ex = new Error();
    log.fatal(marker, message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(Mockito.same(marker), Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalMarkerString() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    log.fatal(marker, message);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(Mockito.same(marker), Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalMarkerStringThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Throwable ex = new Error();
    log.fatal(marker, message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(Mockito.same(marker), Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalMarkerStringSupplierVararg() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Supplier<?> p0 = Mockito.mock(Supplier.class);
    Supplier<?> p1 = Mockito.mock(Supplier.class);
    Supplier<?> p2 = Mockito.mock(Supplier.class);
    Supplier<?> p3 = Mockito.mock(Supplier.class);
    Supplier<?> p4 = Mockito.mock(Supplier.class);
    Supplier<?> p5 = Mockito.mock(Supplier.class);
    Supplier<?> p6 = Mockito.mock(Supplier.class);
    Supplier<?> p7 = Mockito.mock(Supplier.class);
    Supplier<?> p8 = Mockito.mock(Supplier.class);
    Supplier<?> p9 = Mockito.mock(Supplier.class);
    Supplier<?> pa = Mockito.mock(Supplier.class);
    log.fatal(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalMarkerStringVararg() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    Object p8 = new Object();
    Object p9 = new Object();
    Object pa = new Object();
    log.fatal(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalMarkerStringP9() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    Object p8 = new Object();
    Object p9 = new Object();
    log.fatal(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalMarkerStringP8() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    Object p8 = new Object();
    log.fatal(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalMarkerStringP7() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    log.fatal(marker, message, p0, p1, p2, p3, p4, p5, p6, p7);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(marker, message, p0, p1, p2, p3, p4, p5, p6, p7);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalMarkerStringP6() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    log.fatal(marker, message, p0, p1, p2, p3, p4, p5, p6);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(marker, message, p0, p1, p2, p3, p4, p5, p6);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalMarkerStringP5() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    log.fatal(marker, message, p0, p1, p2, p3, p4, p5);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(marker, message, p0, p1, p2, p3, p4, p5);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalMarkerStringP4() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    log.fatal(marker, message, p0, p1, p2, p3, p4);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(marker, message, p0, p1, p2, p3, p4);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalMarkerStringP3() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    log.fatal(marker, message, p0, p1, p2, p3);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(marker, message, p0, p1, p2, p3);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalMarkerStringP2() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    log.fatal(marker, message, p0, p1, p2);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(marker, message, p0, p1, p2);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalMarkerStringP1() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    log.fatal(marker, message, p0, p1);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(marker, message, p0, p1);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testMarkerFatalStringP0() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    log.fatal(marker, message, p0);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(marker, message, p0);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalMessage() {
    Logger log = LogManager.getLogger(getClass());

    Message message = Mockito.mock(Message.class);

    log.fatal(message);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalMessageThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Message message = Mockito.mock(Message.class);
    Throwable ex = new Error();

    log.fatal(message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalMessageSupplier() {
    Logger log = LogManager.getLogger(getClass());

    MessageSupplier message = Mockito.mock(MessageSupplier.class);

    log.fatal(message);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalMessageSupplierThrowable() {
    Logger log = LogManager.getLogger(getClass());

    MessageSupplier message = Mockito.mock(MessageSupplier.class);
    Throwable ex = new Error();

    log.fatal(message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalSupplier() {
    Logger log = LogManager.getLogger(getClass());

    Supplier<?> message = Mockito.mock(Supplier.class);

    log.fatal(message);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalSupplierThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Supplier message = Mockito.mock(Supplier.class);
    Throwable ex = new Error();

    log.fatal(message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalCharSequence() {
    Logger log = LogManager.getLogger(getClass());

    CharSequence message = Mockito.mock(CharSequence.class);
    log.fatal(message);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalCharSequenceThrowable() {
    Logger log = LogManager.getLogger(getClass());

    CharSequence message = Mockito.mock(CharSequence.class);
    Throwable ex = new Error();
    log.fatal(message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalObject() {
    Logger log = LogManager.getLogger(getClass());

    Object message = new Object();
    log.fatal(message);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalObjectThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Object message = new Object();
    Throwable ex = new Error();
    log.fatal(message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalString() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    log.fatal(message);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalStringThrowable() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Throwable ex = new Error();
    log.fatal(message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalStringSupplierVararg() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Supplier<?> p0 = Mockito.mock(Supplier.class);
    Supplier<?> p1 = Mockito.mock(Supplier.class);
    Supplier<?> p2 = Mockito.mock(Supplier.class);
    Supplier<?> p3 = Mockito.mock(Supplier.class);
    Supplier<?> p4 = Mockito.mock(Supplier.class);
    Supplier<?> p5 = Mockito.mock(Supplier.class);
    Supplier<?> p6 = Mockito.mock(Supplier.class);
    Supplier<?> p7 = Mockito.mock(Supplier.class);
    Supplier<?> p8 = Mockito.mock(Supplier.class);
    Supplier<?> p9 = Mockito.mock(Supplier.class);
    Supplier<?> pa = Mockito.mock(Supplier.class);
    log.fatal(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalStringVararg() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    Object p8 = new Object();
    Object p9 = new Object();
    Object pa = new Object();
    log.fatal(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalStringP9() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    Object p8 = new Object();
    Object p9 = new Object();
    log.fatal(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalStringP8() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    Object p8 = new Object();
    log.fatal(message, p0, p1, p2, p3, p4, p5, p6, p7, p8);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalStringP7() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    log.fatal(message, p0, p1, p2, p3, p4, p5, p6, p7);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(message, p0, p1, p2, p3, p4, p5, p6, p7);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalStringP6() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    log.fatal(message, p0, p1, p2, p3, p4, p5, p6);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(message, p0, p1, p2, p3, p4, p5, p6);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalStringP5() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    log.fatal(message, p0, p1, p2, p3, p4, p5);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(message, p0, p1, p2, p3, p4, p5);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalStringP4() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    log.fatal(message, p0, p1, p2, p3, p4);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(message, p0, p1, p2, p3, p4);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalStringP3() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    log.fatal(message, p0, p1, p2, p3);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(message, p0, p1, p2, p3);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalStringP2() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    log.fatal(message, p0, p1, p2);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(message, p0, p1, p2);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalStringP1() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    log.fatal(message, p0, p1);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(message, p0, p1);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testFatalStringP0() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    log.fatal(message, p0);

    verifyMockContext();
    Mockito.verify(_mockLogger).fatal(message, p0);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoMarkerMessage() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    Message message = Mockito.mock(Message.class);

    log.info(marker, message);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(Mockito.same(marker), Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoMarkerMessageThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    Message message = Mockito.mock(Message.class);
    Throwable ex = new Error();

    log.info(marker, message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(Mockito.same(marker), Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoMarkerMessageSupplier() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    MessageSupplier message = Mockito.mock(MessageSupplier.class);

    log.info(marker, message);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(Mockito.same(marker), Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoMarkerMessageSupplierThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    MessageSupplier message = Mockito.mock(MessageSupplier.class);
    Throwable ex = new Error();

    log.info(marker, message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(Mockito.same(marker), Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoMarkerSupplier() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    Supplier message = Mockito.mock(Supplier.class);

    log.info(marker, message);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(Mockito.same(marker), Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoMarkerSupplierThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    Supplier message = Mockito.mock(Supplier.class);
    Throwable ex = new Error();

    log.info(marker, message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(Mockito.same(marker), Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoMarkerCharSequence() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    CharSequence message = Mockito.mock(CharSequence.class);
    log.info(marker, message);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(Mockito.same(marker), Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoMarkerCharSequenceThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    CharSequence message = Mockito.mock(CharSequence.class);
    Throwable ex = new Error();
    log.info(marker, message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(Mockito.same(marker), Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoMarkerObject() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    Object message = new Object();
    log.info(marker, message);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(Mockito.same(marker), Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoMarkerObjectThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    Object message = new Object();
    Throwable ex = new Error();
    log.info(marker, message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(Mockito.same(marker), Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoMarkerString() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    log.info(marker, message);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(Mockito.same(marker), Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoMarkerStringThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Throwable ex = new Error();
    log.info(marker, message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(Mockito.same(marker), Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoMarkerStringSupplierVararg() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Supplier<?> p0 = Mockito.mock(Supplier.class);
    Supplier<?> p1 = Mockito.mock(Supplier.class);
    Supplier<?> p2 = Mockito.mock(Supplier.class);
    Supplier<?> p3 = Mockito.mock(Supplier.class);
    Supplier<?> p4 = Mockito.mock(Supplier.class);
    Supplier<?> p5 = Mockito.mock(Supplier.class);
    Supplier<?> p6 = Mockito.mock(Supplier.class);
    Supplier<?> p7 = Mockito.mock(Supplier.class);
    Supplier<?> p8 = Mockito.mock(Supplier.class);
    Supplier<?> p9 = Mockito.mock(Supplier.class);
    Supplier<?> pa = Mockito.mock(Supplier.class);
    log.info(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoMarkerStringVararg() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    Object p8 = new Object();
    Object p9 = new Object();
    Object pa = new Object();
    log.info(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoMarkerStringP9() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    Object p8 = new Object();
    Object p9 = new Object();
    log.info(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoMarkerStringP8() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    Object p8 = new Object();
    log.info(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoMarkerStringP7() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    log.info(marker, message, p0, p1, p2, p3, p4, p5, p6, p7);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(marker, message, p0, p1, p2, p3, p4, p5, p6, p7);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoMarkerStringP6() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    log.info(marker, message, p0, p1, p2, p3, p4, p5, p6);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(marker, message, p0, p1, p2, p3, p4, p5, p6);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoMarkerStringP5() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    log.info(marker, message, p0, p1, p2, p3, p4, p5);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(marker, message, p0, p1, p2, p3, p4, p5);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoMarkerStringP4() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    log.info(marker, message, p0, p1, p2, p3, p4);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(marker, message, p0, p1, p2, p3, p4);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoMarkerStringP3() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    log.info(marker, message, p0, p1, p2, p3);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(marker, message, p0, p1, p2, p3);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoMarkerStringP2() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    log.info(marker, message, p0, p1, p2);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(marker, message, p0, p1, p2);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoMarkerStringP1() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    log.info(marker, message, p0, p1);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(marker, message, p0, p1);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testMarkerInfoStringP0() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    log.info(marker, message, p0);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(marker, message, p0);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoMessage() {
    Logger log = LogManager.getLogger(getClass());

    Message message = Mockito.mock(Message.class);

    log.info(message);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoMessageThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Message message = Mockito.mock(Message.class);
    Throwable ex = new Error();

    log.info(message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoMessageSupplier() {
    Logger log = LogManager.getLogger(getClass());

    MessageSupplier message = Mockito.mock(MessageSupplier.class);

    log.info(message);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoMessageSupplierThrowable() {
    Logger log = LogManager.getLogger(getClass());

    MessageSupplier message = Mockito.mock(MessageSupplier.class);
    Throwable ex = new Error();

    log.info(message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoSupplier() {
    Logger log = LogManager.getLogger(getClass());

    Supplier<?> message = Mockito.mock(Supplier.class);

    log.info(message);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoSupplierThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Supplier message = Mockito.mock(Supplier.class);
    Throwable ex = new Error();

    log.info(message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoCharSequence() {
    Logger log = LogManager.getLogger(getClass());

    CharSequence message = Mockito.mock(CharSequence.class);
    log.info(message);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoCharSequenceThrowable() {
    Logger log = LogManager.getLogger(getClass());

    CharSequence message = Mockito.mock(CharSequence.class);
    Throwable ex = new Error();
    log.info(message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoObject() {
    Logger log = LogManager.getLogger(getClass());

    Object message = new Object();
    log.info(message);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoObjectThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Object message = new Object();
    Throwable ex = new Error();
    log.info(message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoString() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    log.info(message);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(Mockito.same(message));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoStringThrowable() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Throwable ex = new Error();
    log.info(message, ex);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(Mockito.same(message), Mockito.same(ex));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoStringSupplierVararg() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Supplier<?> p0 = Mockito.mock(Supplier.class);
    Supplier<?> p1 = Mockito.mock(Supplier.class);
    Supplier<?> p2 = Mockito.mock(Supplier.class);
    Supplier<?> p3 = Mockito.mock(Supplier.class);
    Supplier<?> p4 = Mockito.mock(Supplier.class);
    Supplier<?> p5 = Mockito.mock(Supplier.class);
    Supplier<?> p6 = Mockito.mock(Supplier.class);
    Supplier<?> p7 = Mockito.mock(Supplier.class);
    Supplier<?> p8 = Mockito.mock(Supplier.class);
    Supplier<?> p9 = Mockito.mock(Supplier.class);
    Supplier<?> pa = Mockito.mock(Supplier.class);
    log.info(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoStringVararg() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    Object p8 = new Object();
    Object p9 = new Object();
    Object pa = new Object();
    log.info(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoStringP9() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    Object p8 = new Object();
    Object p9 = new Object();
    log.info(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoStringP8() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    Object p8 = new Object();
    log.info(message, p0, p1, p2, p3, p4, p5, p6, p7, p8);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoStringP7() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    log.info(message, p0, p1, p2, p3, p4, p5, p6, p7);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(message, p0, p1, p2, p3, p4, p5, p6, p7);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoStringP6() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    log.info(message, p0, p1, p2, p3, p4, p5, p6);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(message, p0, p1, p2, p3, p4, p5, p6);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoStringP5() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    log.info(message, p0, p1, p2, p3, p4, p5);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(message, p0, p1, p2, p3, p4, p5);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoStringP4() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    log.info(message, p0, p1, p2, p3, p4);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(message, p0, p1, p2, p3, p4);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoStringP3() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    log.info(message, p0, p1, p2, p3);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(message, p0, p1, p2, p3);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoStringP2() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    log.info(message, p0, p1, p2);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(message, p0, p1, p2);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoStringP1() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    log.info(message, p0, p1);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(message, p0, p1);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testInfoStringP0() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    log.info(message, p0);

    verifyMockContext();
    Mockito.verify(_mockLogger).info(message, p0);
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testDebugMarkerMessage() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    Message message = Mockito.mock(Message.class);

    log.debug(marker, message);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);

      log.debug(marker, message);

      Mockito.verify(_mockLogger).debug(Mockito.same(marker), Mockito.same(message));
      Mockito.verifyNoMoreInteractions(_mockLogger);

    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugMarkerMessageThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    Message message = Mockito.mock(Message.class);
    Throwable ex = new Error();

    log.debug(marker, message, ex);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(marker, message, ex);

      Mockito.verify(_mockLogger).debug(Mockito.same(marker), Mockito.same(message), Mockito.same(ex));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugMarkerMessageSupplier() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    MessageSupplier message = Mockito.mock(MessageSupplier.class);

    log.debug(marker, message);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(marker, message);

      Mockito.verify(_mockLogger).debug(Mockito.same(marker), Mockito.same(message));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugMarkerMessageSupplierThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    MessageSupplier message = Mockito.mock(MessageSupplier.class);
    Throwable ex = new Error();

    log.debug(marker, message, ex);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(marker, message, ex);

      Mockito.verify(_mockLogger).debug(Mockito.same(marker), Mockito.same(message), Mockito.same(ex));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugMarkerSupplier() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    Supplier message = Mockito.mock(Supplier.class);

    log.debug(marker, message);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(marker, message);

      Mockito.verify(_mockLogger).debug(Mockito.same(marker), Mockito.same(message));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugMarkerSupplierThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    Supplier message = Mockito.mock(Supplier.class);
    Throwable ex = new Error();

    log.debug(marker, message, ex);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(marker, message, ex);

      Mockito.verify(_mockLogger).debug(Mockito.same(marker), Mockito.same(message), Mockito.same(ex));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugMarkerCharSequence() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    CharSequence message = Mockito.mock(CharSequence.class);
    log.debug(marker, message);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(marker, message);

      Mockito.verify(_mockLogger).debug(Mockito.same(marker), Mockito.same(message));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugMarkerCharSequenceThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    CharSequence message = Mockito.mock(CharSequence.class);
    Throwable ex = new Error();
    log.debug(marker, message, ex);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(marker, message, ex);

      Mockito.verify(_mockLogger).debug(Mockito.same(marker), Mockito.same(message), Mockito.same(ex));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugMarkerObject() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    Object message = new Object();
    log.debug(marker, message);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(marker, message);

      Mockito.verify(_mockLogger).debug(Mockito.same(marker), Mockito.same(message));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugMarkerObjectThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    Object message = new Object();
    Throwable ex = new Error();
    log.debug(marker, message, ex);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(marker, message, ex);

      Mockito.verify(_mockLogger).debug(Mockito.same(marker), Mockito.same(message), Mockito.same(ex));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugMarkerString() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    log.debug(marker, message);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(marker, message);

      Mockito.verify(_mockLogger).debug(Mockito.same(marker), Mockito.same(message));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugMarkerStringThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Throwable ex = new Error();
    log.debug(marker, message, ex);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(marker, message, ex);

      Mockito.verify(_mockLogger).debug(Mockito.same(marker), Mockito.same(message), Mockito.same(ex));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugMarkerStringSupplierVararg() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Supplier<?> p0 = Mockito.mock(Supplier.class);
    Supplier<?> p1 = Mockito.mock(Supplier.class);
    Supplier<?> p2 = Mockito.mock(Supplier.class);
    Supplier<?> p3 = Mockito.mock(Supplier.class);
    Supplier<?> p4 = Mockito.mock(Supplier.class);
    Supplier<?> p5 = Mockito.mock(Supplier.class);
    Supplier<?> p6 = Mockito.mock(Supplier.class);
    Supplier<?> p7 = Mockito.mock(Supplier.class);
    Supplier<?> p8 = Mockito.mock(Supplier.class);
    Supplier<?> p9 = Mockito.mock(Supplier.class);
    Supplier<?> pa = Mockito.mock(Supplier.class);
    log.debug(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);

      Mockito.verify(_mockLogger).debug(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugMarkerStringVararg() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    Object p8 = new Object();
    Object p9 = new Object();
    Object pa = new Object();
    log.debug(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);

      Mockito.verify(_mockLogger).debug(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugMarkerStringP9() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    Object p8 = new Object();
    Object p9 = new Object();
    log.debug(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);

      Mockito.verify(_mockLogger).debug(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugMarkerStringP8() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    Object p8 = new Object();
    log.debug(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);

      Mockito.verify(_mockLogger).debug(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugMarkerStringP7() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    log.debug(marker, message, p0, p1, p2, p3, p4, p5, p6, p7);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(marker, message, p0, p1, p2, p3, p4, p5, p6, p7);

      Mockito.verify(_mockLogger).debug(marker, message, p0, p1, p2, p3, p4, p5, p6, p7);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugMarkerStringP6() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    log.debug(marker, message, p0, p1, p2, p3, p4, p5, p6);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(marker, message, p0, p1, p2, p3, p4, p5, p6);

      Mockito.verify(_mockLogger).debug(marker, message, p0, p1, p2, p3, p4, p5, p6);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugMarkerStringP5() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    log.debug(marker, message, p0, p1, p2, p3, p4, p5);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(marker, message, p0, p1, p2, p3, p4, p5);

      Mockito.verify(_mockLogger).debug(marker, message, p0, p1, p2, p3, p4, p5);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugMarkerStringP4() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    log.debug(marker, message, p0, p1, p2, p3, p4);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(marker, message, p0, p1, p2, p3, p4);

      Mockito.verify(_mockLogger).debug(marker, message, p0, p1, p2, p3, p4);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugMarkerStringP3() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    log.debug(marker, message, p0, p1, p2, p3);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(marker, message, p0, p1, p2, p3);

      Mockito.verify(_mockLogger).debug(marker, message, p0, p1, p2, p3);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugMarkerStringP2() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    log.debug(marker, message, p0, p1, p2);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(marker, message, p0, p1, p2);

      Mockito.verify(_mockLogger).debug(marker, message, p0, p1, p2);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugMarkerStringP1() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    log.debug(marker, message, p0, p1);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(marker, message, p0, p1);

      Mockito.verify(_mockLogger).debug(marker, message, p0, p1);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testMarkerDebugStringP0() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    log.debug(marker, message, p0);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(marker, message, p0);

      Mockito.verify(_mockLogger).debug(marker, message, p0);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugMessage() {
    Logger log = LogManager.getLogger(getClass());

    Message message = Mockito.mock(Message.class);

    log.debug(message);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(message);

      Mockito.verify(_mockLogger).debug(Mockito.same(message));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugMessageThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Message message = Mockito.mock(Message.class);
    Throwable ex = new Error();

    log.debug(message, ex);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(message, ex);

      Mockito.verify(_mockLogger).debug(Mockito.same(message), Mockito.same(ex));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugMessageSupplier() {
    Logger log = LogManager.getLogger(getClass());

    MessageSupplier message = Mockito.mock(MessageSupplier.class);

    log.debug(message);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(message);

      Mockito.verify(_mockLogger).debug(Mockito.same(message));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugMessageSupplierThrowable() {
    Logger log = LogManager.getLogger(getClass());

    MessageSupplier message = Mockito.mock(MessageSupplier.class);
    Throwable ex = new Error();

    log.debug(message, ex);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(message, ex);

      Mockito.verify(_mockLogger).debug(Mockito.same(message), Mockito.same(ex));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugSupplier() {
    Logger log = LogManager.getLogger(getClass());

    Supplier<?> message = Mockito.mock(Supplier.class);

    log.debug(message);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(message);

      Mockito.verify(_mockLogger).debug(Mockito.same(message));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugSupplierThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Supplier message = Mockito.mock(Supplier.class);
    Throwable ex = new Error();

    log.debug(message, ex);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(message, ex);

      Mockito.verify(_mockLogger).debug(Mockito.same(message), Mockito.same(ex));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugCharSequence() {
    Logger log = LogManager.getLogger(getClass());

    CharSequence message = Mockito.mock(CharSequence.class);
    log.debug(message);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(message);

      Mockito.verify(_mockLogger).debug(Mockito.same(message));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugCharSequenceThrowable() {
    Logger log = LogManager.getLogger(getClass());

    CharSequence message = Mockito.mock(CharSequence.class);
    Throwable ex = new Error();
    log.debug(message, ex);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(message, ex);

      Mockito.verify(_mockLogger).debug(Mockito.same(message), Mockito.same(ex));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugObject() {
    Logger log = LogManager.getLogger(getClass());

    Object message = new Object();
    log.debug(message);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(message);

      Mockito.verify(_mockLogger).debug(Mockito.same(message));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugObjectThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Object message = new Object();
    Throwable ex = new Error();
    log.debug(message, ex);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(message, ex);

      Mockito.verify(_mockLogger).debug(Mockito.same(message), Mockito.same(ex));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugString() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    log.debug(message);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(message);

      Mockito.verify(_mockLogger).debug(Mockito.same(message));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugStringThrowable() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Throwable ex = new Error();
    log.debug(message, ex);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(message, ex);

      Mockito.verify(_mockLogger).debug(Mockito.same(message), Mockito.same(ex));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugStringSupplierVararg() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Supplier<?> p0 = Mockito.mock(Supplier.class);
    Supplier<?> p1 = Mockito.mock(Supplier.class);
    Supplier<?> p2 = Mockito.mock(Supplier.class);
    Supplier<?> p3 = Mockito.mock(Supplier.class);
    Supplier<?> p4 = Mockito.mock(Supplier.class);
    Supplier<?> p5 = Mockito.mock(Supplier.class);
    Supplier<?> p6 = Mockito.mock(Supplier.class);
    Supplier<?> p7 = Mockito.mock(Supplier.class);
    Supplier<?> p8 = Mockito.mock(Supplier.class);
    Supplier<?> p9 = Mockito.mock(Supplier.class);
    Supplier<?> pa = Mockito.mock(Supplier.class);
    log.debug(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);

      Mockito.verify(_mockLogger).debug(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugStringVararg() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    Object p8 = new Object();
    Object p9 = new Object();
    Object pa = new Object();
    log.debug(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);

      Mockito.verify(_mockLogger).debug(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugStringP9() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    Object p8 = new Object();
    Object p9 = new Object();
    log.debug(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);

      Mockito.verify(_mockLogger).debug(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugStringP8() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    Object p8 = new Object();
    log.debug(message, p0, p1, p2, p3, p4, p5, p6, p7, p8);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(message, p0, p1, p2, p3, p4, p5, p6, p7, p8);

      Mockito.verify(_mockLogger).debug(message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugStringP7() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    log.debug(message, p0, p1, p2, p3, p4, p5, p6, p7);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(message, p0, p1, p2, p3, p4, p5, p6, p7);

      Mockito.verify(_mockLogger).debug(message, p0, p1, p2, p3, p4, p5, p6, p7);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugStringP6() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    log.debug(message, p0, p1, p2, p3, p4, p5, p6);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(message, p0, p1, p2, p3, p4, p5, p6);

      Mockito.verify(_mockLogger).debug(message, p0, p1, p2, p3, p4, p5, p6);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugStringP5() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    log.debug(message, p0, p1, p2, p3, p4, p5);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(message, p0, p1, p2, p3, p4, p5);

      Mockito.verify(_mockLogger).debug(message, p0, p1, p2, p3, p4, p5);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugStringP4() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    log.debug(message, p0, p1, p2, p3, p4);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(message, p0, p1, p2, p3, p4);

      Mockito.verify(_mockLogger).debug(message, p0, p1, p2, p3, p4);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugStringP3() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    log.debug(message, p0, p1, p2, p3);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(message, p0, p1, p2, p3);

      Mockito.verify(_mockLogger).debug(message, p0, p1, p2, p3);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugStringP2() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    log.debug(message, p0, p1, p2);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(message, p0, p1, p2);

      Mockito.verify(_mockLogger).debug(message, p0, p1, p2);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugStringP1() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    log.debug(message, p0, p1);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(message, p0, p1);

      Mockito.verify(_mockLogger).debug(message, p0, p1);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testDebugStringP0() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    log.debug(message, p0);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);
      log.debug(message, p0);

      Mockito.verify(_mockLogger).debug(message, p0);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testTraceMarkerMessage() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    Message message = Mockito.mock(Message.class);

    log.trace(marker, message);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);

      log.trace(marker, message);

      Mockito.verify(_mockLogger).trace(Mockito.same(marker), Mockito.same(message));
      Mockito.verifyNoMoreInteractions(_mockLogger);

    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceMarkerMessageThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    Message message = Mockito.mock(Message.class);
    Throwable ex = new Error();

    log.trace(marker, message, ex);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(marker, message, ex);

      Mockito.verify(_mockLogger).trace(Mockito.same(marker), Mockito.same(message), Mockito.same(ex));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceMarkerMessageSupplier() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    MessageSupplier message = Mockito.mock(MessageSupplier.class);

    log.trace(marker, message);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(marker, message);

      Mockito.verify(_mockLogger).trace(Mockito.same(marker), Mockito.same(message));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceMarkerMessageSupplierThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    MessageSupplier message = Mockito.mock(MessageSupplier.class);
    Throwable ex = new Error();

    log.trace(marker, message, ex);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(marker, message, ex);

      Mockito.verify(_mockLogger).trace(Mockito.same(marker), Mockito.same(message), Mockito.same(ex));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceMarkerSupplier() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    Supplier message = Mockito.mock(Supplier.class);

    log.trace(marker, message);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(marker, message);

      Mockito.verify(_mockLogger).trace(Mockito.same(marker), Mockito.same(message));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceMarkerSupplierThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    Supplier message = Mockito.mock(Supplier.class);
    Throwable ex = new Error();

    log.trace(marker, message, ex);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(marker, message, ex);

      Mockito.verify(_mockLogger).trace(Mockito.same(marker), Mockito.same(message), Mockito.same(ex));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceMarkerCharSequence() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    CharSequence message = Mockito.mock(CharSequence.class);
    log.trace(marker, message);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(marker, message);

      Mockito.verify(_mockLogger).trace(Mockito.same(marker), Mockito.same(message));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceMarkerCharSequenceThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    CharSequence message = Mockito.mock(CharSequence.class);
    Throwable ex = new Error();
    log.trace(marker, message, ex);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(marker, message, ex);

      Mockito.verify(_mockLogger).trace(Mockito.same(marker), Mockito.same(message), Mockito.same(ex));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceMarkerObject() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    Object message = new Object();
    log.trace(marker, message);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(marker, message);

      Mockito.verify(_mockLogger).trace(Mockito.same(marker), Mockito.same(message));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceMarkerObjectThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    Object message = new Object();
    Throwable ex = new Error();
    log.trace(marker, message, ex);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(marker, message, ex);

      Mockito.verify(_mockLogger).trace(Mockito.same(marker), Mockito.same(message), Mockito.same(ex));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceMarkerString() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    log.trace(marker, message);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(marker, message);

      Mockito.verify(_mockLogger).trace(Mockito.same(marker), Mockito.same(message));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceMarkerStringThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Throwable ex = new Error();
    log.trace(marker, message, ex);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(marker, message, ex);

      Mockito.verify(_mockLogger).trace(Mockito.same(marker), Mockito.same(message), Mockito.same(ex));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceMarkerStringSupplierVararg() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Supplier<?> p0 = Mockito.mock(Supplier.class);
    Supplier<?> p1 = Mockito.mock(Supplier.class);
    Supplier<?> p2 = Mockito.mock(Supplier.class);
    Supplier<?> p3 = Mockito.mock(Supplier.class);
    Supplier<?> p4 = Mockito.mock(Supplier.class);
    Supplier<?> p5 = Mockito.mock(Supplier.class);
    Supplier<?> p6 = Mockito.mock(Supplier.class);
    Supplier<?> p7 = Mockito.mock(Supplier.class);
    Supplier<?> p8 = Mockito.mock(Supplier.class);
    Supplier<?> p9 = Mockito.mock(Supplier.class);
    Supplier<?> pa = Mockito.mock(Supplier.class);
    log.trace(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);

      Mockito.verify(_mockLogger).trace(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceMarkerStringVararg() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    Object p8 = new Object();
    Object p9 = new Object();
    Object pa = new Object();
    log.trace(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);

      Mockito.verify(_mockLogger).trace(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceMarkerStringP9() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    Object p8 = new Object();
    Object p9 = new Object();
    log.trace(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);

      Mockito.verify(_mockLogger).trace(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceMarkerStringP8() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    Object p8 = new Object();
    log.trace(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);

      Mockito.verify(_mockLogger).trace(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceMarkerStringP7() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    log.trace(marker, message, p0, p1, p2, p3, p4, p5, p6, p7);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(marker, message, p0, p1, p2, p3, p4, p5, p6, p7);

      Mockito.verify(_mockLogger).trace(marker, message, p0, p1, p2, p3, p4, p5, p6, p7);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceMarkerStringP6() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    log.trace(marker, message, p0, p1, p2, p3, p4, p5, p6);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(marker, message, p0, p1, p2, p3, p4, p5, p6);

      Mockito.verify(_mockLogger).trace(marker, message, p0, p1, p2, p3, p4, p5, p6);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceMarkerStringP5() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    log.trace(marker, message, p0, p1, p2, p3, p4, p5);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(marker, message, p0, p1, p2, p3, p4, p5);

      Mockito.verify(_mockLogger).trace(marker, message, p0, p1, p2, p3, p4, p5);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceMarkerStringP4() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    log.trace(marker, message, p0, p1, p2, p3, p4);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(marker, message, p0, p1, p2, p3, p4);

      Mockito.verify(_mockLogger).trace(marker, message, p0, p1, p2, p3, p4);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceMarkerStringP3() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    log.trace(marker, message, p0, p1, p2, p3);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(marker, message, p0, p1, p2, p3);

      Mockito.verify(_mockLogger).trace(marker, message, p0, p1, p2, p3);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceMarkerStringP2() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    log.trace(marker, message, p0, p1, p2);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(marker, message, p0, p1, p2);

      Mockito.verify(_mockLogger).trace(marker, message, p0, p1, p2);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceMarkerStringP1() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    log.trace(marker, message, p0, p1);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(marker, message, p0, p1);

      Mockito.verify(_mockLogger).trace(marker, message, p0, p1);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testMarkerTraceStringP0() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    String message = "Hello World";
    Object p0 = new Object();
    log.trace(marker, message, p0);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(marker, message, p0);

      Mockito.verify(_mockLogger).trace(marker, message, p0);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceMessage() {
    Logger log = LogManager.getLogger(getClass());

    Message message = Mockito.mock(Message.class);

    log.trace(message);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(message);

      Mockito.verify(_mockLogger).trace(Mockito.same(message));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceMessageThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Message message = Mockito.mock(Message.class);
    Throwable ex = new Error();

    log.trace(message, ex);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(message, ex);

      Mockito.verify(_mockLogger).trace(Mockito.same(message), Mockito.same(ex));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceMessageSupplier() {
    Logger log = LogManager.getLogger(getClass());

    MessageSupplier message = Mockito.mock(MessageSupplier.class);

    log.trace(message);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(message);

      Mockito.verify(_mockLogger).trace(Mockito.same(message));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceMessageSupplierThrowable() {
    Logger log = LogManager.getLogger(getClass());

    MessageSupplier message = Mockito.mock(MessageSupplier.class);
    Throwable ex = new Error();

    log.trace(message, ex);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(message, ex);

      Mockito.verify(_mockLogger).trace(Mockito.same(message), Mockito.same(ex));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceSupplier() {
    Logger log = LogManager.getLogger(getClass());

    Supplier<?> message = Mockito.mock(Supplier.class);

    log.trace(message);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(message);

      Mockito.verify(_mockLogger).trace(Mockito.same(message));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceSupplierThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Supplier message = Mockito.mock(Supplier.class);
    Throwable ex = new Error();

    log.trace(message, ex);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(message, ex);

      Mockito.verify(_mockLogger).trace(Mockito.same(message), Mockito.same(ex));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceCharSequence() {
    Logger log = LogManager.getLogger(getClass());

    CharSequence message = Mockito.mock(CharSequence.class);
    log.trace(message);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(message);

      Mockito.verify(_mockLogger).trace(Mockito.same(message));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceCharSequenceThrowable() {
    Logger log = LogManager.getLogger(getClass());

    CharSequence message = Mockito.mock(CharSequence.class);
    Throwable ex = new Error();
    log.trace(message, ex);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(message, ex);

      Mockito.verify(_mockLogger).trace(Mockito.same(message), Mockito.same(ex));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceObject() {
    Logger log = LogManager.getLogger(getClass());

    Object message = new Object();
    log.trace(message);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(message);

      Mockito.verify(_mockLogger).trace(Mockito.same(message));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceObjectThrowable() {
    Logger log = LogManager.getLogger(getClass());

    Object message = new Object();
    Throwable ex = new Error();
    log.trace(message, ex);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(message, ex);

      Mockito.verify(_mockLogger).trace(Mockito.same(message), Mockito.same(ex));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceString() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    log.trace(message);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(message);

      Mockito.verify(_mockLogger).trace(Mockito.same(message));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceStringThrowable() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Throwable ex = new Error();
    log.trace(message, ex);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(message, ex);

      Mockito.verify(_mockLogger).trace(Mockito.same(message), Mockito.same(ex));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceStringSupplierVararg() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Supplier<?> p0 = Mockito.mock(Supplier.class);
    Supplier<?> p1 = Mockito.mock(Supplier.class);
    Supplier<?> p2 = Mockito.mock(Supplier.class);
    Supplier<?> p3 = Mockito.mock(Supplier.class);
    Supplier<?> p4 = Mockito.mock(Supplier.class);
    Supplier<?> p5 = Mockito.mock(Supplier.class);
    Supplier<?> p6 = Mockito.mock(Supplier.class);
    Supplier<?> p7 = Mockito.mock(Supplier.class);
    Supplier<?> p8 = Mockito.mock(Supplier.class);
    Supplier<?> p9 = Mockito.mock(Supplier.class);
    Supplier<?> pa = Mockito.mock(Supplier.class);
    log.trace(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);

      Mockito.verify(_mockLogger).trace(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceStringVararg() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    Object p8 = new Object();
    Object p9 = new Object();
    Object pa = new Object();
    log.trace(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);

      Mockito.verify(_mockLogger).trace(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, pa);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceStringP9() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    Object p8 = new Object();
    Object p9 = new Object();
    log.trace(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);

      Mockito.verify(_mockLogger).trace(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceStringP8() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    Object p8 = new Object();
    log.trace(message, p0, p1, p2, p3, p4, p5, p6, p7, p8);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(message, p0, p1, p2, p3, p4, p5, p6, p7, p8);

      Mockito.verify(_mockLogger).trace(message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceStringP7() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    Object p7 = new Object();
    log.trace(message, p0, p1, p2, p3, p4, p5, p6, p7);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(message, p0, p1, p2, p3, p4, p5, p6, p7);

      Mockito.verify(_mockLogger).trace(message, p0, p1, p2, p3, p4, p5, p6, p7);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceStringP6() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    Object p6 = new Object();
    log.trace(message, p0, p1, p2, p3, p4, p5, p6);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(message, p0, p1, p2, p3, p4, p5, p6);

      Mockito.verify(_mockLogger).trace(message, p0, p1, p2, p3, p4, p5, p6);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceStringP5() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    Object p5 = new Object();
    log.trace(message, p0, p1, p2, p3, p4, p5);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(message, p0, p1, p2, p3, p4, p5);

      Mockito.verify(_mockLogger).trace(message, p0, p1, p2, p3, p4, p5);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceStringP4() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    Object p4 = new Object();
    log.trace(message, p0, p1, p2, p3, p4);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(message, p0, p1, p2, p3, p4);

      Mockito.verify(_mockLogger).trace(message, p0, p1, p2, p3, p4);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceStringP3() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    Object p3 = new Object();
    log.trace(message, p0, p1, p2, p3);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(message, p0, p1, p2, p3);

      Mockito.verify(_mockLogger).trace(message, p0, p1, p2, p3);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceStringP2() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    Object p2 = new Object();
    log.trace(message, p0, p1, p2);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(message, p0, p1, p2);

      Mockito.verify(_mockLogger).trace(message, p0, p1, p2);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceStringP1() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    Object p1 = new Object();
    log.trace(message, p0, p1);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(message, p0, p1);

      Mockito.verify(_mockLogger).trace(message, p0, p1);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceStringP0() {
    Logger log = LogManager.getLogger(getClass());

    String message = "Hello World";
    Object p0 = new Object();
    log.trace(message, p0);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.trace(message, p0);

      Mockito.verify(_mockLogger).trace(message, p0);
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceCatching() {
    Logger log = LogManager.getLogger(getClass());

    Throwable ex = new Error();
    log.catching(ex);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.catching(ex);

      Mockito.verify(_mockLogger).catching(Mockito.same(ex));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceEntry() {
    Logger log = LogManager.getLogger(getClass());

    log.entry();

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.entry();

      Mockito.verify(_mockLogger).entry();
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceEntryVarargs() {
    Logger log = LogManager.getLogger(getClass());

    Object o = new Object();
    log.entry(o);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.entry(o);

      Mockito.verify(_mockLogger).entry(Mockito.same(o));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceExit() {
    Logger log = LogManager.getLogger(getClass());

    log.exit();
    log.traceExit();

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      log.exit();
      log.traceExit();

      Mockito.verify(_mockLogger).exit();
      Mockito.verify(_mockLogger).traceExit();
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceExitArg() {
    Logger log = LogManager.getLogger(getClass());

    Mockito.when(_mockLogger.exit(Mockito.any())).thenAnswer(invocation -> invocation.getArgument(0));
    Mockito.when(_mockLogger.traceExit(Mockito.any(Object.class))).thenAnswer(invocation -> invocation.getArgument(0));

    Object o = new Object();
    Assert.assertSame(log.exit(o), o);
    Assert.assertSame(log.traceExit(o), o);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      Assert.assertSame(log.exit(o), o);
      Assert.assertSame(log.traceExit(o), o);

      Mockito.verify(_mockLogger).exit(Mockito.same(o));
      Mockito.verify(_mockLogger).traceExit(Mockito.same(o));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceEntryExit() {
    Logger log = LogManager.getLogger(getClass());

    EntryMessage msg1 = log.traceEntry();
    Assert.assertNotNull(msg1);
    log.traceExit(msg1);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);

      log.traceExit(msg1);
      Mockito.verifyNoMoreInteractions(_mockLogger);

      Mockito.doReturn(Mockito.mock(EntryMessage.class)).when(_mockLogger).traceEntry();

      EntryMessage msg3 = log.traceEntry();
      Assert.assertNotNull(msg1);
      log.traceExit(msg3);

      Mockito.verify(_mockLogger).traceEntry();
      Mockito.verify(_mockLogger).traceExit(Mockito.same(msg3));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceEntryExitArg() {
    Logger log = LogManager.getLogger(getClass());

    Mockito.when(_mockLogger.traceExit(Mockito.any(EntryMessage.class), Mockito.any(Object.class)))
        .thenAnswer(invocation -> invocation.getArgument(1));
    Mockito.when(_mockLogger.traceExit(Mockito.any(String.class), Mockito.any(Object.class)))
        .thenAnswer(invocation -> invocation.getArgument(1));
    Mockito.when(_mockLogger.traceExit(Mockito.any(Message.class), Mockito.any(Object.class)))
        .thenAnswer(invocation -> invocation.getArgument(1));

    Object o = new Object();
    Message msg0 = Mockito.mock(Message.class);
    EntryMessage e0 = log.traceEntry(msg0);
    Assert.assertNotNull(e0);
    Assert.assertSame(log.traceExit(e0, o), o);
    Assert.assertSame(log.traceExit("foo", o), o);
    Assert.assertSame(log.traceExit(Mockito.mock(Message.class), o), o);

    String msg2 = "Hello world";
    Assert.assertSame(log.traceEntry(msg2), e0);
    Assert.assertSame(log.traceEntry(msg2, Mockito.mock(Supplier.class)), e0);
    Supplier<?> msg1 = Mockito.mock(Supplier.class);
    Assert.assertSame(log.traceEntry(msg1), e0);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);

      Assert.assertSame(log.traceExit(e0, o), o);
      Mockito.verifyNoMoreInteractions(_mockLogger);

      EntryMessage r1 = Mockito.mock(EntryMessage.class);
      EntryMessage r2 = Mockito.mock(EntryMessage.class);
      EntryMessage r3 = Mockito.mock(EntryMessage.class);
      EntryMessage r4 = Mockito.mock(EntryMessage.class);

      Mockito.doReturn(r1).when(_mockLogger).traceEntry(Mockito.any(Message.class));
      Mockito.doReturn(r2).when(_mockLogger).traceEntry(Mockito.anyString(), Mockito.anyString());
      Mockito.doReturn(r3).when(_mockLogger).traceEntry(Mockito.anyString(), Mockito.any(Supplier.class));
      Mockito.doReturn(r4).when(_mockLogger).traceEntry(Mockito.any(Supplier.class));

      Assert.assertSame(log.traceEntry(msg0), r1);
      String obj = "arg";
      Assert.assertSame(log.traceEntry(msg2, obj), r2);
      Supplier<?> supplier = Mockito.mock(Supplier.class);
      Assert.assertSame(log.traceEntry(msg2, supplier), r3);
      Assert.assertSame(log.traceEntry(msg1), r4);

      Assert.assertSame(log.traceExit(r1, o), o);
      Assert.assertSame(log.traceExit(msg2, o), o);

      Message msg4 = Mockito.mock(Message.class);
      Assert.assertSame(log.traceExit(msg4, o), o);

      Mockito.verify(_mockLogger).traceEntry(Mockito.same(msg0));
      Mockito.verify(_mockLogger).traceEntry(Mockito.same(msg1));
      Mockito.verify(_mockLogger).traceEntry(Mockito.same(msg2), Mockito.same(obj));
      Mockito.verify(_mockLogger).traceEntry(Mockito.same(msg2), Mockito.same(supplier));
      Mockito.verify(_mockLogger).traceExit(Mockito.same(r1), Mockito.same(o));
      Mockito.verify(_mockLogger).traceExit(Mockito.same(msg2), Mockito.same(o));
      Mockito.verify(_mockLogger).traceExit(Mockito.same(msg4), Mockito.same(o));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testTraceThrowing() {
    Logger log = LogManager.getLogger(getClass());

    Mockito.doAnswer(invocation -> invocation.getArgument(0)).when(_mockLogger).throwing(Mockito.any(Throwable.class));

    Throwable ex = new Error();
    Assert.assertSame(log.throwing(ex), ex);

    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);
      Assert.assertSame(log.throwing(ex), ex);

      Mockito.verify(_mockLogger).throwing(Mockito.same(ex));
      Mockito.verifyNoMoreInteractions(_mockLogger);
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testGetLevel() {
    Logger log = LogManager.getLogger(getClass());
    Level level = Level.forName("TEST", 42);

    Mockito.doReturn(level).when(_mockLogger).getLevel();

    Assert.assertSame(log.getLevel(), level);
    verifyMockContext();
    Mockito.verify(_mockLogger).getLevel();
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testGetMessageFactory() {
    Logger log = LogManager.getLogger(getClass());
    MessageFactory factory = Mockito.mock(MessageFactory.class);

    Mockito.doReturn(factory).when(_mockLogger).getMessageFactory();

    Assert.assertSame(log.getMessageFactory(), factory);
    verifyMockContext();
    Mockito.verify(_mockLogger).getMessageFactory();
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testGetName() {
    Logger log = LogManager.getLogger(getClass());
    String name = "Hwllo world!";

    Mockito.doReturn(name).when(_mockLogger).getName();

    Assert.assertSame(log.getName(), name);
    verifyMockContext();
    Mockito.verify(_mockLogger).getName();
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testIsDebugEnabled() {
    Logger log = LogManager.getLogger(getClass());

    Assert.assertFalse(log.isDebugEnabled());
    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);

      Assert.assertFalse(log.isDebugEnabled());
      Mockito.verify(_mockLogger).isDebugEnabled();

      Mockito.reset(_mockLogger);
      Mockito.doReturn(true).when(_mockLogger).isDebugEnabled();

      Assert.assertTrue(log.isDebugEnabled());
      Mockito.verify(_mockLogger).isDebugEnabled();
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testIsDebugEnabledMarker() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    Assert.assertFalse(log.isDebugEnabled(marker));
    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setDebug(true);

      Assert.assertFalse(log.isDebugEnabled(marker));
      Mockito.verify(_mockLogger).isDebugEnabled(Mockito.same(marker));

      Mockito.reset(_mockLogger);
      Mockito.doReturn(true).when(_mockLogger).isDebugEnabled(Mockito.any(Marker.class));

      Assert.assertTrue(log.isDebugEnabled(marker));
      Mockito.verify(_mockLogger).isDebugEnabled(Mockito.same(marker));
    } finally {
      _mBean.setDebug(false);
    }
  }

  @Test
  public void testIsTraceEnabled() {
    Logger log = LogManager.getLogger(getClass());

    Assert.assertFalse(log.isTraceEnabled());
    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);

      Assert.assertFalse(log.isTraceEnabled());
      Mockito.verify(_mockLogger).isTraceEnabled();

      Mockito.reset(_mockLogger);
      Mockito.doReturn(true).when(_mockLogger).isTraceEnabled();

      Assert.assertTrue(log.isTraceEnabled());
      Mockito.verify(_mockLogger).isTraceEnabled();
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testIsTraceEnabledMarker() {
    Logger log = LogManager.getLogger(getClass());

    Marker marker = Mockito.mock(Marker.class);
    Assert.assertFalse(log.isTraceEnabled(marker));
    verifyMockContext();
    Mockito.verifyNoMoreInteractions(_mockLogger);

    try {
      _mBean.setTrace(true);

      Assert.assertFalse(log.isTraceEnabled(marker));
      Mockito.verify(_mockLogger).isTraceEnabled(Mockito.same(marker));

      Mockito.reset(_mockLogger);
      Mockito.doReturn(true).when(_mockLogger).isTraceEnabled(Mockito.any(Marker.class));

      Assert.assertTrue(log.isTraceEnabled(marker));
      Mockito.verify(_mockLogger).isTraceEnabled(Mockito.same(marker));
    } finally {
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testIsInfoEnabled() {
    Logger log = LogManager.getLogger(getClass());
    Assert.assertFalse(log.isInfoEnabled());
    Mockito.doReturn(true).when(_mockLogger).isInfoEnabled();
    Assert.assertTrue(log.isInfoEnabled());
    verifyMockContext();
    Mockito.verify(_mockLogger, Mockito.times(2)).isInfoEnabled();
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testIsWarnEnabled() {
    Logger log = LogManager.getLogger(getClass());
    Assert.assertFalse(log.isWarnEnabled());
    Mockito.doReturn(true).when(_mockLogger).isWarnEnabled();
    Assert.assertTrue(log.isWarnEnabled());
    verifyMockContext();
    Mockito.verify(_mockLogger, Mockito.times(2)).isWarnEnabled();
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testIsErrorEnabled() {
    Logger log = LogManager.getLogger(getClass());
    Assert.assertFalse(log.isErrorEnabled());
    Mockito.doReturn(true).when(_mockLogger).isErrorEnabled();
    Assert.assertTrue(log.isErrorEnabled());
    verifyMockContext();
    Mockito.verify(_mockLogger, Mockito.times(2)).isErrorEnabled();
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testIsFatalEnabled() {
    Logger log = LogManager.getLogger(getClass());
    Assert.assertFalse(log.isFatalEnabled());
    Mockito.doReturn(true).when(_mockLogger).isFatalEnabled();
    Assert.assertTrue(log.isFatalEnabled());
    verifyMockContext();
    Mockito.verify(_mockLogger, Mockito.times(2)).isFatalEnabled();
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testIsInfoEnabledMarker() {
    Logger log = LogManager.getLogger(getClass());
    Marker marker = Mockito.mock(Marker.class);
    Assert.assertFalse(log.isInfoEnabled(marker));
    Mockito.doReturn(true).when(_mockLogger).isInfoEnabled(Mockito.any(Marker.class));
    Assert.assertTrue(log.isInfoEnabled(marker));
    verifyMockContext();
    Mockito.verify(_mockLogger, Mockito.times(2)).isInfoEnabled(Mockito.same(marker));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testIsWarnEnabledMarker() {
    Logger log = LogManager.getLogger(getClass());
    Marker marker = Mockito.mock(Marker.class);
    Assert.assertFalse(log.isWarnEnabled(marker));
    Mockito.doReturn(true).when(_mockLogger).isWarnEnabled(Mockito.any(Marker.class));
    Assert.assertTrue(log.isWarnEnabled(marker));
    verifyMockContext();
    Mockito.verify(_mockLogger, Mockito.times(2)).isWarnEnabled(Mockito.same(marker));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testIsErrorEnabledMarker() {
    Logger log = LogManager.getLogger(getClass());
    Marker marker = Mockito.mock(Marker.class);
    Assert.assertFalse(log.isErrorEnabled(marker));
    Mockito.doReturn(true).when(_mockLogger).isErrorEnabled(Mockito.any(Marker.class));
    Assert.assertTrue(log.isErrorEnabled(marker));
    verifyMockContext();
    Mockito.verify(_mockLogger, Mockito.times(2)).isErrorEnabled(Mockito.same(marker));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testIsFatalEnabledMarker() {
    Logger log = LogManager.getLogger(getClass());
    Marker marker = Mockito.mock(Marker.class);
    Assert.assertFalse(log.isFatalEnabled(marker));
    Mockito.doReturn(true).when(_mockLogger).isFatalEnabled(Mockito.any(Marker.class));
    Assert.assertTrue(log.isFatalEnabled(marker));
    verifyMockContext();
    Mockito.verify(_mockLogger, Mockito.times(2)).isFatalEnabled(Mockito.same(marker));
    Mockito.verifyNoMoreInteractions(_mockLogger);
  }

  @Test
  public void testIsEnabled() {
    Logger log = LogManager.getLogger(getClass());
    Assert.assertFalse(log.isEnabled(Level.TRACE));
    Assert.assertFalse(log.isEnabled(Level.DEBUG));
    Assert.assertFalse(log.isEnabled(Level.INFO));
    verifyMockContext();
    Mockito.verify(_mockLogger).isEnabled(Mockito.same(Level.INFO));
    Mockito.verifyNoMoreInteractions(_mockLogger);
    Mockito.reset(_mockLogger);

    Mockito.when(_mockLogger.isEnabled(Mockito.eq(Level.INFO))).thenReturn(true);
    Assert.assertTrue(log.isEnabled(Level.INFO));
    Mockito.verify(_mockLogger).isEnabled(Mockito.same(Level.INFO));
    Mockito.verifyNoMoreInteractions(_mockLogger);
    Mockito.reset(_mockLogger);

    try {
      _mBean.setDebug(true);

      Assert.assertFalse(log.isEnabled(Level.TRACE));
      Assert.assertFalse(log.isEnabled(Level.DEBUG));
      Mockito.verify(_mockLogger).isEnabled(Mockito.same(Level.DEBUG));
      Mockito.verifyNoMoreInteractions(_mockLogger);
      Mockito.reset(_mockLogger);

      Mockito.when(_mockLogger.isEnabled(Mockito.eq(Level.DEBUG))).thenReturn(true);
      Assert.assertFalse(log.isEnabled(Level.TRACE));
      Assert.assertTrue(log.isEnabled(Level.DEBUG));
      Mockito.verify(_mockLogger).isEnabled(Mockito.same(Level.DEBUG));
      Mockito.verifyNoMoreInteractions(_mockLogger);
      Mockito.reset(_mockLogger);

      _mBean.setTrace(true);

      Assert.assertFalse(log.isEnabled(Level.TRACE));
      Assert.assertFalse(log.isEnabled(Level.DEBUG));
      Mockito.verify(_mockLogger).isEnabled(Mockito.same(Level.DEBUG));
      Mockito.verify(_mockLogger).isEnabled(Mockito.same(Level.TRACE));
      Mockito.verifyNoMoreInteractions(_mockLogger);
      Mockito.reset(_mockLogger);

      Mockito.when(_mockLogger.isEnabled(Mockito.eq(Level.TRACE))).thenReturn(true);
      Assert.assertTrue(log.isEnabled(Level.TRACE));
      Assert.assertFalse(log.isEnabled(Level.DEBUG));
      Mockito.verify(_mockLogger).isEnabled(Mockito.same(Level.DEBUG));
      Mockito.verify(_mockLogger).isEnabled(Mockito.same(Level.TRACE));
      Mockito.verifyNoMoreInteractions(_mockLogger);
      Mockito.reset(_mockLogger);

    } finally {
      _mBean.setDebug(false);
      _mBean.setTrace(false);
    }
  }

  @Test
  public void testIsEnabledMarker() {
    Logger log = LogManager.getLogger(getClass());
    Marker marker = Mockito.mock(Marker.class);
    Assert.assertFalse(log.isEnabled(Level.TRACE, marker));
    Assert.assertFalse(log.isEnabled(Level.DEBUG, marker));
    Assert.assertFalse(log.isEnabled(Level.INFO, marker));
    verifyMockContext();
    Mockito.verify(_mockLogger).isEnabled(Mockito.same(Level.INFO), Mockito.same(marker));
    Mockito.verifyNoMoreInteractions(_mockLogger);
    Mockito.reset(_mockLogger);

    Mockito.when(_mockLogger.isEnabled(Mockito.eq(Level.INFO), Mockito.any(Marker.class))).thenReturn(true);
    Assert.assertTrue(log.isEnabled(Level.INFO, marker));
    Mockito.verify(_mockLogger).isEnabled(Mockito.same(Level.INFO), Mockito.same(marker));
    Mockito.verifyNoMoreInteractions(_mockLogger);
    Mockito.reset(_mockLogger);

    try {
      _mBean.setDebug(true);

      Assert.assertFalse(log.isEnabled(Level.TRACE, marker));
      Assert.assertFalse(log.isEnabled(Level.DEBUG, marker));
      Mockito.verify(_mockLogger).isEnabled(Mockito.same(Level.DEBUG), Mockito.same(marker));
      Mockito.verifyNoMoreInteractions(_mockLogger);
      Mockito.reset(_mockLogger);

      Mockito.when(_mockLogger.isEnabled(Mockito.eq(Level.DEBUG), Mockito.any(Marker.class))).thenReturn(true);
      Assert.assertFalse(log.isEnabled(Level.TRACE, marker));
      Assert.assertTrue(log.isEnabled(Level.DEBUG, marker));
      Mockito.verify(_mockLogger).isEnabled(Mockito.same(Level.DEBUG), Mockito.same(marker));
      Mockito.verifyNoMoreInteractions(_mockLogger);
      Mockito.reset(_mockLogger);

      _mBean.setTrace(true);

      Assert.assertFalse(log.isEnabled(Level.TRACE, marker));
      Assert.assertFalse(log.isEnabled(Level.DEBUG, marker));
      Mockito.verify(_mockLogger).isEnabled(Mockito.same(Level.DEBUG), Mockito.same(marker));
      Mockito.verify(_mockLogger).isEnabled(Mockito.same(Level.TRACE), Mockito.same(marker));
      Mockito.verifyNoMoreInteractions(_mockLogger);
      Mockito.reset(_mockLogger);

      Mockito.when(_mockLogger.isEnabled(Mockito.eq(Level.TRACE), Mockito.any(Marker.class))).thenReturn(true);
      Assert.assertTrue(log.isEnabled(Level.TRACE, marker));
      Assert.assertFalse(log.isEnabled(Level.DEBUG, marker));
      Mockito.verify(_mockLogger).isEnabled(Mockito.same(Level.DEBUG), Mockito.same(marker));
      Mockito.verify(_mockLogger).isEnabled(Mockito.same(Level.TRACE), Mockito.same(marker));
      Mockito.verifyNoMoreInteractions(_mockLogger);
      Mockito.reset(_mockLogger);

    } finally {
      _mBean.setDebug(false);
      _mBean.setTrace(false);
    }
  }

}

package com.linkedin.alpini.log;

import javax.annotation.Nonnull;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.message.EntryMessage;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.message.MessageFactory;
import org.apache.logging.log4j.spi.ExtendedLogger;
import org.apache.logging.log4j.util.MessageSupplier;
import org.apache.logging.log4j.util.Supplier;


public final class FastLogger implements ExtendedLogger {
  private final FastLogContext _context;
  private final ExtendedLogger _logger;

  public FastLogger(@Nonnull FastLogContext context, @Nonnull ExtendedLogger logger) {
    _context = context;
    _logger = logger;
  }

  public FastLogMBean getManagementMBean() {
    return _context.getManagementMBean();
  }

  @Override
  public void catching(Level level, Throwable t) {
    if (maybeEnabled(level)) {
      _logger.catching(level, t);
    }
  }

  @Override
  public void catching(Throwable t) {
    if (_context.isTraceEnabled()) {
      _logger.catching(t);
    }
  }

  @Override
  public void debug(Marker marker, Message msg) {
    if (logMsg(msg) && _context.isDebugEnabled()) {
      _logger.debug(marker, msg);
    }
  }

  @Override
  public void debug(Marker marker, Message msg, Throwable t) {
    if (logMsg(msg) && _context.isDebugEnabled()) {
      _logger.debug(marker, msg, t);
    }
  }

  @Override
  public void debug(Marker marker, MessageSupplier msgSupplier) {
    if (_context.isDebugEnabled()) {
      _logger.debug(marker, msgSupplier);
    }
  }

  @Override
  public void debug(Marker marker, MessageSupplier msgSupplier, Throwable t) {
    if (_context.isDebugEnabled()) {
      _logger.debug(marker, msgSupplier, t);
    }
  }

  @Override
  public void debug(Marker marker, CharSequence message) {
    if (_context.isDebugEnabled()) {
      _logger.debug(marker, message);
    }
  }

  @Override
  public void debug(Marker marker, CharSequence message, Throwable t) {
    if (_context.isDebugEnabled()) {
      _logger.debug(marker, message, t);
    }
  }

  @Override
  public void debug(Marker marker, Object message) {
    if (_context.isDebugEnabled()) {
      _logger.debug(marker, message);
    }
  }

  @Override
  public void debug(Marker marker, Object message, Throwable t) {
    if (_context.isDebugEnabled()) {
      _logger.debug(marker, message, t);
    }
  }

  @Override
  public void debug(Marker marker, String message) {
    if (_context.isDebugEnabled()) {
      _logger.debug(marker, message);
    }
  }

  @Override
  public void debug(Marker marker, String message, Object... params) {
    if (_context.isDebugEnabled()) {
      _logger.debug(marker, message, params);
    }
  }

  @Override
  public void debug(Marker marker, String message, Supplier<?>... paramSuppliers) {
    if (_context.isDebugEnabled()) {
      _logger.debug(marker, message, paramSuppliers);
    }
  }

  @Override
  public void debug(Marker marker, String message, Throwable t) {
    if (_context.isDebugEnabled()) {
      _logger.debug(marker, message, t);
    }
  }

  @Override
  public void debug(Marker marker, Supplier<?> msgSupplier) {
    if (_context.isDebugEnabled()) {
      _logger.debug(marker, msgSupplier);
    }
  }

  @Override
  public void debug(Marker marker, Supplier<?> msgSupplier, Throwable t) {
    if (_context.isDebugEnabled()) {
      _logger.debug(marker, msgSupplier, t);
    }
  }

  @Override
  public void debug(Message msg) {
    if (logMsg(msg) && _context.isDebugEnabled()) {
      _logger.debug(msg);
    }
  }

  @Override
  public void debug(Message msg, Throwable t) {
    if (logMsg(msg) && _context.isDebugEnabled()) {
      _logger.debug(msg, t);
    }
  }

  @Override
  public void debug(MessageSupplier msgSupplier) {
    if (_context.isDebugEnabled()) {
      _logger.debug(msgSupplier);
    }
  }

  @Override
  public void debug(MessageSupplier msgSupplier, Throwable t) {
    if (_context.isDebugEnabled()) {
      _logger.debug(msgSupplier, t);
    }
  }

  @Override
  public void debug(CharSequence message) {
    if (_context.isDebugEnabled()) {
      _logger.debug(message);
    }
  }

  @Override
  public void debug(CharSequence message, Throwable t) {
    if (_context.isDebugEnabled()) {
      _logger.debug(message, t);
    }
  }

  @Override
  public void debug(Object message) {
    if (_context.isDebugEnabled()) {
      _logger.debug(message);
    }
  }

  @Override
  public void debug(Object message, Throwable t) {
    if (_context.isDebugEnabled()) {
      _logger.debug(message, t);
    }
  }

  @Override
  public void debug(String message) {
    if (_context.isDebugEnabled()) {
      _logger.debug(message);
    }
  }

  @Override
  public void debug(String message, Object... params) {
    if (_context.isDebugEnabled()) {
      _logger.debug(message, params);
    }
  }

  @Override
  public void debug(String message, Supplier<?>... paramSuppliers) {
    if (_context.isDebugEnabled()) {
      _logger.debug(message, paramSuppliers);
    }
  }

  @Override
  public void debug(String message, Throwable t) {
    if (_context.isDebugEnabled()) {
      _logger.debug(message, t);
    }
  }

  @Override
  public void debug(Supplier<?> msgSupplier) {
    if (_context.isDebugEnabled()) {
      _logger.debug(msgSupplier);
    }
  }

  @Override
  public void debug(Supplier<?> msgSupplier, Throwable t) {
    if (_context.isDebugEnabled()) {
      _logger.debug(msgSupplier, t);
    }
  }

  @Override
  public void debug(Marker marker, String message, Object p0) {
    if (_context.isDebugEnabled()) {
      _logger.debug(marker, message, p0);
    }
  }

  @Override
  public void debug(Marker marker, String message, Object p0, Object p1) {
    if (_context.isDebugEnabled()) {
      _logger.debug(marker, message, p0, p1);
    }
  }

  @Override
  public void debug(Marker marker, String message, Object p0, Object p1, Object p2) {
    if (_context.isDebugEnabled()) {
      _logger.debug(marker, message, p0, p1, p2);
    }
  }

  @Override
  public void debug(Marker marker, String message, Object p0, Object p1, Object p2, Object p3) {
    if (_context.isDebugEnabled()) {
      _logger.debug(marker, message, p0, p1, p2, p3);
    }
  }

  @Override
  public void debug(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
    if (_context.isDebugEnabled()) {
      _logger.debug(marker, message, p0, p1, p2, p3, p4);
    }
  }

  @Override
  public void debug(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
    if (_context.isDebugEnabled()) {
      _logger.debug(marker, message, p0, p1, p2, p3, p4, p5);
    }
  }

  @Override
  public void debug(
      Marker marker,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6) {
    if (_context.isDebugEnabled()) {
      _logger.debug(marker, message, p0, p1, p2, p3, p4, p5, p6);
    }
  }

  @Override
  public void debug(
      Marker marker,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7) {
    if (_context.isDebugEnabled()) {
      _logger.debug(marker, message, p0, p1, p2, p3, p4, p5, p6, p7);
    }
  }

  @Override
  public void debug(
      Marker marker,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7,
      Object p8) {
    if (_context.isDebugEnabled()) {
      _logger.debug(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
    }
  }

  @Override
  public void debug(
      Marker marker,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7,
      Object p8,
      Object p9) {
    if (_context.isDebugEnabled()) {
      _logger.debug(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
    }
  }

  @Override
  public void debug(String message, Object p0) {
    if (_context.isDebugEnabled()) {
      _logger.debug(message, p0);
    }
  }

  @Override
  public void debug(String message, Object p0, Object p1) {
    if (_context.isDebugEnabled()) {
      _logger.debug(message, p0, p1);
    }
  }

  @Override
  public void debug(String message, Object p0, Object p1, Object p2) {
    if (_context.isDebugEnabled()) {
      _logger.debug(message, p0, p1, p2);
    }
  }

  @Override
  public void debug(String message, Object p0, Object p1, Object p2, Object p3) {
    if (_context.isDebugEnabled()) {
      _logger.debug(message, p0, p1, p2, p3);
    }
  }

  @Override
  public void debug(String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
    if (_context.isDebugEnabled()) {
      _logger.debug(message, p0, p1, p2, p3, p4);
    }
  }

  @Override
  public void debug(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
    if (_context.isDebugEnabled()) {
      _logger.debug(message, p0, p1, p2, p3, p4, p5);
    }
  }

  @Override
  public void debug(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6) {
    if (_context.isDebugEnabled()) {
      _logger.debug(message, p0, p1, p2, p3, p4, p5, p6);
    }
  }

  @Override
  public void debug(
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7) {
    if (_context.isDebugEnabled()) {
      _logger.debug(message, p0, p1, p2, p3, p4, p5, p6, p7);
    }
  }

  @Override
  public void debug(
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7,
      Object p8) {
    if (_context.isDebugEnabled()) {
      _logger.debug(message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
    }
  }

  @Override
  public void debug(
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7,
      Object p8,
      Object p9) {
    if (_context.isDebugEnabled()) {
      _logger.debug(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
    }
  }

  @Override
  public void entry() {
    if (_context.isTraceEnabled()) {
      _logger.entry();
    }
  }

  @Override
  public void entry(Object... params) {
    if (_context.isTraceEnabled()) {
      _logger.entry(params);
    }
  }

  @Override
  public void error(Marker marker, Message msg) {
    if (logMsg(msg)) {
      _logger.error(marker, msg);
    }
  }

  @Override
  public void error(Marker marker, Message msg, Throwable t) {
    if (logMsg(msg)) {
      _logger.error(marker, msg, t);
    }
  }

  @Override
  public void error(Marker marker, MessageSupplier msgSupplier) {
    _logger.error(marker, msgSupplier);
  }

  @Override
  public void error(Marker marker, MessageSupplier msgSupplier, Throwable t) {
    _logger.error(marker, msgSupplier, t);
  }

  @Override
  public void error(Marker marker, CharSequence message) {
    _logger.error(marker, message);
  }

  @Override
  public void error(Marker marker, CharSequence message, Throwable t) {
    _logger.error(marker, message, t);
  }

  @Override
  public void error(Marker marker, Object message) {
    _logger.error(marker, message);
  }

  @Override
  public void error(Marker marker, Object message, Throwable t) {
    _logger.error(marker, message, t);
  }

  @Override
  public void error(Marker marker, String message) {
    _logger.error(marker, message);
  }

  @Override
  public void error(Marker marker, String message, Object... params) {
    _logger.error(marker, message, params);
  }

  @Override
  public void error(Marker marker, String message, Supplier<?>... paramSuppliers) {
    _logger.error(marker, message, paramSuppliers);
  }

  @Override
  public void error(Marker marker, String message, Throwable t) {
    _logger.error(marker, message, t);
  }

  @Override
  public void error(Marker marker, Supplier<?> msgSupplier) {
    _logger.error(marker, msgSupplier);
  }

  @Override
  public void error(Marker marker, Supplier<?> msgSupplier, Throwable t) {
    _logger.error(marker, msgSupplier, t);
  }

  @Override
  public void error(Message msg) {
    _logger.error(msg);
  }

  @Override
  public void error(Message msg, Throwable t) {
    _logger.error(msg, t);
  }

  @Override
  public void error(MessageSupplier msgSupplier) {
    _logger.error(msgSupplier);
  }

  @Override
  public void error(MessageSupplier msgSupplier, Throwable t) {
    _logger.error(msgSupplier, t);
  }

  @Override
  public void error(CharSequence message) {
    _logger.error(message);
  }

  @Override
  public void error(CharSequence message, Throwable t) {
    _logger.error(message, t);
  }

  @Override
  public void error(Object message) {
    _logger.error(message);
  }

  @Override
  public void error(Object message, Throwable t) {
    _logger.error(message, t);
  }

  @Override
  public void error(String message) {
    _logger.error(message);
  }

  @Override
  public void error(String message, Object... params) {
    _logger.error(message, params);
  }

  @Override
  public void error(String message, Supplier<?>... paramSuppliers) {
    _logger.error(message, paramSuppliers);
  }

  @Override
  public void error(String message, Throwable t) {
    _logger.error(message, t);
  }

  @Override
  public void error(Supplier<?> msgSupplier) {
    _logger.error(msgSupplier);
  }

  @Override
  public void error(Supplier<?> msgSupplier, Throwable t) {
    _logger.error(msgSupplier, t);
  }

  @Override
  public void error(Marker marker, String message, Object p0) {
    _logger.error(marker, message, p0);
  }

  @Override
  public void error(Marker marker, String message, Object p0, Object p1) {
    _logger.error(marker, message, p0, p1);
  }

  @Override
  public void error(Marker marker, String message, Object p0, Object p1, Object p2) {
    _logger.error(marker, message, p0, p1, p2);
  }

  @Override
  public void error(Marker marker, String message, Object p0, Object p1, Object p2, Object p3) {
    _logger.error(marker, message, p0, p1, p2, p3);
  }

  @Override
  public void error(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
    _logger.error(marker, message, p0, p1, p2, p3, p4);
  }

  @Override
  public void error(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
    _logger.error(marker, message, p0, p1, p2, p3, p4, p5);
  }

  @Override
  public void error(
      Marker marker,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6) {
    _logger.error(marker, message, p0, p1, p2, p3, p4, p5, p6);
  }

  @Override
  public void error(
      Marker marker,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7) {
    _logger.error(marker, message, p0, p1, p2, p3, p4, p5, p6, p7);
  }

  @Override
  public void error(
      Marker marker,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7,
      Object p8) {
    _logger.error(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
  }

  @Override
  public void error(
      Marker marker,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7,
      Object p8,
      Object p9) {
    _logger.error(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
  }

  @Override
  public void error(String message, Object p0) {
    _logger.error(message, p0);
  }

  @Override
  public void error(String message, Object p0, Object p1) {
    _logger.error(message, p0, p1);
  }

  @Override
  public void error(String message, Object p0, Object p1, Object p2) {
    _logger.error(message, p0, p1, p2);
  }

  @Override
  public void error(String message, Object p0, Object p1, Object p2, Object p3) {
    _logger.error(message, p0, p1, p2, p3);
  }

  @Override
  public void error(String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
    _logger.error(message, p0, p1, p2, p3, p4);
  }

  @Override
  public void error(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
    _logger.error(message, p0, p1, p2, p3, p4, p5);
  }

  @Override
  public void error(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6) {
    _logger.error(message, p0, p1, p2, p3, p4, p5, p6);
  }

  @Override
  public void error(
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7) {
    _logger.error(message, p0, p1, p2, p3, p4, p5, p6, p7);
  }

  @Override
  public void error(
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7,
      Object p8) {
    _logger.error(message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
  }

  @Override
  public void error(
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7,
      Object p8,
      Object p9) {
    _logger.error(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
  }

  @Override
  public void exit() {
    if (_context.isTraceEnabled()) {
      _logger.exit();
    }
  }

  @Override
  public <R> R exit(R result) {
    if (_context.isTraceEnabled()) {
      return _logger.exit(result);
    }
    return result;
  }

  @Override
  public void fatal(Marker marker, Message msg) {
    if (logMsg(msg)) {
      _logger.fatal(marker, msg);
    }
  }

  @Override
  public void fatal(Marker marker, Message msg, Throwable t) {
    if (logMsg(msg)) {
      _logger.fatal(marker, msg, t);
    }
  }

  @Override
  public void fatal(Marker marker, MessageSupplier msgSupplier) {
    _logger.fatal(marker, msgSupplier);
  }

  @Override
  public void fatal(Marker marker, MessageSupplier msgSupplier, Throwable t) {
    _logger.fatal(marker, msgSupplier, t);
  }

  @Override
  public void fatal(Marker marker, CharSequence message) {
    _logger.fatal(marker, message);
  }

  @Override
  public void fatal(Marker marker, CharSequence message, Throwable t) {
    _logger.fatal(marker, message, t);
  }

  @Override
  public void fatal(Marker marker, Object message) {
    _logger.fatal(marker, message);
  }

  @Override
  public void fatal(Marker marker, Object message, Throwable t) {
    _logger.fatal(marker, message, t);
  }

  @Override
  public void fatal(Marker marker, String message) {
    _logger.fatal(marker, message);
  }

  @Override
  public void fatal(Marker marker, String message, Object... params) {
    _logger.fatal(marker, message, params);
  }

  @Override
  public void fatal(Marker marker, String message, Supplier<?>... paramSuppliers) {
    _logger.fatal(marker, message, paramSuppliers);
  }

  @Override
  public void fatal(Marker marker, String message, Throwable t) {
    _logger.fatal(marker, message, t);
  }

  @Override
  public void fatal(Marker marker, Supplier<?> msgSupplier) {
    _logger.fatal(marker, msgSupplier);
  }

  @Override
  public void fatal(Marker marker, Supplier<?> msgSupplier, Throwable t) {
    _logger.fatal(marker, msgSupplier, t);
  }

  @Override
  public void fatal(Message msg) {
    if (logMsg(msg)) {
      _logger.fatal(msg);
    }
  }

  @Override
  public void fatal(Message msg, Throwable t) {
    if (logMsg(msg)) {
      _logger.fatal(msg, t);
    }
  }

  @Override
  public void fatal(MessageSupplier msgSupplier) {
    _logger.fatal(msgSupplier);
  }

  @Override
  public void fatal(MessageSupplier msgSupplier, Throwable t) {
    _logger.fatal(msgSupplier, t);
  }

  @Override
  public void fatal(CharSequence message) {
    _logger.fatal(message);
  }

  @Override
  public void fatal(CharSequence message, Throwable t) {
    _logger.fatal(message, t);
  }

  @Override
  public void fatal(Object message) {
    _logger.fatal(message);
  }

  @Override
  public void fatal(Object message, Throwable t) {
    _logger.fatal(message, t);
  }

  @Override
  public void fatal(String message) {
    _logger.fatal(message);
  }

  @Override
  public void fatal(String message, Object... params) {
    _logger.fatal(message, params);
  }

  @Override
  public void fatal(String message, Supplier<?>... paramSuppliers) {
    _logger.fatal(message, paramSuppliers);
  }

  @Override
  public void fatal(String message, Throwable t) {
    _logger.fatal(message, t);
  }

  @Override
  public void fatal(Supplier<?> msgSupplier) {
    _logger.fatal(msgSupplier);
  }

  @Override
  public void fatal(Supplier<?> msgSupplier, Throwable t) {
    _logger.fatal(msgSupplier, t);
  }

  @Override
  public void fatal(Marker marker, String message, Object p0) {
    _logger.fatal(marker, message, p0);
  }

  @Override
  public void fatal(Marker marker, String message, Object p0, Object p1) {
    _logger.fatal(marker, message, p0, p1);
  }

  @Override
  public void fatal(Marker marker, String message, Object p0, Object p1, Object p2) {
    _logger.fatal(marker, message, p0, p1, p2);
  }

  @Override
  public void fatal(Marker marker, String message, Object p0, Object p1, Object p2, Object p3) {
    _logger.fatal(marker, message, p0, p1, p2, p3);
  }

  @Override
  public void fatal(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
    _logger.fatal(marker, message, p0, p1, p2, p3, p4);
  }

  @Override
  public void fatal(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
    _logger.fatal(marker, message, p0, p1, p2, p3, p4, p5);
  }

  @Override
  public void fatal(
      Marker marker,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6) {
    _logger.fatal(marker, message, p0, p1, p2, p3, p4, p5, p6);
  }

  @Override
  public void fatal(
      Marker marker,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7) {
    _logger.fatal(marker, message, p0, p1, p2, p3, p4, p5, p6, p7);
  }

  @Override
  public void fatal(
      Marker marker,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7,
      Object p8) {
    _logger.fatal(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
  }

  @Override
  public void fatal(
      Marker marker,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7,
      Object p8,
      Object p9) {
    _logger.fatal(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
  }

  @Override
  public void fatal(String message, Object p0) {
    _logger.fatal(message, p0);
  }

  @Override
  public void fatal(String message, Object p0, Object p1) {
    _logger.fatal(message, p0, p1);
  }

  @Override
  public void fatal(String message, Object p0, Object p1, Object p2) {
    _logger.fatal(message, p0, p1, p2);
  }

  @Override
  public void fatal(String message, Object p0, Object p1, Object p2, Object p3) {
    _logger.fatal(message, p0, p1, p2, p3);
  }

  @Override
  public void fatal(String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
    _logger.fatal(message, p0, p1, p2, p3, p4);
  }

  @Override
  public void fatal(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
    _logger.fatal(message, p0, p1, p2, p3, p4, p5);
  }

  @Override
  public void fatal(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6) {
    _logger.fatal(message, p0, p1, p2, p3, p4, p5, p6);
  }

  @Override
  public void fatal(
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7) {
    _logger.fatal(message, p0, p1, p2, p3, p4, p5, p6, p7);
  }

  @Override
  public void fatal(
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7,
      Object p8) {
    _logger.fatal(message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
  }

  @Override
  public void fatal(
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7,
      Object p8,
      Object p9) {
    _logger.fatal(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
  }

  @Override
  public Level getLevel() {
    return _logger.getLevel();
  }

  @Override
  public <MF extends MessageFactory> MF getMessageFactory() {
    return _logger.getMessageFactory();
  }

  @Override
  public String getName() {
    return _logger.getName();
  }

  @Override
  public void info(Marker marker, Message msg) {
    _logger.info(marker, msg);
  }

  @Override
  public void info(Marker marker, Message msg, Throwable t) {
    _logger.info(marker, msg, t);
  }

  @Override
  public void info(Marker marker, MessageSupplier msgSupplier) {
    _logger.info(marker, msgSupplier);
  }

  @Override
  public void info(Marker marker, MessageSupplier msgSupplier, Throwable t) {
    _logger.info(marker, msgSupplier, t);
  }

  @Override
  public void info(Marker marker, CharSequence message) {
    _logger.info(marker, message);
  }

  @Override
  public void info(Marker marker, CharSequence message, Throwable t) {
    _logger.info(marker, message, t);
  }

  @Override
  public void info(Marker marker, Object message) {
    _logger.info(marker, message);
  }

  @Override
  public void info(Marker marker, Object message, Throwable t) {
    _logger.info(marker, message, t);
  }

  @Override
  public void info(Marker marker, String message) {
    _logger.info(marker, message);
  }

  @Override
  public void info(Marker marker, String message, Object... params) {
    _logger.info(marker, message, params);
  }

  @Override
  public void info(Marker marker, String message, Supplier<?>... paramSuppliers) {
    _logger.info(marker, message, paramSuppliers);
  }

  @Override
  public void info(Marker marker, String message, Throwable t) {
    _logger.info(marker, message, t);
  }

  @Override
  public void info(Marker marker, Supplier<?> msgSupplier) {
    _logger.info(marker, msgSupplier);
  }

  @Override
  public void info(Marker marker, Supplier<?> msgSupplier, Throwable t) {
    _logger.info(marker, msgSupplier, t);
  }

  @Override
  public void info(Message msg) {
    if (logMsg(msg)) {
      _logger.info(msg);
    }
  }

  @Override
  public void info(Message msg, Throwable t) {
    if (logMsg(msg)) {
      _logger.info(msg, t);
    }
  }

  @Override
  public void info(MessageSupplier msgSupplier) {
    _logger.info(msgSupplier);
  }

  @Override
  public void info(MessageSupplier msgSupplier, Throwable t) {
    _logger.info(msgSupplier, t);
  }

  @Override
  public void info(CharSequence message) {
    _logger.info(message);
  }

  @Override
  public void info(CharSequence message, Throwable t) {
    _logger.info(message, t);
  }

  @Override
  public void info(Object message) {
    _logger.info(message);
  }

  @Override
  public void info(Object message, Throwable t) {
    _logger.info(message, t);
  }

  @Override
  public void info(String message) {
    _logger.info(message);
  }

  @Override
  public void info(String message, Object... params) {
    _logger.info(message, params);
  }

  @Override
  public void info(String message, Supplier<?>... paramSuppliers) {
    _logger.info(message, paramSuppliers);
  }

  @Override
  public void info(String message, Throwable t) {
    _logger.info(message, t);
  }

  @Override
  public void info(Supplier<?> msgSupplier) {
    _logger.info(msgSupplier);
  }

  @Override
  public void info(Supplier<?> msgSupplier, Throwable t) {
    _logger.info(msgSupplier, t);
  }

  @Override
  public void info(Marker marker, String message, Object p0) {
    _logger.info(marker, message, p0);
  }

  @Override
  public void info(Marker marker, String message, Object p0, Object p1) {
    _logger.info(marker, message, p0, p1);
  }

  @Override
  public void info(Marker marker, String message, Object p0, Object p1, Object p2) {
    _logger.info(marker, message, p0, p1, p2);
  }

  @Override
  public void info(Marker marker, String message, Object p0, Object p1, Object p2, Object p3) {
    _logger.info(marker, message, p0, p1, p2, p3);
  }

  @Override
  public void info(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
    _logger.info(marker, message, p0, p1, p2, p3, p4);
  }

  @Override
  public void info(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
    _logger.info(marker, message, p0, p1, p2, p3, p4, p5);
  }

  @Override
  public void info(
      Marker marker,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6) {
    _logger.info(marker, message, p0, p1, p2, p3, p4, p5, p6);
  }

  @Override
  public void info(
      Marker marker,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7) {
    _logger.info(marker, message, p0, p1, p2, p3, p4, p5, p6, p7);
  }

  @Override
  public void info(
      Marker marker,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7,
      Object p8) {
    _logger.info(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
  }

  @Override
  public void info(
      Marker marker,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7,
      Object p8,
      Object p9) {
    _logger.info(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
  }

  @Override
  public void info(String message, Object p0) {
    _logger.info(message, p0);
  }

  @Override
  public void info(String message, Object p0, Object p1) {
    _logger.info(message, p0, p1);
  }

  @Override
  public void info(String message, Object p0, Object p1, Object p2) {
    _logger.info(message, p0, p1, p2);
  }

  @Override
  public void info(String message, Object p0, Object p1, Object p2, Object p3) {
    _logger.info(message, p0, p1, p2, p3);
  }

  @Override
  public void info(String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
    _logger.info(message, p0, p1, p2, p3, p4);
  }

  @Override
  public void info(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
    _logger.info(message, p0, p1, p2, p3, p4, p5);
  }

  @Override
  public void info(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6) {
    _logger.info(message, p0, p1, p2, p3, p4, p5, p6);
  }

  @Override
  public void info(
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7) {
    _logger.info(message, p0, p1, p2, p3, p4, p5, p6, p7);
  }

  @Override
  public void info(
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7,
      Object p8) {
    _logger.info(message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
  }

  @Override
  public void info(
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7,
      Object p8,
      Object p9) {
    _logger.info(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
  }

  @Override
  public boolean isDebugEnabled() {
    return _context.isDebugEnabled() && _logger.isDebugEnabled();
  }

  @Override
  public boolean isDebugEnabled(Marker marker) {
    return _context.isDebugEnabled() && _logger.isDebugEnabled(marker);
  }

  private boolean maybeEnabled(Level level) {
    if (level.isLessSpecificThan(Level.TRACE)) {
      return _context.isTraceEnabled();
    }
    if (level.isLessSpecificThan(Level.DEBUG)) {
      return _context.isDebugEnabled();
    }
    return true;
  }

  @Override
  public boolean isEnabled(Level level) {
    return maybeEnabled(level) && _logger.isEnabled(level);
  }

  @Override
  public boolean isEnabled(Level level, Marker marker) {
    return maybeEnabled(level) && _logger.isEnabled(level, marker);
  }

  @Override
  public boolean isErrorEnabled() {
    return _logger.isErrorEnabled();
  }

  @Override
  public boolean isErrorEnabled(Marker marker) {
    return _logger.isErrorEnabled(marker);
  }

  @Override
  public boolean isFatalEnabled() {
    return _logger.isFatalEnabled();
  }

  @Override
  public boolean isFatalEnabled(Marker marker) {
    return _logger.isFatalEnabled(marker);
  }

  @Override
  public boolean isInfoEnabled() {
    return _logger.isInfoEnabled();
  }

  @Override
  public boolean isInfoEnabled(Marker marker) {
    return _logger.isInfoEnabled(marker);
  }

  @Override
  public boolean isTraceEnabled() {
    return _context.isTraceEnabled() && _logger.isTraceEnabled();
  }

  @Override
  public boolean isTraceEnabled(Marker marker) {
    return _context.isTraceEnabled() && _logger.isTraceEnabled(marker);
  }

  @Override
  public boolean isWarnEnabled() {
    return _logger.isWarnEnabled();
  }

  @Override
  public boolean isWarnEnabled(Marker marker) {
    return _logger.isWarnEnabled(marker);
  }

  @Override
  public void log(Level level, Marker marker, Message msg) {
    if (logMsg(msg) && maybeEnabled(level)) {
      _logger.log(level, marker, msg);
    }
  }

  @Override
  public void log(Level level, Marker marker, Message msg, Throwable t) {
    if (logMsg(msg) && maybeEnabled(level)) {
      _logger.log(level, marker, msg, t);
    }
  }

  @Override
  public void log(Level level, Marker marker, MessageSupplier msgSupplier) {
    if (maybeEnabled(level)) {
      _logger.log(level, marker, msgSupplier);
    }
  }

  @Override
  public void log(Level level, Marker marker, MessageSupplier msgSupplier, Throwable t) {
    if (maybeEnabled(level)) {
      _logger.log(level, marker, msgSupplier, t);
    }
  }

  @Override
  public void log(Level level, Marker marker, CharSequence message) {
    if (maybeEnabled(level)) {
      _logger.log(level, marker, message);
    }
  }

  @Override
  public void log(Level level, Marker marker, CharSequence message, Throwable t) {
    if (maybeEnabled(level)) {
      _logger.log(level, marker, message, t);
    }
  }

  @Override
  public void log(Level level, Marker marker, Object message) {
    if (maybeEnabled(level)) {
      _logger.log(level, marker, message);
    }
  }

  @Override
  public void log(Level level, Marker marker, Object message, Throwable t) {
    if (maybeEnabled(level)) {
      _logger.log(level, marker, message, t);
    }
  }

  @Override
  public void log(Level level, Marker marker, String message) {
    if (maybeEnabled(level)) {
      _logger.log(level, marker, message);
    }
  }

  @Override
  public void log(Level level, Marker marker, String message, Object... params) {
    if (maybeEnabled(level)) {
      _logger.log(level, marker, message, params);
    }
  }

  @Override
  public void log(Level level, Marker marker, String message, Supplier<?>... paramSuppliers) {
    if (maybeEnabled(level)) {
      _logger.log(level, marker, message, paramSuppliers);
    }
  }

  @Override
  public void log(Level level, Marker marker, String message, Throwable t) {
    if (maybeEnabled(level)) {
      _logger.log(level, marker, message, t);
    }
  }

  @Override
  public void log(Level level, Marker marker, Supplier<?> msgSupplier) {
    if (maybeEnabled(level)) {
      _logger.log(level, marker, msgSupplier);
    }
  }

  @Override
  public void log(Level level, Marker marker, Supplier<?> msgSupplier, Throwable t) {
    if (maybeEnabled(level)) {
      _logger.log(level, marker, msgSupplier, t);
    }
  }

  @Override
  public void log(Level level, Message msg) {
    if (logMsg(msg) && maybeEnabled(level)) {
      _logger.log(level, msg);
    }
  }

  @Override
  public void log(Level level, Message msg, Throwable t) {
    if (logMsg(msg) && maybeEnabled(level)) {
      _logger.log(level, msg, t);
    }
  }

  @Override
  public void log(Level level, MessageSupplier msgSupplier) {
    if (maybeEnabled(level)) {
      _logger.log(level, msgSupplier);
    }
  }

  @Override
  public void log(Level level, MessageSupplier msgSupplier, Throwable t) {
    if (maybeEnabled(level)) {
      _logger.log(level, msgSupplier, t);
    }
  }

  @Override
  public void log(Level level, CharSequence message) {
    if (maybeEnabled(level)) {
      _logger.log(level, message);
    }
  }

  @Override
  public void log(Level level, CharSequence message, Throwable t) {
    if (maybeEnabled(level)) {
      _logger.log(level, message, t);
    }
  }

  @Override
  public void log(Level level, Object message) {
    if (maybeEnabled(level)) {
      _logger.log(level, message);
    }
  }

  @Override
  public void log(Level level, Object message, Throwable t) {
    if (maybeEnabled(level)) {
      _logger.log(level, message, t);
    }
  }

  @Override
  public void log(Level level, String message) {
    if (maybeEnabled(level)) {
      _logger.log(level, message);
    }
  }

  @Override
  public void log(Level level, String message, Object... params) {
    if (maybeEnabled(level)) {
      _logger.log(level, message, params);
    }
  }

  @Override
  public void log(Level level, String message, Supplier<?>... paramSuppliers) {
    if (maybeEnabled(level)) {
      _logger.log(level, message, paramSuppliers);
    }
  }

  @Override
  public void log(Level level, String message, Throwable t) {
    if (maybeEnabled(level)) {
      _logger.log(level, message, t);
    }
  }

  @Override
  public void log(Level level, Supplier<?> msgSupplier) {
    if (maybeEnabled(level)) {
      _logger.log(level, msgSupplier);
    }
  }

  @Override
  public void log(Level level, Supplier<?> msgSupplier, Throwable t) {
    if (maybeEnabled(level)) {
      _logger.log(level, msgSupplier, t);
    }
  }

  @Override
  public void log(Level level, Marker marker, String message, Object p0) {
    if (maybeEnabled(level)) {
      _logger.log(level, marker, message, p0);
    }
  }

  @Override
  public void log(Level level, Marker marker, String message, Object p0, Object p1) {
    if (maybeEnabled(level)) {
      _logger.log(level, marker, message, p0, p1);
    }
  }

  @Override
  public void log(Level level, Marker marker, String message, Object p0, Object p1, Object p2) {
    if (maybeEnabled(level)) {
      _logger.log(level, marker, message, p0, p1, p2);
    }
  }

  @Override
  public void log(Level level, Marker marker, String message, Object p0, Object p1, Object p2, Object p3) {
    if (maybeEnabled(level)) {
      _logger.log(level, marker, message, p0, p1, p2, p3);
    }
  }

  @Override
  public void log(Level level, Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
    if (maybeEnabled(level)) {
      _logger.log(level, marker, message, p0, p1, p2, p3, p4);
    }
  }

  @Override
  public void log(
      Level level,
      Marker marker,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5) {
    if (maybeEnabled(level)) {
      _logger.log(level, marker, message, p0, p1, p2, p3, p4, p5);
    }
  }

  @Override
  public void log(
      Level level,
      Marker marker,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6) {
    if (maybeEnabled(level)) {
      _logger.log(level, marker, message, p0, p1, p2, p3, p4, p5, p6);
    }
  }

  @Override
  public void log(
      Level level,
      Marker marker,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7) {
    if (maybeEnabled(level)) {
      _logger.log(level, marker, message, p0, p1, p2, p3, p4, p5, p6, p7);
    }
  }

  @Override
  public void log(
      Level level,
      Marker marker,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7,
      Object p8) {
    if (maybeEnabled(level)) {
      _logger.log(level, marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
    }
  }

  @Override
  public void log(
      Level level,
      Marker marker,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7,
      Object p8,
      Object p9) {
    if (maybeEnabled(level)) {
      _logger.log(level, marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
    }
  }

  @Override
  public void log(Level level, String message, Object p0) {
    if (maybeEnabled(level)) {
      _logger.log(level, message, p0);
    }
  }

  @Override
  public void log(Level level, String message, Object p0, Object p1) {
    if (maybeEnabled(level)) {
      _logger.log(level, message, p0, p1);
    }
  }

  @Override
  public void log(Level level, String message, Object p0, Object p1, Object p2) {
    if (maybeEnabled(level)) {
      _logger.log(level, message, p0, p1, p2);
    }
  }

  @Override
  public void log(Level level, String message, Object p0, Object p1, Object p2, Object p3) {
    if (maybeEnabled(level)) {
      _logger.log(level, message, p0, p1, p2, p3);
    }
  }

  @Override
  public void log(Level level, String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
    if (maybeEnabled(level)) {
      _logger.log(level, message, p0, p1, p2, p3, p4);
    }
  }

  @Override
  public void log(Level level, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
    if (maybeEnabled(level)) {
      _logger.log(level, message, p0, p1, p2, p3, p4, p5);
    }
  }

  @Override
  public void log(
      Level level,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6) {
    if (maybeEnabled(level)) {
      _logger.log(level, message, p0, p1, p2, p3, p4, p5, p6);
    }
  }

  @Override
  public void log(
      Level level,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7) {
    if (maybeEnabled(level)) {
      _logger.log(level, message, p0, p1, p2, p3, p4, p5, p6, p7);
    }
  }

  @Override
  public void log(
      Level level,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7,
      Object p8) {
    if (maybeEnabled(level)) {
      _logger.log(level, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
    }
  }

  @Override
  public void log(
      Level level,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7,
      Object p8,
      Object p9) {
    if (maybeEnabled(level)) {
      _logger.log(level, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
    }
  }

  @Override
  public void printf(Level level, Marker marker, String format, Object... params) {
    if (maybeEnabled(level)) {
      _logger.printf(level, marker, format, params);
    }
  }

  @Override
  public void printf(Level level, String format, Object... params) {
    if (maybeEnabled(level)) {
      _logger.printf(level, format, params);
    }
  }

  @Override
  public <T extends Throwable> T throwing(Level level, T t) {
    if (maybeEnabled(level)) {
      return _logger.throwing(level, t);
    }
    return t;
  }

  @Override
  public <T extends Throwable> T throwing(T t) {
    if (_context.isTraceEnabled()) {
      return _logger.throwing(t);
    }
    return t;
  }

  @Override
  public void trace(Marker marker, Message msg) {
    if (logMsg(msg) && _context.isTraceEnabled()) {
      _logger.trace(marker, msg);
    }
  }

  @Override
  public void trace(Marker marker, Message msg, Throwable t) {
    if (logMsg(msg) && _context.isTraceEnabled()) {
      _logger.trace(marker, msg, t);
    }
  }

  @Override
  public void trace(Marker marker, MessageSupplier msgSupplier) {
    if (_context.isTraceEnabled()) {
      _logger.trace(marker, msgSupplier);
    }
  }

  @Override
  public void trace(Marker marker, MessageSupplier msgSupplier, Throwable t) {
    if (_context.isTraceEnabled()) {
      _logger.trace(marker, msgSupplier, t);
    }
  }

  @Override
  public void trace(Marker marker, CharSequence message) {
    if (_context.isTraceEnabled()) {
      _logger.trace(marker, message);
    }
  }

  @Override
  public void trace(Marker marker, CharSequence message, Throwable t) {
    if (_context.isTraceEnabled()) {
      _logger.trace(marker, message, t);
    }
  }

  @Override
  public void trace(Marker marker, Object message) {
    if (_context.isTraceEnabled()) {
      _logger.trace(marker, message);
    }
  }

  @Override
  public void trace(Marker marker, Object message, Throwable t) {
    if (_context.isTraceEnabled()) {
      _logger.trace(marker, message, t);
    }
  }

  @Override
  public void trace(Marker marker, String message) {
    if (_context.isTraceEnabled()) {
      _logger.trace(marker, message);
    }
  }

  @Override
  public void trace(Marker marker, String message, Object... params) {
    if (_context.isTraceEnabled()) {
      _logger.trace(marker, message, params);
    }
  }

  @Override
  public void trace(Marker marker, String message, Supplier<?>... paramSuppliers) {
    if (_context.isTraceEnabled()) {
      _logger.trace(marker, message, paramSuppliers);
    }
  }

  @Override
  public void trace(Marker marker, String message, Throwable t) {
    if (_context.isTraceEnabled()) {
      _logger.trace(marker, message, t);
    }
  }

  @Override
  public void trace(Marker marker, Supplier<?> msgSupplier) {
    if (_context.isTraceEnabled()) {
      _logger.trace(marker, msgSupplier);
    }
  }

  @Override
  public void trace(Marker marker, Supplier<?> msgSupplier, Throwable t) {
    if (_context.isTraceEnabled()) {
      _logger.trace(marker, msgSupplier, t);
    }
  }

  @Override
  public void trace(Message msg) {
    if (logMsg(msg) && _context.isTraceEnabled()) {
      _logger.trace(msg);
    }
  }

  @Override
  public void trace(Message msg, Throwable t) {
    if (logMsg(msg) && _context.isTraceEnabled()) {
      _logger.trace(msg, t);
    }
  }

  @Override
  public void trace(MessageSupplier msgSupplier) {
    if (_context.isTraceEnabled()) {
      _logger.trace(msgSupplier);
    }
  }

  @Override
  public void trace(MessageSupplier msgSupplier, Throwable t) {
    if (_context.isTraceEnabled()) {
      _logger.trace(msgSupplier, t);
    }
  }

  @Override
  public void trace(CharSequence message) {
    if (_context.isTraceEnabled()) {
      _logger.trace(message);
    }
  }

  @Override
  public void trace(CharSequence message, Throwable t) {
    if (_context.isTraceEnabled()) {
      _logger.trace(message, t);
    }
  }

  @Override
  public void trace(Object message) {
    if (_context.isTraceEnabled()) {
      _logger.trace(message);
    }
  }

  @Override
  public void trace(Object message, Throwable t) {
    if (_context.isTraceEnabled()) {
      _logger.trace(message, t);
    }
  }

  @Override
  public void trace(String message) {
    if (_context.isTraceEnabled()) {
      _logger.trace(message);
    }
  }

  @Override
  public void trace(String message, Object... params) {
    if (_context.isTraceEnabled()) {
      _logger.trace(message, params);
    }
  }

  @Override
  public void trace(String message, Supplier<?>... paramSuppliers) {
    if (_context.isTraceEnabled()) {
      _logger.trace(message, paramSuppliers);
    }
  }

  @Override
  public void trace(String message, Throwable t) {
    if (_context.isTraceEnabled()) {
      _logger.trace(message, t);
    }
  }

  @Override
  public void trace(Supplier<?> msgSupplier) {
    if (_context.isTraceEnabled()) {
      _logger.trace(msgSupplier);
    }
  }

  @Override
  public void trace(Supplier<?> msgSupplier, Throwable t) {
    if (_context.isTraceEnabled()) {
      _logger.trace(msgSupplier, t);
    }
  }

  @Override
  public void trace(Marker marker, String message, Object p0) {
    if (_context.isTraceEnabled()) {
      _logger.trace(marker, message, p0);
    }
  }

  @Override
  public void trace(Marker marker, String message, Object p0, Object p1) {
    if (_context.isTraceEnabled()) {
      _logger.trace(marker, message, p0, p1);
    }
  }

  @Override
  public void trace(Marker marker, String message, Object p0, Object p1, Object p2) {
    if (_context.isTraceEnabled()) {
      _logger.trace(marker, message, p0, p1, p2);
    }
  }

  @Override
  public void trace(Marker marker, String message, Object p0, Object p1, Object p2, Object p3) {
    if (_context.isTraceEnabled()) {
      _logger.trace(marker, message, p0, p1, p2, p3);
    }
  }

  @Override
  public void trace(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
    if (_context.isTraceEnabled()) {
      _logger.trace(marker, message, p0, p1, p2, p3, p4);
    }
  }

  @Override
  public void trace(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
    if (_context.isTraceEnabled()) {
      _logger.trace(marker, message, p0, p1, p2, p3, p4, p5);
    }
  }

  @Override
  public void trace(
      Marker marker,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6) {
    if (_context.isTraceEnabled()) {
      _logger.trace(marker, message, p0, p1, p2, p3, p4, p5, p6);
    }
  }

  @Override
  public void trace(
      Marker marker,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7) {
    if (_context.isTraceEnabled()) {
      _logger.trace(marker, message, p0, p1, p2, p3, p4, p5, p6, p7);
    }
  }

  @Override
  public void trace(
      Marker marker,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7,
      Object p8) {
    if (_context.isTraceEnabled()) {
      _logger.trace(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
    }
  }

  @Override
  public void trace(
      Marker marker,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7,
      Object p8,
      Object p9) {
    if (_context.isTraceEnabled()) {
      _logger.trace(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
    }
  }

  @Override
  public void trace(String message, Object p0) {
    if (_context.isTraceEnabled()) {
      _logger.trace(message, p0);
    }
  }

  @Override
  public void trace(String message, Object p0, Object p1) {
    if (_context.isTraceEnabled()) {
      _logger.trace(message, p0, p1);
    }
  }

  @Override
  public void trace(String message, Object p0, Object p1, Object p2) {
    if (_context.isTraceEnabled()) {
      _logger.trace(message, p0, p1, p2);
    }
  }

  @Override
  public void trace(String message, Object p0, Object p1, Object p2, Object p3) {
    if (_context.isTraceEnabled()) {
      _logger.trace(message, p0, p1, p2, p3);
    }
  }

  @Override
  public void trace(String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
    if (_context.isTraceEnabled()) {
      _logger.trace(message, p0, p1, p2, p3, p4);
    }
  }

  @Override
  public void trace(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
    if (_context.isTraceEnabled()) {
      _logger.trace(message, p0, p1, p2, p3, p4, p5);
    }
  }

  @Override
  public void trace(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6) {
    if (_context.isTraceEnabled()) {
      _logger.trace(message, p0, p1, p2, p3, p4, p5, p6);
    }
  }

  @Override
  public void trace(
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7) {
    if (_context.isTraceEnabled()) {
      _logger.trace(message, p0, p1, p2, p3, p4, p5, p6, p7);
    }
  }

  @Override
  public void trace(
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7,
      Object p8) {
    if (_context.isTraceEnabled()) {
      _logger.trace(message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
    }
  }

  @Override
  public void trace(
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7,
      Object p8,
      Object p9) {
    if (_context.isTraceEnabled()) {
      _logger.trace(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
    }
  }

  @Override
  public EntryMessage traceEntry() {
    return _context.isTraceEnabled() ? _logger.traceEntry() : NoLog.INSTANCE;
  }

  @Override
  public EntryMessage traceEntry(String format, Object... params) {
    return _context.isTraceEnabled() ? _logger.traceEntry(format, params) : NoLog.INSTANCE;
  }

  @Override
  public EntryMessage traceEntry(Supplier<?>... paramSuppliers) {
    return _context.isTraceEnabled() ? _logger.traceEntry(paramSuppliers) : NoLog.INSTANCE;
  }

  @Override
  public EntryMessage traceEntry(String format, Supplier<?>... paramSuppliers) {
    return _context.isTraceEnabled() ? _logger.traceEntry(format, paramSuppliers) : NoLog.INSTANCE;
  }

  @Override
  public EntryMessage traceEntry(Message message) {
    return logMsg(message) && _context.isTraceEnabled() ? _logger.traceEntry(message) : NoLog.INSTANCE;
  }

  @Override
  public void traceExit() {
    if (_context.isTraceEnabled()) {
      _logger.traceExit();
    }
  }

  @Override
  public <R> R traceExit(R result) {
    if (_context.isTraceEnabled()) {
      return _logger.traceExit(result);
    }
    return result;
  }

  @Override
  public <R> R traceExit(String format, R result) {
    if (_context.isTraceEnabled()) {
      return _logger.traceExit(format, result);
    }
    return result;
  }

  @Override
  public void traceExit(EntryMessage message) {
    if (logMsg(message) && _context.isTraceEnabled()) {
      _logger.traceExit(message);
    }
  }

  @Override
  public <R> R traceExit(EntryMessage message, R result) {
    if (logMsg(message) && _context.isTraceEnabled()) {
      return _logger.traceExit(message, result);
    }
    return result;
  }

  @Override
  public <R> R traceExit(Message message, R result) {
    if (logMsg(message) && _context.isTraceEnabled()) {
      return _logger.traceExit(message, result);
    }
    return result;
  }

  @Override
  public void warn(Marker marker, Message msg) {
    if (logMsg(msg)) {
      _logger.warn(marker, msg);
    }
  }

  @Override
  public void warn(Marker marker, Message msg, Throwable t) {
    if (logMsg(msg)) {
      _logger.warn(marker, msg, t);
    }
  }

  @Override
  public void warn(Marker marker, MessageSupplier msgSupplier) {
    _logger.warn(marker, msgSupplier);
  }

  @Override
  public void warn(Marker marker, MessageSupplier msgSupplier, Throwable t) {
    _logger.warn(marker, msgSupplier, t);
  }

  @Override
  public void warn(Marker marker, CharSequence message) {
    _logger.warn(marker, message);
  }

  @Override
  public void warn(Marker marker, CharSequence message, Throwable t) {
    _logger.warn(marker, message, t);
  }

  @Override
  public void warn(Marker marker, Object message) {
    _logger.warn(marker, message);
  }

  @Override
  public void warn(Marker marker, Object message, Throwable t) {
    _logger.warn(marker, message, t);
  }

  @Override
  public void warn(Marker marker, String message) {
    _logger.warn(marker, message);
  }

  @Override
  public void warn(Marker marker, String message, Object... params) {
    _logger.warn(marker, message, params);
  }

  @Override
  public void warn(Marker marker, String message, Supplier<?>... paramSuppliers) {
    _logger.warn(marker, message, paramSuppliers);
  }

  @Override
  public void warn(Marker marker, String message, Throwable t) {
    _logger.warn(marker, message, t);
  }

  @Override
  public void warn(Marker marker, Supplier<?> msgSupplier) {
    _logger.warn(marker, msgSupplier);
  }

  @Override
  public void warn(Marker marker, Supplier<?> msgSupplier, Throwable t) {
    _logger.warn(marker, msgSupplier, t);
  }

  @Override
  public void warn(Message msg) {
    if (logMsg(msg)) {
      _logger.warn(msg);
    }
  }

  @Override
  public void warn(Message msg, Throwable t) {
    if (logMsg(msg)) {
      _logger.warn(msg, t);
    }
  }

  @Override
  public void warn(MessageSupplier msgSupplier) {
    _logger.warn(msgSupplier);
  }

  @Override
  public void warn(MessageSupplier msgSupplier, Throwable t) {
    _logger.warn(msgSupplier, t);
  }

  @Override
  public void warn(CharSequence message) {
    _logger.warn(message);
  }

  @Override
  public void warn(CharSequence message, Throwable t) {
    _logger.warn(message, t);
  }

  @Override
  public void warn(Object message) {
    _logger.warn(message);
  }

  @Override
  public void warn(Object message, Throwable t) {
    _logger.warn(message, t);
  }

  @Override
  public void warn(String message) {
    _logger.warn(message);
  }

  @Override
  public void warn(String message, Object... params) {
    _logger.warn(message, params);
  }

  @Override
  public void warn(String message, Supplier<?>... paramSuppliers) {
    _logger.warn(message, paramSuppliers);
  }

  @Override
  public void warn(String message, Throwable t) {
    _logger.warn(message, t);
  }

  @Override
  public void warn(Supplier<?> msgSupplier) {
    _logger.warn(msgSupplier);
  }

  @Override
  public void warn(Supplier<?> msgSupplier, Throwable t) {
    _logger.warn(msgSupplier, t);
  }

  @Override
  public void warn(Marker marker, String message, Object p0) {
    _logger.warn(marker, message, p0);
  }

  @Override
  public void warn(Marker marker, String message, Object p0, Object p1) {
    _logger.warn(marker, message, p0, p1);
  }

  @Override
  public void warn(Marker marker, String message, Object p0, Object p1, Object p2) {
    _logger.warn(marker, message, p0, p1, p2);
  }

  @Override
  public void warn(Marker marker, String message, Object p0, Object p1, Object p2, Object p3) {
    _logger.warn(marker, message, p0, p1, p2, p3);
  }

  @Override
  public void warn(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
    _logger.warn(marker, message, p0, p1, p2, p3, p4);
  }

  @Override
  public void warn(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
    _logger.warn(marker, message, p0, p1, p2, p3, p4, p5);
  }

  @Override
  public void warn(
      Marker marker,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6) {
    _logger.warn(marker, message, p0, p1, p2, p3, p4, p5, p6);
  }

  @Override
  public void warn(
      Marker marker,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7) {
    _logger.warn(marker, message, p0, p1, p2, p3, p4, p5, p6, p7);
  }

  @Override
  public void warn(
      Marker marker,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7,
      Object p8) {
    _logger.warn(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
  }

  @Override
  public void warn(
      Marker marker,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7,
      Object p8,
      Object p9) {
    _logger.warn(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
  }

  @Override
  public void warn(String message, Object p0) {
    _logger.warn(message, p0);
  }

  @Override
  public void warn(String message, Object p0, Object p1) {
    _logger.warn(message, p0, p1);
  }

  @Override
  public void warn(String message, Object p0, Object p1, Object p2) {
    _logger.warn(message, p0, p1, p2);
  }

  @Override
  public void warn(String message, Object p0, Object p1, Object p2, Object p3) {
    _logger.warn(message, p0, p1, p2, p3);
  }

  @Override
  public void warn(String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
    _logger.warn(message, p0, p1, p2, p3, p4);
  }

  @Override
  public void warn(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
    _logger.warn(message, p0, p1, p2, p3, p4, p5);
  }

  @Override
  public void warn(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6) {
    _logger.warn(message, p0, p1, p2, p3, p4, p5, p6);
  }

  @Override
  public void warn(
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7) {
    _logger.warn(message, p0, p1, p2, p3, p4, p5, p6, p7);
  }

  @Override
  public void warn(
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7,
      Object p8) {
    _logger.warn(message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
  }

  @Override
  public void warn(
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7,
      Object p8,
      Object p9) {
    _logger.warn(message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
  }

  @Override
  public boolean isEnabled(Level level, Marker marker, Message message, Throwable t) {
    return logMsg(message) && maybeEnabled(level) && _logger.isEnabled(level, marker, message, t);
  }

  @Override
  public boolean isEnabled(Level level, Marker marker, CharSequence message, Throwable t) {
    return maybeEnabled(level) && _logger.isEnabled(level, marker, message, t);
  }

  @Override
  public boolean isEnabled(Level level, Marker marker, Object message, Throwable t) {
    return maybeEnabled(level) && _logger.isEnabled(level, marker, message, t);
  }

  @Override
  public boolean isEnabled(Level level, Marker marker, String message, Throwable t) {
    return maybeEnabled(level) && _logger.isEnabled(level, marker, message, t);
  }

  @Override
  public boolean isEnabled(Level level, Marker marker, String message) {
    return maybeEnabled(level) && _logger.isEnabled(level, marker, message);
  }

  @Override
  public boolean isEnabled(Level level, Marker marker, String message, Object... params) {
    return maybeEnabled(level) && _logger.isEnabled(level, marker, message, params);
  }

  @Override
  public boolean isEnabled(Level level, Marker marker, String message, Object p0) {
    return maybeEnabled(level) && _logger.isEnabled(level, marker, message, p0);
  }

  @Override
  public boolean isEnabled(Level level, Marker marker, String message, Object p0, Object p1) {
    return maybeEnabled(level) && _logger.isEnabled(level, marker, message, p0, p1);
  }

  @Override
  public boolean isEnabled(Level level, Marker marker, String message, Object p0, Object p1, Object p2) {
    return maybeEnabled(level) && _logger.isEnabled(level, marker, message, p0, p1, p2);
  }

  @Override
  public boolean isEnabled(Level level, Marker marker, String message, Object p0, Object p1, Object p2, Object p3) {
    return maybeEnabled(level) && _logger.isEnabled(level, marker, message, p0, p1, p2, p3);
  }

  @Override
  public boolean isEnabled(
      Level level,
      Marker marker,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4) {
    return maybeEnabled(level) && _logger.isEnabled(level, marker, message, p0, p1, p2, p3, p4);
  }

  @Override
  public boolean isEnabled(
      Level level,
      Marker marker,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5) {
    return maybeEnabled(level) && _logger.isEnabled(level, marker, message, p0, p1, p2, p3, p4, p5);
  }

  @Override
  public boolean isEnabled(
      Level level,
      Marker marker,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6) {
    return maybeEnabled(level) && _logger.isEnabled(level, marker, message, p0, p1, p2, p3, p4, p5, p6);
  }

  @Override
  public boolean isEnabled(
      Level level,
      Marker marker,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7) {
    return maybeEnabled(level) && _logger.isEnabled(level, marker, message, p0, p1, p2, p3, p4, p5, p6, p7);
  }

  @Override
  public boolean isEnabled(
      Level level,
      Marker marker,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7,
      Object p8) {
    return maybeEnabled(level) && _logger.isEnabled(level, marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
  }

  @Override
  public boolean isEnabled(
      Level level,
      Marker marker,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7,
      Object p8,
      Object p9) {
    return maybeEnabled(level) && _logger.isEnabled(level, marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
  }

  @Override
  public void logIfEnabled(String fqcn, Level level, Marker marker, Message message, Throwable t) {
    if (logMsg(message) && maybeEnabled(level)) {
      _logger.logIfEnabled(fqcn, level, marker, message, t);
    }
  }

  @Override
  public void logIfEnabled(String fqcn, Level level, Marker marker, CharSequence message, Throwable t) {
    if (maybeEnabled(level)) {
      _logger.logIfEnabled(fqcn, level, marker, message, t);
    }
  }

  @Override
  public void logIfEnabled(String fqcn, Level level, Marker marker, Object message, Throwable t) {
    if (maybeEnabled(level)) {
      _logger.logIfEnabled(fqcn, level, marker, message, t);
    }
  }

  @Override
  public void logIfEnabled(String fqcn, Level level, Marker marker, String message, Throwable t) {
    if (maybeEnabled(level)) {
      _logger.logIfEnabled(fqcn, level, marker, message, t);
    }
  }

  @Override
  public void logIfEnabled(String fqcn, Level level, Marker marker, String message) {
    if (maybeEnabled(level)) {
      _logger.logIfEnabled(fqcn, level, marker, message);
    }
  }

  @Override
  public void logIfEnabled(String fqcn, Level level, Marker marker, String message, Object... params) {
    if (maybeEnabled(level)) {
      _logger.logIfEnabled(fqcn, level, marker, message, params);
    }
  }

  @Override
  public void logIfEnabled(String fqcn, Level level, Marker marker, String message, Object p0) {
    if (maybeEnabled(level)) {
      _logger.logIfEnabled(fqcn, level, marker, message, p0);
    }
  }

  @Override
  public void logIfEnabled(String fqcn, Level level, Marker marker, String message, Object p0, Object p1) {
    if (maybeEnabled(level)) {
      _logger.logIfEnabled(fqcn, level, marker, message, p0, p1);
    }
  }

  @Override
  public void logIfEnabled(String fqcn, Level level, Marker marker, String message, Object p0, Object p1, Object p2) {
    if (maybeEnabled(level)) {
      _logger.logIfEnabled(fqcn, level, marker, message, p0, p1, p2);
    }
  }

  @Override
  public void logIfEnabled(
      String fqcn,
      Level level,
      Marker marker,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3) {
    if (maybeEnabled(level)) {
      _logger.logIfEnabled(fqcn, level, marker, message, p0, p1, p2, p3);
    }
  }

  @Override
  public void logIfEnabled(
      String fqcn,
      Level level,
      Marker marker,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4) {
    if (maybeEnabled(level)) {
      _logger.logIfEnabled(fqcn, level, marker, message, p0, p1, p2, p3, p4);
    }
  }

  @Override
  public void logIfEnabled(
      String fqcn,
      Level level,
      Marker marker,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5) {
    if (maybeEnabled(level)) {
      _logger.logIfEnabled(fqcn, level, marker, message, p0, p1, p2, p3, p4, p5);
    }
  }

  @Override
  public void logIfEnabled(
      String fqcn,
      Level level,
      Marker marker,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6) {
    if (maybeEnabled(level)) {
      _logger.logIfEnabled(fqcn, level, marker, message, p0, p1, p2, p3, p4, p5, p6);
    }
  }

  @Override
  public void logIfEnabled(
      String fqcn,
      Level level,
      Marker marker,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7) {
    if (maybeEnabled(level)) {
      _logger.logIfEnabled(fqcn, level, marker, message, p0, p1, p2, p3, p4, p5, p6, p7);
    }
  }

  @Override
  public void logIfEnabled(
      String fqcn,
      Level level,
      Marker marker,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7,
      Object p8) {
    if (maybeEnabled(level)) {
      _logger.logIfEnabled(fqcn, level, marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
    }
  }

  @Override
  public void logIfEnabled(
      String fqcn,
      Level level,
      Marker marker,
      String message,
      Object p0,
      Object p1,
      Object p2,
      Object p3,
      Object p4,
      Object p5,
      Object p6,
      Object p7,
      Object p8,
      Object p9) {
    if (maybeEnabled(level)) {
      _logger.logIfEnabled(fqcn, level, marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
    }
  }

  @Override
  public void logMessage(String fqcn, Level level, Marker marker, Message message, Throwable t) {
    if (logMsg(message) && maybeEnabled(level)) {
      _logger.logIfEnabled(fqcn, level, marker, message, t);
    }
  }

  @Override
  public void logIfEnabled(String fqcn, Level level, Marker marker, MessageSupplier msgSupplier, Throwable t) {
    if (maybeEnabled(level)) {
      _logger.logIfEnabled(fqcn, level, marker, msgSupplier, t);
    }
  }

  @Override
  public void logIfEnabled(String fqcn, Level level, Marker marker, String message, Supplier<?>... paramSuppliers) {
    if (maybeEnabled(level)) {
      _logger.logIfEnabled(fqcn, level, marker, message, paramSuppliers);
    }
  }

  @Override
  public void logIfEnabled(String fqcn, Level level, Marker marker, Supplier<?> msgSupplier, Throwable t) {
    if (maybeEnabled(level)) {
      _logger.logIfEnabled(fqcn, level, marker, msgSupplier, t);
    }
  }

  private static boolean logMsg(Message msg) {
    return msg != null && msg != NoMsg.INSTANCE && msg != NoLog.INSTANCE;
  }

  private static class NoMsg implements Message {
    private static final Message INSTANCE = new NoMsg();
    private static final Object[] PARAMS = {};

    @Override
    public final String getFormattedMessage() {
      return "";
    }

    @Override
    public final String getFormat() {
      return "";
    }

    @Override
    public final Object[] getParameters() {
      return PARAMS;
    }

    @Override
    public final Throwable getThrowable() {
      return null;
    }

    @Override
    public final String toString() {
      return "";
    }
  }

  private static final class NoLog extends NoMsg implements EntryMessage {
    private static final EntryMessage INSTANCE = new NoLog();

    @Override
    public String getText() {
      return "";
    }

    @Override
    public Message getMessage() {
      return NoMsg.INSTANCE;
    }
  }
}

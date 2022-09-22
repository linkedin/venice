/*
 * $Id$
 */
package com.linkedin.alpini.base.misc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Formatter;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.util.StringBuilderFormattable;


/**
 * Handy utility class to track the timing while processing a request. A class can create a TouchTimer,
 * then "touch" it at various points during processing. At the end, if the request took a long time,
 * it can log the output. For example:
 *
 * <pre>
 * void doSomething()
 * {
 *      TouchTimer timer = new TouchTimer();
 *      timer.touch("Calling fooService");
 *      fooService.doSomething();
 *
 *      timer.touch("Calling barService");
 *      barService.doSomething();
 *
 *      timer.touch("All Done!");
 *
 *      if(timer.getElapsedTimeMillis() &gt; 1000)
 *      {
 *         log.warn("doSomething took over a second to complete! " + timer);
 *      }
 * }
 * </pre>
 *
 * The block of code tracks the time to call each service. If the total time spent was greater than 1 second, it logs
 * a message. The logged message will include timing for each "touch". The timer is thread safe, and output from
 * TouchTimer.toString() will include the thread name along with each touch.
 *
 * @author Jemiah Westerman &lt;jwesterman@linkedin.com&gt;
 * @version $Revision$
 */
public final class TouchTimer implements StringBuilderFormattable {
  /**
   * Default maximum number of messages allowed on one timer.
   * This is a safety to prevent a timer from growing without bounds
   * if someone has it in a ThreadLocal or a static or something like that.
   */
  public static final int DEFAULT_MAX_MESSAGES;

  public static final String MAX_MESSAGES_PROPERTY = "com.linkedin.alpini.base.misc.TouchTimer.max";

  /**
   * Message used to indicate that the maximum number of messages has been reached.
   */
  private static final String MAX_MESSAGES_WARNING;

  private static final String DATE_FORMAT = "%1$tY/%1$tm/%1$td %1$tT.%1$tL";

  /**
   * Stores a singleton (deduped) copy of the thread name. The deduplication ensures that if we reference the same thread
   * name multiple times, that we only reference a single instance of that String on the heap.
   */
  private static final ThreadLocal<String> DEDUPED_THREAD_NAME = new ThreadLocal<>();

  private final int _maxMessageCount;

  /** Non-null sentinel; _first._next points to the oldest message in the list. */
  private Message _first = new Message(0, null, null, null, null);

  /** Last (most recent) message in the list. */
  private volatile Message _last = _first;

  /** Number of messages in the list. */
  private int _messageCount;

  private final long _startTimeMillis;
  private final long _startTimeNanos;

  /**
   * Default constructor
   */
  public TouchTimer() {
    this(DEFAULT_MAX_MESSAGES);
  }

  /**
   * Alternative constructor
   * @param maxMessageCount Maximum number of messages
   */
  public TouchTimer(@Nonnegative int maxMessageCount) {
    this(maxMessageCount, Time.currentTimeMillis(), Time.nanoTime());
  }

  public TouchTimer(
      @Nonnegative long startTimeMillis,
      @Nonnegative long startTimeNanos,
      @Nonnegative int maxMessageCount) {
    this(maxMessageCount, startTimeMillis, startTimeNanos);
    addMessage(new Message(startTimeNanos, getDedupedThreadName(), "start", null, null));
  }

  private TouchTimer(int maxMessageCount, long startTimeMillis, long startTimeNanos) {
    _maxMessageCount = maxMessageCount;
    _startTimeMillis = startTimeMillis;
    _startTimeNanos = startTimeNanos;
  }

  public long getStartTimeMillis() {
    return _startTimeMillis;
  }

  public long getStartTimeNanos() {
    return _startTimeNanos;
  }

  /**
   * Add an event to this timer.
   * @param name event name
   * @param klass class generating the event
   */
  public void touch(Class<?> klass, String name, Object... args) {
    String threadName = getDedupedThreadName();

    addMessage(
        new Message(Time.nanoTime(), threadName, name, args != null && args.length > 0 ? args.clone() : null, klass));
  }

  /**
   * Add an event to this timer.
   * @param name event name
   */
  public void touch(Class<?> klass, String name) {
    String threadName = getDedupedThreadName();

    addMessage(new Message(Time.nanoTime(), threadName, name, null, klass));
  }

  /**
   * Add an event to this timer.
   * @param name event name
   */
  public void touch(String name, Object... args) {
    touch(null, name, args);
  }

  /**
   * Add an event to this timer.
   * @param name event name
   */
  public void touch(String name) {
    touch(null, name);
  }

  /**
   * Add a new message to the list in a thread-safe way without using synchronization nor locks.
   * @param message message to add
   */
  private void addMessage(@Nonnull Message message) {
    Message last = _last;
    for (;;) {
      message._count = last._count + 1;
      if (message._count > _maxMessageCount) {
        // DEFAULT_MAX_MESSAGES exceeded, discard the message
        return;
      }
      if (last._next == null && NEXT_UPDATER.compareAndSet(last, null, message)) {
        _messageCount = message._count;
        _last = message;

        // We exceeded DEFAULT_MAX_MESSAGES. Add a warning and don't store any more touches for this timer.
        if (message._count == _maxMessageCount) {
          message._next = new Message(
              Time.nanoTime(),
              message._threadName,
              _maxMessageCount == DEFAULT_MAX_MESSAGES ? MAX_MESSAGES_WARNING : getMaxMessagesWarning(_maxMessageCount),
              null,
              message._klass);
        }

        return;
      }
      last = last._next;
    }
  }

  private static long nanosToMillis(long nanos) {
    return TimeUnit.NANOSECONDS.toMillis(nanos);
  }

  /**
   * Return the total time elapsed between the first and last events.
   * @return time in milliseconds
   */
  public long getElapsedTimeMillis() {
    return nanosToMillis(getElapsedTimeNanos());
  }

  /**
   * Return the total time elapsed between the first and last events.
   * @return time in nanoseconds
   */
  public long getElapsedTimeNanos() {
    return _messageCount == 0 ? 0 : _last._nanos - _first._next._nanos;
  }

  @Override
  public String toString() {
    StringBuilder trace = new StringBuilder();
    formatTo(trace);
    return trace.toString();
  }

  @Override
  public void formatTo(StringBuilder trace) {
    Formatter formatter = new Formatter(trace);

    trace.append("Total Time: ");
    TimeFormat.formatTimespan(getElapsedTimeMillis(), trace);
    trace.append(" Trace: ");

    long prevMessageNanos = 0;

    Message current = _first._next;
    while (current != null) {
      // If this is not the first message then out the time since the previous message
      if (prevMessageNanos != 0) {
        trace.append(' ');
        TimeFormat.formatTimespan(nanosToMillis(current._nanos - prevMessageNanos), trace);
        trace.append(' ');
      }

      trace.append("[").append(current._threadName).append(' ');

      // if a class was given, then build the message name using the class name.
      if (current._klass != null) {
        String longClassName = current._klass.getName();
        int lastDot = longClassName.lastIndexOf('.');
        trace.append(longClassName.subSequence(lastDot + 1, longClassName.length())).append(':');
      }

      if (current._args == null) {
        trace.append(current._name);
      } else {
        formatter.format(current._name, current._args);
      }

      trace.append(' ');

      long timeStamp = _startTimeMillis + nanosToMillis(current._nanos - _startTimeNanos);
      formatter.format(DATE_FORMAT, new Date(timeStamp));

      trace.append(']');

      prevMessageNanos = current._nanos;
      current = current._next;
    }
  }

  /** Return the messages list for this TouchTimer. For use in unit tests. */
  /* package private */ List<Message> getMessages() {
    List<Message> messages = new ArrayList<Message>(_messageCount);
    Message current = _first._next;
    while (current != null) {
      messages.add(current);
      current = current._next;
    }
    return Collections.unmodifiableList(messages);
  }

  /**
   * Visit all the messages for this TouchTimer
   * @param visitor message visitor
   */
  public void forEachMessage(@Nonnull Visitor visitor) {
    Message current = _first._next;
    while (current != null) {
      long timeStamp = _startTimeMillis + nanosToMillis(current._nanos - _startTimeNanos);
      visitor.visit(
          timeStamp,
          current._threadName,
          current._klass,
          current._name,
          current._args != null ? current._args.clone() : null);
      current = current._next;
    }
  }

  /**
   * Return the deduplicated name of the current thread. Deduplication makes suer that multiple references to the same thread name
   * reference a single String instance.
   */
  /*package private*/ static String getDedupedThreadName() {
    // Get the de-duped thread name from the map from the ThreadLocal.
    // If the name wasn't already cached or if it the name has changed, then we have to update the ThreadLocal.
    String localThreadName = Thread.currentThread().getName();
    String dedupedThreadName = DEDUPED_THREAD_NAME.get();
    if (dedupedThreadName == null || !dedupedThreadName.equals(localThreadName)) {
      DEDUPED_THREAD_NAME.set(localThreadName);
      dedupedThreadName = localThreadName;
    }

    // Return the deduped name
    return dedupedThreadName;
  }

  /**
   * Message Visitor interface
   */
  public interface Visitor {
    void visit(long timestamp, String threadName, Class<?> klass, String message, Object[] args);
  }

  /**
   * Stores an individual "touch" with the timestamp and relevant data. We try to keep the memory footprint as small
   * as possible, so we just store references to the arguments instead of storing the fully formatted String as it
   * would be printed in the log. Since the vast majority of these will not be logged (typical use case is to only log
   * very slow requests), it is preferable to only do that final formatting if and when we actually need it.
   */
  /* package private */ static final class Message {
    final long _nanos;
    final String _threadName;
    final String _name;
    final Object[] _args;
    final Class<?> _klass;
    int _count;
    volatile Message _next;

    public Message(long nanos, String threadName, String name, Object[] args, Class<?> klass) {
      _nanos = nanos;
      _threadName = threadName;
      _name = name;
      _args = args;
      _klass = klass;
    }
  }

  private static final AtomicReferenceFieldUpdater<Message, Message> NEXT_UPDATER =
      AtomicReferenceFieldUpdater.newUpdater(Message.class, Message.class, "_next");

  private static String getMaxMessagesWarning(int maxMessageCount) {
    return "TouchTimer Warning: Exceeded the maximum number of messages allowed (" + maxMessageCount
        + "). No further messages will be logged for this timer.";
  }

  static {
    DEFAULT_MAX_MESSAGES = Integer.parseUnsignedInt(System.getProperty(MAX_MESSAGES_PROPERTY, "2000"));

    MAX_MESSAGES_WARNING = getMaxMessagesWarning(DEFAULT_MAX_MESSAGES);
  }
}

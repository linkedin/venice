package com.linkedin.alpini.base.misc;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;
import javax.annotation.Nonnull;


public enum ExceptionUtil {
  SINGLETON; // Effective Java, Item 3 - Enum singleton

  /**
   * Return the cause of the given {@code Throwable} if the cause is one of the classes provided; otherwise
   * return the Throwable itself. If there are multiple matches, this method will return the cause deepest
   * in the getCause() stack (e.g. closest to the root cause).
   * <p/>
   * For example, consider "ex", a RuntimeException caused by SQLException caused by ConnectException.
   * <ul>
   *  <li/>getCause(ex, ConnectException.class) --&gt; returns the ConnectException
   *  <li/>getCause(ex, SomeOtherException.class, ConnectException.class) --&gt; returns the ConnectException
   *  <li/>getCause(ex, SQLException.class, ConnectException.class) --&gt; returns the ConnectException
   *  <li/>getCause(ex, ConnectException.class, SQLException.class) --&gt; returns the ConnectException
   *  <li/>getCause(ex, SomeOtherException.class) --&gt; returns ex
   * </ul>
   * <p/>
   * Note that subclasses do not match. For example, looking for RuntimeException would not match to
   * an IndexOutOfBoundsException. This is intentional.
   *
   * @param throwable Throwable to find the cause of
   * @param classes classes to look for in the cause stack
   * @return cause of exception.
   */
  public static Throwable getCause(Throwable throwable, @Nonnull Class<?>... classes) {
    Throwable cause = throwable;
    Throwable ret = throwable;
    while (cause != null) {
      for (Class<?> klass: classes) {
        if (cause.getClass() == klass) {
          ret = cause;
        }
      }
      Throwable next = cause.getCause();
      if (next == cause) {
        break;
      }
      cause = next;
    }
    return ret;
  }

  /**
   * Return the specific cause of the given {@code Throwable} if the cause is one of the classes provided; otherwise
   * return {@literal null}. If there are multiple matches, this method will return the cause deepest
   * in the getCause() stack (e.g. closest to the root cause).
   * <p/>
   * For example, consider "ex", a RuntimeException caused by SQLException caused by ConnectException.
   * <ul>
   *  <li/>cause(ex, ConnectException.class) --&gt; returns the ConnectException
   *  <li/>cause(ex, SomeOtherException.class) --&gt; returns {@literal null}
   * </ul>
   * <p/>
   * Note that subclasses do not match. For example, looking for RuntimeException would not match to
   * an IndexOutOfBoundsException. This is intentional.
   *
   * @param throwable Throwable to find the cause of
   * @param clazz classes to look for in the cause stack
   * @return cause of exception.
   */
  public static <E extends Throwable> E cause(Throwable throwable, @Nonnull Class<E> clazz) {
    Throwable cause = throwable;
    E ret = null;
    while (cause != null) {
      if (cause.getClass() == clazz) {
        ret = clazz.cast(cause);
      }
      Throwable next = cause.getCause();
      if (next == cause) {
        break;
      }
      cause = next;
    }
    return ret;
  }

  /**
   * Unwrap the cause of the given {@code Throwable} if the cause is assignable to the provided class; otherwise
   * return {@literal null}. If there are multiple matches, this method will return the cause deepest
   * in the getCause() stack (e.g. closest to the root cause).
   * <p/>
   * For example, consider "ex", a RuntimeException caused by SQLException caused by ConnectException.
   * <ul>
   *  <li/>getCause(ex, ConnectException.class) --&gt; returns the {@literal ConnectException}
   *  <li/>getCause(ex, SomeOtherException.class) --&gt; returns {@literal null}
   * </ul>
   * <p/>
   *
   * @param throwable Throwable to find the cause of
   * @param clazz classes to look for in the cause stack
   * @return cause of exception.
   */
  public static <E extends Throwable> E unwrap(Throwable throwable, @Nonnull Class<? extends E> clazz) {
    Throwable cause = throwable;
    E ret = null;
    while (cause != null) {
      if (clazz.isAssignableFrom(cause.getClass())) {
        ret = clazz.cast(cause);
      }
      Throwable next = cause.getCause();
      if (next == cause) {
        break;
      }
      cause = next;
    }
    return ret;
  }

  /**
   * Unwrap the cause of the given {@code Throwable} if the cause is assignable to the provided class; otherwise
   * return {@literal null}. If there are multiple matches, this method will return the cause deepest
   * in the getCause() stack (e.g. closest to the root cause).
   * <p/>
   * For example, consider "ex", a RuntimeException caused by SQLException caused by ConnectException.
   * <ul>
   *  <li/>getCause(ex) --&gt; returns the {@literal ConnectException}
   * </ul>
   * For example, consider "ex", a NullPointerException.
   * <ul>
   *  <li/>getCause(ex) --&gt; returns the {@literal NullPointerException}
   * </ul>
   * <p/>
   *
   * @param throwable Throwable to find the cause of
   * @return cause of exception.
   */
  @Nonnull
  public static Throwable unwrapCompletion(@Nonnull Throwable throwable) {
    Throwable cause = throwable;
    while (cause instanceof CompletionException || cause instanceof InvocationTargetException
        || cause.getClass() == RuntimeException.class) {
      Throwable next = cause.getCause();
      if (next == cause || next == null) {
        break;
      }
      cause = next;
    }
    return cause;
  }

  public static String getStackTrace(@Nonnull final Throwable throwable) {
    return Msg.toString(sb -> appendStackTrace(sb, throwable));
  }

  /**
   * Print our stack trace as an enclosed exception for the specified
   * stack trace.
   */
  private static void appendEnclosedStackTrace(
      StringBuilder builder,
      Throwable throwable,
      StackTraceElement[] enclosingTrace,
      String caption,
      int prefix,
      Set<Throwable> dejaVu) {
    if (dejaVu.contains(throwable)) {
      builder.append("\t[CIRCULAR REFERENCE:").append(throwable).append("]\n");
    } else {
      dejaVu.add(throwable);
      // Compute number of frames in common between this and enclosing trace
      StackTraceElement[] trace = throwable.getStackTrace();
      int m = trace.length - 1;
      int n = enclosingTrace.length - 1;
      while (m >= 0 && n >= 0 && trace[m].equals(enclosingTrace[n])) {
        m--;
        n--;
      }
      int framesInCommon = trace.length - 1 - m;

      // Print our stack trace
      appendPrefix(builder, prefix).append(caption).append(throwable).append("\n");
      for (int i = 0; i <= m; i++) {
        appendStackTraceElement(appendPrefix(builder, prefix).append("\tat "), trace[i]).append("\n");
      }
      if (framesInCommon != 0) {
        appendPrefix(builder, prefix).append("\t... ").append(framesInCommon).append(" more\n");
      }

      // Print suppressed exceptions, if any
      appendSuppressed(builder, throwable, trace, prefix + 1, dejaVu);

      // Print cause, if any
      appendCause(builder, throwable.getCause(), trace, prefix, dejaVu);
    }
  }

  @Nonnull
  public static StringBuilder appendStackTrace(@Nonnull StringBuilder builder, @Nonnull Throwable throwable) {
    // Guard against malicious overrides of Throwable.equals by
    // using a Set with identity equality semantics.
    Set<Throwable> dejaVu = Collections.newSetFromMap(new IdentityHashMap<>());
    dejaVu.add(throwable);

    // Print our stack trace
    builder.append(throwable).append("\n");
    StackTraceElement[] trace = throwable.getStackTrace();
    for (StackTraceElement traceElement: trace) {
      appendStackTraceElement(builder.append("\tat "), traceElement).append("\n");
    }
    // Print suppressed exceptions, if any
    appendSuppressed(builder, throwable, trace, 1, dejaVu);

    // Print cause, if any
    appendCause(builder, throwable.getCause(), trace, 0, dejaVu);
    return builder;
  }

  private static void appendSuppressed(
      StringBuilder builder,
      Throwable throwable,
      StackTraceElement[] trace,
      int prefix,
      Set<Throwable> dejaVu) {
    for (Throwable se: throwable.getSuppressed()) {
      appendEnclosedStackTrace(builder, se, trace, SUPPRESSED_CAPTION, prefix, dejaVu);
    }
  }

  private static void appendCause(
      StringBuilder builder,
      Throwable cause,
      StackTraceElement[] trace,
      int prefix,
      Set<Throwable> dejaVu) {
    if (cause != null) {
      appendEnclosedStackTrace(builder, cause, trace, CAUSE_CAPTION, prefix, dejaVu);
    }
  }

  private static StringBuilder appendPrefix(StringBuilder builder, int prefix) {
    while (prefix-- > 0) {
      builder.append("\t");
    }
    return builder;
  }

  /** Caption  for labeling causative exception stack traces */
  private static final String CAUSE_CAPTION = "Caused by: ";

  /** Caption for labeling suppressed exception stack traces */
  private static final String SUPPRESSED_CAPTION = "Suppressed: ";

  /**
   * Appends a {@linkplain StackTraceElement} in the same way that {@linkplain StackTraceElement#toString()} would
   * except without creating intermediate {@linkplain String} objects.
   * @param builder StringBuilder
   * @param element StackTraceElement
   * @return StringBuilder
   */
  public static StringBuilder appendStackTraceElement(StringBuilder builder, StackTraceElement element) {
    builder.append(element.getClassName()).append(".").append(element.getMethodName());
    if (element.isNativeMethod()) {
      builder.append("(Native Method)");
    } else {
      if (element.getFileName() != null && element.getLineNumber() >= 0) {
        builder.append("(").append(element.getFileName()).append(":").append(element.getLineNumber()).append(")");
      } else if (element.getFileName() != null) {
        builder.append("(").append(element.getFileName()).append(")");
      } else {
        builder.append("(Unknown Source)");
      }
    }
    return builder;
  }

  public static <T extends Throwable> T withoutStackTrace(@Nonnull T throwable) {
    throwable.setStackTrace(EMPTY_STACK_TRACE_ELEMENTS);
    return throwable;
  }

  /**
   * Utility method to catch checked exceptions from a {@linkplain Callable} and to rethrow
   * as a {@linkplain RuntimeException}.
   * @param callable Callable with checked exception
   * @param exceptionMessage Message for thrown exception
   * @return return value of callable
   */
  public static <T> T checkException(@Nonnull Callable<T> callable, @Nonnull String exceptionMessage) {
    try {
      return callable.call();
    } catch (Exception ex) {
      throw new RuntimeException(exceptionMessage, ex);
    }
  }

  private static final StackTraceElement[] EMPTY_STACK_TRACE_ELEMENTS = new StackTraceElement[0];

  public static <T> T throwException(Throwable cause) {
    if (_throwMethod == null) {
      try {
        // noinspection unchecked
        _throwMethod = Class.forName("com.linkedin.alpini.base.misc.Netty4ThrowException")
            .asSubclass(ExceptionThrower.class)
            .newInstance();
      } catch (Exception ex) {
        _throwMethod = ExceptionUtil::<RuntimeException>throw0;
      }
    }
    _throwMethod.throwException(cause);
    return null;
  }

  private static <T extends Throwable> void throw0(Throwable exception) throws T {
    // noinspection unchecked
    throw (T) exception;
  }

  private static ExceptionThrower _throwMethod;

  public interface ExceptionThrower {
    void throwException(Throwable exception);
  }
}

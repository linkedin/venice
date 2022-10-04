package com.linkedin.alpini.base.misc;

import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.logging.log4j.util.StringBuilderFormattable;


/**
 * Message holder object for use with lazy logging.
 *
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public final class Msg implements Supplier<Object>, StringBuilderFormattable {
  private static final SoftThreadLocal<StringBuilder> STRING_BUILDER_SOFT_THREAD_LOCAL =
      SoftThreadLocal.withInitial(StringBuilder::new);

  private static final int MAX_STRINGBUILDER_LENGTH = 4000;

  private Object _value;

  private Msg(Object value) {
    _value = value;
  }

  public static Msg make(Supplier<?> supplier) {
    return new Msg(supplier);
  }

  public static <T> Msg make(T value, @Nonnull Function<T, ?> action) {
    return make(() -> action.apply(value));
  }

  public static <T> Msg makeNullable(@Nullable T value, @Nonnull Function<T, ?> action) {
    return value != null ? make(value, action) : null;
  }

  public String toString() {
    return render();
  }

  private String render() {
    String result;
    if (_value instanceof String) {
      result = (String) _value;
    } else {
      StringBuilder sb = toStringBuilder(this::formatTo0);
      if (_value instanceof String) {
        result = (String) _value;
      } else {
        result = sb.toString();
        _value = result;
      }
    }
    return result;
  }

  @Override
  public void formatTo(StringBuilder buffer) {
    formatTo0(buffer);
  }

  private StringBuilder formatTo0(StringBuilder buffer) {
    Object value = get();
    if (value instanceof String) {
      buffer.append((String) value);
      _value = value;
    } else if (value instanceof StringBuilderFormattable) {
      ((StringBuilderFormattable) value).formatTo(buffer);
      _value = value;
    } else if (value instanceof Number) {
      Number number = (Number) value;
      if (number instanceof Double) {
        buffer.append(number.doubleValue());
      } else if (number instanceof Float) {
        buffer.append(number.floatValue());
      } else if (number instanceof Long) {
        buffer.append(number.longValue());
      } else {
        buffer.append(number.intValue());
      }
      _value = number;
    } else {
      String valueString = String.valueOf(value);
      buffer.append(valueString);
      _value = valueString;
    }
    return buffer;
  }

  public static StringBuilder stringBuilder() {
    StringBuilder sb = STRING_BUILDER_SOFT_THREAD_LOCAL.get();
    sb.setLength(0);
    return sb;
  }

  public static String toString(UnaryOperator<StringBuilder> fn) {
    return String.valueOf(toStringBuilder(fn));
  }

  private static StringBuilder toStringBuilder(UnaryOperator<StringBuilder> fn) {
    StringBuilder sb = stringBuilder();
    try {
      STRING_BUILDER_SOFT_THREAD_LOCAL.remove();
      return fn.apply(sb);
    } finally {
      if (sb.capacity() < MAX_STRINGBUILDER_LENGTH) {
        STRING_BUILDER_SOFT_THREAD_LOCAL.set(sb);
      }
    }
  }

  @Override
  public Object get() {
    Object value = _value;
    for (;;) {
      if (value instanceof Supplier) {
        Object next = ((Supplier) value).get();
        if (value != next) {
          value = next;
          continue;
        }
        value = next.toString();
      }
      return value;
    }
  }
}

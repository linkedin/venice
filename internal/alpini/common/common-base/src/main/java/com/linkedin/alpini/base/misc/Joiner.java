package com.linkedin.alpini.base.misc;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;


/**
 * A replacement to the use of the Google collections class of the same name,
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class Joiner {
  private static final Predicate<Object> SKIP_NULLS = o -> o != null;
  private static final Function<Stream<?>, Stream<?>> DEFAULT_STREAM = s -> s;
  private static final Function<Object, CharSequence> DEFAULT_MAPPER =
      o -> o instanceof CharSequence ? (CharSequence) o : o.toString();

  private final String _delimiter;
  private final Function<Object, CharSequence> _mapper;
  private final Function<Stream<?>, Stream<?>> _stream;

  public Joiner(@Nonnull String delimiter) {
    this(delimiter, DEFAULT_MAPPER, DEFAULT_STREAM);
  }

  private Joiner(
      @Nonnull String delimiter,
      @Nonnull Function<Object, CharSequence> mapper,
      @Nonnull Function<Stream<?>, Stream<?>> stream) {
    _delimiter = delimiter;
    _mapper = mapper;
    _stream = stream;
  }

  @CheckReturnValue
  public static @Nonnull Joiner on(char delimiter) {
    return on(Character.toString(delimiter));
  }

  @CheckReturnValue
  public static @Nonnull Joiner on(@Nonnull String delimiter) {
    return new Joiner(delimiter);
  }

  @Nonnull
  @SuppressWarnings("unchecked")
  private <T> Stream<T> streamOf(@Nonnull Stream<T> stream) {
    return (Stream<T>) _stream.apply(stream);
  }

  public @Nonnull <T> Stream<T> streamOf(@Nonnull Iterable<T> iterable) {
    return streamOf(CollectionUtil.stream(iterable));
  }

  public @Nonnull <T> Stream<T> streamOf(@Nonnull Iterator<T> iterator) {
    return streamOf(CollectionUtil.stream(iterator));
  }

  private @Nonnull String joinStream(@Nonnull Stream<?> stream) {
    // Manual implementation to preserve existing behaviour because the java.utils.StringJoiner may change
    // otherwise TestJoiner.testDontConvertCharSequenceToString test fails.
    CharSequence[] array = stream.map(_mapper).toArray(CharSequence[]::new);
    if (array.length == 0) {
      return "";
    } else if (array.length == 1) {
      return array[0].toString();
    } else {
      StringBuilder sb = new StringBuilder();
      sb.append(array[0]);
      for (int i = 1; i < array.length; i++) {
        sb.append(_delimiter).append(array[i]);
      }
      return sb.toString();
    }
  }

  private @Nonnull <B extends Appendable> B appendStream(@Nonnull B sb, @Nonnull Stream<?> stream) throws IOException {
    Iterator<CharSequence> it = stream.map(_mapper).iterator();
    if (it.hasNext()) {
      sb.append(it.next());
      while (it.hasNext()) {
        sb.append(_delimiter).append(it.next());
      }
    }
    return sb;
  }

  @CheckReturnValue
  public @Nonnull @SafeVarargs final <A> String join(@Nonnull A... array) {
    return joinStream(streamOf(Arrays.stream(array)));
  }

  @CheckReturnValue
  public @Nonnull final <A> String join(A first, A second, @Nonnull A[] array) {
    Iterable<A> prefix = Arrays.asList(first, second);
    Iterable<A> suffix = Arrays.asList(array);
    return join(CollectionUtil.concat(prefix, suffix));
  }

  @CheckReturnValue
  public @Nonnull <T> String join(@Nonnull Iterable<T> iterable) {
    return joinStream(streamOf(iterable));
  }

  @CheckReturnValue
  public @Nonnull <T> String join(@Nonnull Iterator<T> iterator) {
    return joinStream(streamOf(iterator));
  }

  @CheckReturnValue
  public @Nonnull @SafeVarargs final <B extends Appendable, T> B appendTo(@Nonnull B sb, @Nonnull T... array)
      throws IOException {
    return appendStream(sb, streamOf(Arrays.stream(array)));
  }

  @CheckReturnValue
  public @Nonnull final <B extends Appendable, T> B appendTo(@Nonnull B sb, T first, T second, @Nonnull T[] array)
      throws IOException {
    Iterable<T> prefix = Arrays.asList(first, second);
    Iterable<T> suffix = Arrays.asList(array);
    return appendTo(sb, CollectionUtil.concat(prefix, suffix));
  }

  @CheckReturnValue
  public @Nonnull <B extends Appendable, T> B appendTo(@Nonnull B sb, @Nonnull Iterable<T> iterable)
      throws IOException {
    return appendStream(sb, streamOf(iterable));
  }

  @CheckReturnValue
  public @Nonnull <B extends Appendable, T> B appendTo(@Nonnull B sb, @Nonnull Iterator<T> iterator)
      throws IOException {
    return appendStream(sb, streamOf(iterator));
  }

  @CheckReturnValue
  public @Nonnull @SafeVarargs final <T> StringBuilder appendTo(@Nonnull StringBuilder sb, @Nonnull T... array) {
    return (StringBuilder) catchIoException(this::appendTo, (Appendable) sb, array);
  }

  @CheckReturnValue
  public @Nonnull final <T> StringBuilder appendTo(@Nonnull StringBuilder sb, T first, T second, @Nonnull T[] array) {
    Iterable<T> prefix = Arrays.asList(first, second);
    Iterable<T> suffix = Arrays.asList(array);
    return appendTo(sb, CollectionUtil.concat(prefix, suffix));
  }

  @CheckReturnValue
  public @Nonnull <T> StringBuilder appendTo(@Nonnull StringBuilder sb, @Nonnull Iterable<T> iterable) {
    return (StringBuilder) catchIoException(this::appendTo, (Appendable) sb, iterable);
  }

  @CheckReturnValue
  public @Nonnull <T> StringBuilder appendTo(@Nonnull StringBuilder sb, @Nonnull Iterator<T> iterator) {
    return (StringBuilder) catchIoException(this::appendTo, (Appendable) sb, iterator);
  }

  private interface Method<T, V> {
    T run(T first, V second) throws IOException;
  }

  private static <T, V> T catchIoException(Method<T, V> method, T first, V second) {
    return ExceptionUtil.checkException(() -> method.run(first, second), "Should not occur");
  }

  @CheckReturnValue
  public @Nonnull Joiner skipNulls() {
    if (_stream == DEFAULT_STREAM) {
      return skipNulls(this);
    }
    throw new UnsupportedOperationException();
  }

  @CheckReturnValue
  private @Nonnull static Joiner skipNulls(final @Nonnull Joiner joiner) {
    return new Joiner(joiner._delimiter, joiner._mapper, o -> joiner.streamOf(o).filter(SKIP_NULLS));
  }

  @CheckReturnValue
  public @Nonnull Joiner useForNull(final @Nonnull String nullText) {
    if (_stream == DEFAULT_STREAM) {
      return useForNull(this, nullText);
    }
    throw new UnsupportedOperationException();
  }

  @CheckReturnValue
  private static @Nonnull Joiner useForNull(final @Nonnull Joiner joiner, final @Nonnull String nullText) {
    return new Joiner(joiner._delimiter, o -> o != null ? joiner._mapper.apply(o) : nullText, joiner::streamOf);
  }
}

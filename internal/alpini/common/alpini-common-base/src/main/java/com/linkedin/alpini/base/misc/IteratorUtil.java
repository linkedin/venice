package com.linkedin.alpini.base.misc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


/**
 * Utility methods for {@link Iterator}s.
 */
public final class IteratorUtil {
  private IteratorUtil() {
  }

  public static <E> Iterator<E> empty() {
    return Collections.emptyIterator();
  }

  public static <E> Iterator<E> singleton(E element) {
    return Collections.singleton(element).iterator();
  }

  @SafeVarargs
  public static <E> Iterator<E> of(E... elements) {
    return Arrays.asList(elements).iterator();
  }

  public static int count(Iterator<?> iterator) {
    int count = 0;
    while (iterator.hasNext()) {
      iterator.next();
      count++;
    }
    return count;
  }

  @SafeVarargs
  public static <T> Iterator<T> concat(Iterator<T>... source) {
    return flatMap(Arrays.asList(source).iterator(), Function.identity());
  }

  public static <T, R> Iterator<R> map(Iterator<T> iterator, Function<? super T, ? extends R> mapper) {
    return new Iterator<R>() {
      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public R next() {
        return mapper.apply(iterator.next());
      }

      @Override
      public void remove() {
        iterator.remove();
      }

      @Override
      public void forEachRemaining(Consumer<? super R> action) {
        iterator.forEachRemaining(element -> action.accept(mapper.apply(element)));
      }
    };
  }

  public static <T, R> Iterator<R> flatMap(Iterator<T> iterator, Function<? super T, Iterator<? extends R>> mapper) {
    return new Iterator<R>() {
      Iterator<? extends R> it = empty();

      @Override
      public boolean hasNext() {
        boolean hasNext;
        while (!(hasNext = it.hasNext()) && iterator.hasNext()) { // SUPPRESS CHECKSTYLE InnerAssignment
          it = Objects.requireNonNull(mapper.apply(iterator.next()));
        }
        return hasNext;
      }

      @Override
      public R next() {
        // noinspection ResultOfMethodCallIgnored
        hasNext();
        return it.next();
      }

      @Override
      public void forEachRemaining(Consumer<? super R> action) {
        it.forEachRemaining(action);
        it = empty();
        iterator.forEachRemaining(e -> mapper.apply(e).forEachRemaining(action));

      }
    };
  }

  public static <T> Iterator<T> filter(Iterator<T> iterator, Predicate<? super T> filter) {
    return new Iterator<T>() {
      T nextValue;

      @Override
      public boolean hasNext() {
        while (nextValue == null || !filter.test(nextValue)) {
          if (iterator.hasNext()) {
            nextValue = iterator.next();
          } else {
            return false;
          }
        }
        return true;
      }

      @Override
      public T next() {
        T value = hasNext() ? nextValue : iterator.next();
        nextValue = null;
        return value;
      }

      @Override
      public void remove() {
        if (nextValue == null) {
          iterator.remove();
        } else {
          throw new UnsupportedOperationException();
        }
      }
    };
  }

  public static <T> Spliterator<T> spliterator(Iterator<T> iterator) {
    return Spliterators.spliteratorUnknownSize(iterator, 0);
  }

  public static <T> Stream<T> stream(Iterator<T> iterator) {
    return StreamSupport.stream(spliterator(iterator), false);
  }

  public static <T> List<T> toList(Iterator<T> iterator) {
    List<T> list = new ArrayList<>();
    iterator.forEachRemaining(list::add);
    return list;
  }
}

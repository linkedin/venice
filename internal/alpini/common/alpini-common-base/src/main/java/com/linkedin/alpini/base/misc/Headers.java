package com.linkedin.alpini.base.misc;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public interface Headers extends Iterable<Map.Entry<String, String>> {
  boolean isEmpty();

  Headers add(CharSequence name, Object value);

  default Headers add(CharSequence name, int value) {
    return add(name, Integer.toString(value));
  }

  default Headers add(CharSequence name, long value) {
    return add(name, Long.toString(value));
  }

  Headers set(CharSequence name, Object value);

  default Headers set(CharSequence name, int value) {
    return set(name, Integer.toString(value));
  }

  default Headers set(CharSequence name, long value) {
    return set(name, Long.toString(value));
  }

  default Headers set(Iterable<Map.Entry<String, String>> entries) {
    entries.forEach(entry -> remove(entry.getKey()));
    entries.forEach(entry -> add(entry.getKey(), entry.getValue()));
    return this;
  }

  Headers remove(CharSequence name);

  List<CharSequence> getAll(CharSequence name);

  CharSequence get(CharSequence name);

  boolean contains(CharSequence name);

  boolean contains(CharSequence name, CharSequence value, boolean ignoreCase);

  Iterator<Map.Entry<CharSequence, CharSequence>> iteratorCharSequence();

  <T> Optional<T> unwrap(Class<T> type);

  Headers EMPTY_HEADERS = new Headers() {
    @Override
    public boolean isEmpty() {
      return true;
    }

    @Override
    public Headers add(CharSequence name, Object value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Headers set(CharSequence name, Object value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Headers remove(CharSequence name) {
      return this;
    }

    @Override
    public List<CharSequence> getAll(CharSequence name) {
      return Collections.emptyList();
    }

    @Override
    public CharSequence get(CharSequence name) {
      return null;
    }

    @Override
    public boolean contains(CharSequence name) {
      return false;
    }

    @Override
    public boolean contains(CharSequence name, CharSequence value, boolean ignoreCase) {
      return false;
    }

    @Override
    public Iterator<Map.Entry<CharSequence, CharSequence>> iteratorCharSequence() {
      return Collections.emptyIterator();
    }

    @Override
    public <T> Optional<T> unwrap(Class<T> type) {
      return Optional.empty();
    }

    @Override
    public Iterator<Map.Entry<String, String>> iterator() {
      return Collections.emptyIterator();
    }
  };
}

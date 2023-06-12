package com.linkedin.alpini.netty4.misc;

import com.linkedin.alpini.base.misc.Headers;
import io.netty.handler.codec.http.HttpHeaders;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class BasicHeaders implements Headers {
  private final HttpHeaders _headers;

  public BasicHeaders(@Nonnull HttpHeaders headers) {
    _headers = Objects.requireNonNull(headers);
  }

  @Override
  public boolean isEmpty() {
    return _headers.isEmpty();
  }

  @Override
  public Headers add(CharSequence name, Object value) {
    _headers.add(name, value);
    return this;
  }

  @Override
  public Headers add(CharSequence name, int value) {
    _headers.addInt(name, value);
    return this;
  }

  @Override
  public Headers set(CharSequence name, Object value) {
    _headers.set(name, value);
    return this;
  }

  @Override
  public Headers set(CharSequence name, int value) {
    _headers.setInt(name, value);
    return this;
  }

  @Override
  public Headers set(Iterable<Map.Entry<String, String>> entries) {
    entries.forEach(entry -> remove(entry.getKey()));
    entries.forEach(entry -> add(entry.getKey(), entry.getValue()));
    return this;
  }

  @Override
  public Headers remove(CharSequence name) {
    _headers.remove(name);
    return this;
  }

  @Override
  public List<CharSequence> getAll(CharSequence name) {
    return _headers.getAll(name).stream().collect(Collectors.toList());
  }

  @Override
  public CharSequence get(CharSequence name) {
    return _headers.get(name);
  }

  @Override
  public boolean contains(CharSequence name) {
    return _headers.contains(name);
  }

  @Override
  public boolean contains(CharSequence name, CharSequence value, boolean ignoreCase) {
    return _headers.contains(name, value, ignoreCase);
  }

  @Override
  public <T> Optional<T> unwrap(Class<T> type) {
    if (type.isAssignableFrom(_headers.getClass())) {
      return Optional.of(type.cast(_headers));
    }
    return Optional.empty();
  }

  @Override
  public Iterator<Map.Entry<String, String>> iterator() {
    return _headers.iterator();
  }

  @Override
  public Iterator<Map.Entry<CharSequence, CharSequence>> iteratorCharSequence() {
    return _headers.iteratorCharSequence();
  }
}

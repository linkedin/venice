package com.linkedin.alpini.netty4.http2;

import com.linkedin.alpini.base.misc.IteratorUtil;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.util.AsciiString;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;


public class Http1Headers extends HttpHeaders {
  private final Http2Headers _headers;
  private final boolean _validate;
  private transient Set<String> _names;

  public Http1Headers(boolean validateCopies) {
    this(new DefaultHttp2Headers(validateCopies), validateCopies);
  }

  public Http1Headers(Http2Headers headers, boolean validateCopies) {
    _headers = headers;
    _validate = validateCopies;
  }

  public Http2Headers getHttp2Headers() {
    return _headers;
  }

  static CharSequence lc(String s) {
    return AsciiString.cached(s).toLowerCase();
  }

  static CharSequence lc(CharSequence s) {
    return AsciiString.of(s).toLowerCase();
  }

  static CharSequence cs(CharSequence s) {
    return s;
  }

  static String str(CharSequence cs) {
    return cs == null || cs instanceof String ? (String) cs : cs.toString();
  }

  static String strNotNull(CharSequence cs) {
    return cs instanceof String ? (String) cs : cs.toString();
  }

  @Override
  public String get(String name) {
    return str(_headers.get(lc(name)));
  }

  @Override
  public String get(CharSequence name) {
    return str(_headers.get(lc(name)));
  }

  @Override
  public String get(CharSequence name, String defaultValue) {
    return str(_headers.get(lc(name), defaultValue));
  }

  @Override
  public Integer getInt(CharSequence name) {
    return _headers.getInt(lc(name));
  }

  @Override
  public int getInt(CharSequence name, int defaultValue) {
    return _headers.getInt(lc(name), defaultValue);
  }

  @Override
  public Short getShort(CharSequence name) {
    return _headers.getShort(lc(name));
  }

  @Override
  public short getShort(CharSequence name, short defaultValue) {
    return _headers.getShort(lc(name), defaultValue);
  }

  @Override
  public Long getTimeMillis(CharSequence name) {
    return _headers.getTimeMillis(lc(name));
  }

  @Override
  public long getTimeMillis(CharSequence name, long defaultValue) {
    return _headers.getTimeMillis(lc(name), defaultValue);
  }

  @Override
  public List<String> getAll(String name) {
    return getAll0(lc(name));
  }

  @Override
  public List<String> getAll(CharSequence name) {
    return getAll0(lc(name));
  }

  private List<String> getAll0(CharSequence name) {
    List<CharSequence> list = _headers.getAll(name);
    list.replaceAll(Http1Headers::strNotNull);
    // noinspection unchecked
    return (List<String>) (List<?>) list;
  }

  @Override
  public List<Map.Entry<String, String>> entries() {
    return IteratorUtil.toList(iterator());
  }

  @Override
  public boolean contains(String name) {
    return _headers.contains(lc(name));
  }

  @Override
  public boolean contains(CharSequence name) {
    return _headers.contains(lc(name));
  }

  static class LazyAdaptor implements Map.Entry<String, String> {
    private final Map.Entry<CharSequence, CharSequence> _entry;
    private transient String _key;
    private transient String _value;

    public LazyAdaptor(@Nonnull Map.Entry<CharSequence, CharSequence> entry) {
      _entry = entry;
    }

    @Override
    public String getKey() {
      if (_key == null) {
        _key = _entry.getKey().toString();
      }
      return _key;
    }

    @Override
    public String getValue() {
      CharSequence value = _entry.getValue();
      if (_value == null || !AsciiString.contentEquals(_value, value)) {
        _value = str(value);
      }
      return _value;
    }

    @Override
    public String setValue(String value) {
      return str(_entry.setValue(value));
    }
  }

  @SuppressWarnings("unchecked")
  static Map.Entry<String, String> castMapEntry(Map.Entry<CharSequence, CharSequence> entry) {
    return entry.getKey() instanceof String && entry.getValue() instanceof String
        ? (Map.Entry<String, String>) (Map.Entry<?, ?>) entry
        : new LazyAdaptor(entry);

  }

  @Nonnull
  @Override
  public Iterator<Map.Entry<String, String>> iterator() {
    return IteratorUtil.map(iteratorCharSequence(), Http1Headers::castMapEntry);
  }

  @Override
  public Iterator<Map.Entry<CharSequence, CharSequence>> iteratorCharSequence() {
    return IteratorUtil.filter(_headers.iterator(), this::isVisible);
  }

  @Override
  public Iterator<String> valueStringIterator(CharSequence name) {
    return IteratorUtil.map(valueCharSequenceIterator(name), CharSequence::toString);
  }

  @Override
  public Iterator<? extends CharSequence> valueCharSequenceIterator(CharSequence name) {
    return _headers.valueIterator(lc(name));
  }

  @Override
  public boolean isEmpty() {
    return !iteratorCharSequence().hasNext();
  }

  @Override
  public int size() {
    return IteratorUtil.count(iteratorCharSequence());
  }

  private boolean isVisible(Map.Entry<? extends CharSequence, ?> entry) {
    return isVisible(entry.getKey());
  }

  protected boolean isVisible(CharSequence cs) {
    return cs.charAt(0) != ':';
  }

  @Override
  public Set<String> names() {
    if (_names == null) {
      _names = new AbstractSet<String>() {
        private Iterator<CharSequence> namesIterator() {
          return IteratorUtil.filter(_headers.names().iterator(), Http1Headers.this::isVisible);
        }

        @Nonnull
        @Override
        public Iterator<String> iterator() {
          return IteratorUtil.map(namesIterator(), CharSequence::toString);
        }

        @Override
        public int size() {
          return IteratorUtil.count(namesIterator());
        }
      };
    }
    return _names;
  }

  @Override
  public HttpHeaders add(String name, Object value) {
    _headers.addObject(lc(name), value);
    return this;
  }

  @Override
  public HttpHeaders add(CharSequence name, Object value) {
    _headers.addObject(lc(name), value);
    return this;
  }

  @Override
  public HttpHeaders add(String name, Iterable<?> values) {
    _headers.addObject(lc(name), values);
    return this;
  }

  @Override
  public HttpHeaders add(CharSequence name, Iterable<?> values) {
    _headers.addObject(lc(name), values);
    return this;
  }

  @Override
  public HttpHeaders add(HttpHeaders headers) {
    if (headers instanceof Http1Headers) {
      _headers.add(((Http1Headers) headers)._headers);
    } else {
      super.add(headers);
    }
    return this;
  }

  @Override
  public HttpHeaders addInt(CharSequence name, int value) {
    _headers.addInt(lc(name), value);
    return this;
  }

  @Override
  public HttpHeaders addShort(CharSequence name, short value) {
    _headers.addShort(lc(name), value);
    return this;
  }

  @Override
  public HttpHeaders set(String name, Object value) {
    _headers.setObject(lc(name), value);
    return this;
  }

  @Override
  public HttpHeaders set(CharSequence name, Object value) {
    _headers.setObject(lc(name), value);
    return this;
  }

  @Override
  public HttpHeaders set(String name, Iterable<?> values) {
    _headers.setObject(lc(name), values);
    return this;
  }

  @Override
  public HttpHeaders set(CharSequence name, Iterable<?> values) {
    _headers.setObject(lc(name), values);
    return this;
  }

  @Override
  public HttpHeaders set(HttpHeaders headers) {
    if (headers instanceof Http1Headers) {
      _headers.set(((Http1Headers) headers)._headers);
    } else {
      super.set(headers);
    }
    return this;
  }

  @Override
  public HttpHeaders setAll(HttpHeaders headers) {
    if (headers instanceof Http1Headers) {
      _headers.setAll(((Http1Headers) headers)._headers);
    } else {
      super.setAll(headers);
    }
    return this;
  }

  @Override
  public HttpHeaders setInt(CharSequence name, int value) {
    _headers.setInt(lc(name), value);
    return this;
  }

  @Override
  public HttpHeaders setShort(CharSequence name, short value) {
    _headers.setShort(lc(name), value);
    return this;
  }

  @Override
  public HttpHeaders remove(String name) {
    _headers.remove(lc(name));
    return this;
  }

  @Override
  public HttpHeaders remove(CharSequence name) {
    _headers.remove(lc(name));
    return this;
  }

  @Override
  public HttpHeaders clear() {
    _headers.clear();
    return this;
  }

  @Override
  public boolean contains(String name, String value, boolean ignoreCase) {
    return _headers.contains(lc(name), value, ignoreCase);
  }

  @Override
  public boolean contains(CharSequence name, CharSequence value, boolean ignoreCase) {
    return _headers.contains(lc(name), value, ignoreCase);
  }

  @Override
  public Http1Headers copy() {
    return new Http1Headers(new DefaultHttp2Headers(_validate, _headers.size()).setAll(_headers), _validate);
  }
}

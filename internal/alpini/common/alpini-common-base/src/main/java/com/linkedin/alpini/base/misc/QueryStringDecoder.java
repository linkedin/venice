/*
 * Copyright 2009 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.linkedin.alpini.base.misc;

import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;


/**
 * Imported this implementation from netty 3.2.4 because the implementation within 3.5.11 will corrupt
 * uri paths, converting ';' semicolons into '&amp;' ampersands.
 * <p/>
 * Splits an HTTP query string into a path string and key-value parameter pairs.
 * This decoder is for one time use only.  Create a new instance for each URI:
 * <pre>
 * {@link QueryStringDecoder} decoder = new {@link QueryStringDecoder}("/hello?recipient=world");
 * assert decoder.getPath().equals("/hello");
 * assert decoder.getParameters().get("recipient").equals("world");
 * </pre>
 *
 * @author <a href="http://www.jboss.org/netty/">The Netty Project</a>
 * @author Andy Taylor (andy.taylor@jboss.org)
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 * @author <a href="http://tsunanet.net/">Benoit Sigoure</a>
 * @version $Rev: 2302 $, $Date: 2010-06-14 20:07:44 +0900 (Mon, 14 Jun 2010) $
 *
 * @see "org.jboss.netty.handler.codec.http.QueryStringEncoder"
 */
public class QueryStringDecoder {
  private final Charset _charset;
  private final String _uri;
  private String _path;
  private Map<String, List<String>> _params;

  /**
   * Creates a new decoder that decodes the specified URI. The decoder will
   * assume that the query string is encoded in UTF-8.
   */
  public QueryStringDecoder(String uri) {
    this(uri, StandardCharsets.UTF_8);
  }

  /**
   * Creates a new decoder that decodes the specified URI encoded in the
   * specified charset.
   */
  public QueryStringDecoder(String uri, Charset charset) {
    this._uri = Objects.requireNonNull(uri, "uri");
    this._charset = Objects.requireNonNull(charset, "charset");
  }

  /**
   * @deprecated Use {@link #QueryStringDecoder(String, Charset)} instead.
   */
  @Deprecated
  public QueryStringDecoder(String uri, String charset) {
    this(uri, Charset.forName(charset));
  }

  /**
   * Creates a new decoder that decodes the specified URI. The decoder will
   * assume that the query string is encoded in UTF-8.
   */
  public QueryStringDecoder(URI uri) {
    this(uri, StandardCharsets.UTF_8);
  }

  /**
   * Creates a new decoder that decodes the specified URI encoded in the
   * specified charset.
   */
  public QueryStringDecoder(URI uri, Charset charset) {
    this._uri = Objects.requireNonNull(uri, "uri").toASCIIString();
    this._charset = Objects.requireNonNull(charset, "charset");
  }

  /**
   * @deprecated Use {@link #QueryStringDecoder(URI, Charset)} instead.
   */
  @Deprecated
  public QueryStringDecoder(URI uri, String charset) {
    this(uri, Charset.forName(charset));
  }

  /**
   * Returns the decoded path string of the URI.
   */
  public String getPath() {
    if (_path == null) {
      int pathEndPos = _uri.indexOf('?');
      if (pathEndPos < 0) {
        _path = _uri;
      } else {
        return _path = _uri.substring(0, pathEndPos); // SUPPRESS CHECKSTYLE InnerAssignment
      }
    }
    return _path;
  }

  /**
   * Returns the decoded key-value parameter pairs of the URI.
   */
  public Map<String, List<String>> getParameters() {
    if (_params == null) {
      int pathLength = getPath().length();
      if (_uri.length() == pathLength) {
        return Collections.emptyMap();
      }
      _params = decodeParams(_uri.substring(pathLength + 1));
    }
    return _params;
  }

  private Map<String, List<String>> decodeParams(String s) {
    Map<String, List<String>> params = new LinkedHashMap<>();
    String name = null;
    int pos = 0; // Beginning of the unprocessed region
    int i; // End of the unprocessed region
    char c = 0; // Current character
    for (i = 0; i < s.length(); i++) {
      c = s.charAt(i);
      if (c == '=' && name == null) {
        if (pos != i) {
          name = decodeComponent(s.substring(pos, i), _charset);
        }
        pos = i + 1;
      } else if (c == '&') {
        if (name == null && pos != i) {
          // We haven't seen an `=' so far but moved forward.
          // Must be a param of the form '&a&' so add it with
          // an empty value.
          addParam(params, decodeComponent(s.substring(pos, i), _charset), "");
        } else if (name != null) {
          addParam(params, name, decodeComponent(s.substring(pos, i), _charset));
          name = null;
        }
        pos = i + 1;
      }
    }

    if (pos != i) { // Are there characters we haven't dealt with?
      if (name == null) { // Yes and we haven't seen any `='.
        addParam(params, decodeComponent(s.substring(pos, i), _charset), "");
      } else { // Yes and this must be the last value.
        addParam(params, name, decodeComponent(s.substring(pos, i), _charset));
      }
    } else if (name != null) { // Have we seen a name without value?
      addParam(params, name, "");
    }

    return params;
  }

  private static String decodeComponent(String s, Charset charset) {
    if (s == null) {
      return "";
    }

    return URLCodec.decode(s, charset);
  }

  private static void addParam(Map<String, List<String>> params, String name, String value) {
    List<String> values = params.get(name);
    if (values == null) {
      values = new ArrayList<>(1); // Often there's only 1 value.
      params.put(name, values);
    }
    values.add(value);
  }
}

package com.linkedin.alpini.netty4.misc;

import com.linkedin.alpini.base.misc.URLCodec;
import io.netty.util.AsciiString;
import java.nio.charset.Charset;
import javax.annotation.Nonnull;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public enum AsciiStringURLCodec {
  SINGLETON; // Effective Java, Item 3 - Enum singleton

  /**
   * Translates a string into {@code application/x-www-form-urlencoded}
   * format using a specific encoding scheme. This method uses the
   * supplied encoding scheme to obtain the bytes for unsafe
   * characters.
   * <p>
   * <em><strong>Note:</strong> The <a href=
   * "http://www.w3.org/TR/html40/appendix/notes.html#non-ascii-chars">
   * World Wide Web Consortium Recommendation</a> states that
   * UTF-8 should be used. Not doing so may introduce
   * incompatibilities.</em>
   *
   * @param   s   {@code CharSequence} to be translated.
   * @param enc   The name of a supported
   *    <a href="../lang/package-summary.html#charenc">character
   *    encoding</a>.
   * @return  the translated {@code AsciiString}.
   * @see java.net.URLEncoder#encode(java.lang.String, java.lang.String)
   */
  public static @Nonnull AsciiString encode(@Nonnull CharSequence s, @Nonnull Charset enc) {
    return AsciiString.of(URLCodec.encode(s, enc));
  }

  /**
   * Decodes a {@code application/x-www-form-urlencoded} string using a specific
   * encoding scheme.
   * The supplied encoding is used to determine
   * what characters are represented by any consecutive sequences of the
   * form "<i>{@code %xy}</i>".
   * <p>
   * <em><strong>Note:</strong> The <a href=
   * "http://www.w3.org/TR/html40/appendix/notes.html#non-ascii-chars">
   * World Wide Web Consortium Recommendation</a> states that
   * UTF-8 should be used. Not doing so may introduce
   * incompatibilities.</em>
   *
   * @param s the {@code AsciiString} to decode
   * @param enc   The name of a supported
   *    <a href="../lang/package-summary.html#charenc">character
   *    encoding</a>.
   * @return the newly decoded {@code CharSequence}
   * @see java.net.URLDecoder#decode(java.lang.String, java.lang.String)
   */
  public static @Nonnull CharSequence decode(@Nonnull AsciiString s, @Nonnull Charset enc) {
    return URLCodec.decode(s, enc);
  }

}

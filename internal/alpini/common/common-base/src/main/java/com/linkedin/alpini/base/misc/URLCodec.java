/*
 * Portions Copyright (c) 1998, 2013, Oracle and/or its affiliates. All rights reserved.
 */
package com.linkedin.alpini.base.misc;

import java.io.CharArrayWriter;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;


/**
 * Copies of the implementation of {@link java.net.URLEncoder#encode(String, String)} and
 * {@link java.net.URLDecoder#decode(String, String)} except with the following changes:
 *
 * <ul>
 *   <li>Accept a {@link Charset} argument for the character set instead of a string
 *   which would cause {@link Charset#forName(String)} to be called.</li>
 *   <li>Internally use {@link StringBuilder} instead of {@link StringBuffer}.</li>
 *   <li>These methods no longer throw a checked exception because it
 *   no longer calls {@link Charset#forName(String)}.</li>
 *   <li>These methods are annotated with {@link Nonnull} annotations.</li>
 * </ul>
 *
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public enum URLCodec {
  SINGLETON; // Effective Java, Item 3 - Enum singleton

  private static final SoftThreadLocal<StringBuilder> STRING_BUILDER = SoftThreadLocal.withInitial(StringBuilder::new);

  private static final ThreadLocal<CharBuffer> CHAR_BUFFER_THREAD_LOCAL =
      ThreadLocal.withInitial(() -> CharBuffer.allocate(512));

  static final BitSet DONT_NEED_ENCODING = getDontNeedEncoding();
  static final int CASE_DIFF = ('a' - 'A');

  /**
   * The list of characters that are not encoded has been
   * determined as follows:
   *
   * RFC 2396 states:
   * -----
   * Data characters that are allowed in a URI but do not have a
   * reserved purpose are called unreserved.  These include upper
   * and lower case letters, decimal digits, and a limited set of
   * punctuation marks and symbols.
   *
   * unreserved  = alphanum | mark
   *
   * mark        = "-" | "_" | "." | "!" | "~" | "*" | "'" | "(" | ")"
   *
   * Unreserved characters can be escaped without changing the
   * semantics of the URI, but this should not be done unless the
   * URI is being used in a context that does not allow the
   * unescaped character to appear.
   * -----
   *
   * It appears that both Netscape and Internet Explorer escape
   * all special characters from this list with the exception
   * of "-", "_", ".", "*". While it is not clear why they are
   * escaping the other characters, perhaps it is safest to
   * assume that there might be contexts in which the others
   * are unsafe if not escaped. Therefore, we will use the same
   * list. It is also noteworthy that this is consistent with
   * O'Reilly's "HTML: The Definitive Guide" (page 164).
   *
   * As a last note, Intenet Explorer does not encode the "@"
   * character which is clearly not unreserved according to the
   * RFC. We are being consistent with the RFC in this matter,
   * as is Netscape.
   *
   */
  private static BitSet getDontNeedEncoding() {

    BitSet dontNeedEncoding = new BitSet(256);
    int i;
    for (i = 'a'; i <= 'z'; i++) {
      dontNeedEncoding.set(i);
    }
    for (i = 'A'; i <= 'Z'; i++) {
      dontNeedEncoding.set(i);
    }
    for (i = '0'; i <= '9'; i++) {
      dontNeedEncoding.set(i);
    }
    dontNeedEncoding.set(' '); /* encoding a space to a + is done
                                    * in the encode() method */
    dontNeedEncoding.set('-');
    dontNeedEncoding.set('_');
    dontNeedEncoding.set('.');
    dontNeedEncoding.set('*');

    return dontNeedEncoding;
  }

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
   * @param   s   {@code String} to be translated.
   * @param   charset   The name of a supported
   *    <a href="../lang/package-summary.html#charenc">character
   *    encoding</a>.
   * @return  the translated {@code String}.
   * @see java.net.URLEncoder#encode(java.lang.String, java.lang.String)
   */
  public static @Nonnull String encode(@Nonnull String s, @Nonnull Charset charset) {
    return String.valueOf(encode((CharSequence) s, charset));
  }

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
   * @param   charset   The name of a supported
   *    <a href="../lang/package-summary.html#charenc">character
   *    encoding</a>.
   * @return  the translated {@linkplain CharSequence}. Note that if a conversion was required, the
   *    returned {@linkplain CharSequence} is shared thread local and that the caller is required to copy it.
   * @see java.net.URLEncoder#encode(java.lang.String, java.lang.String)
   */
  public static @Nonnull CharSequence encode(@Nonnull CharSequence s, @Nonnull Charset charset) {
    boolean needToChange = false;
    StringBuilder out = STRING_BUILDER.get();
    out.setLength(0);
    out.ensureCapacity(s.length());
    CharArrayWriter charArrayWriter = new CharArrayWriter();

    for (int i = 0; i < s.length();) {
      int c = (int) s.charAt(i);
      // System.out.println("Examining character: " + c);
      if (DONT_NEED_ENCODING.get(c)) {
        if (c == ' ') {
          c = '+';
          needToChange = true;
        }
        // System.out.println("Storing: " + c);
        out.append((char) c);
        i++; // SUPPRESS CHECKSTYLE ModifiedControlVariable
      } else {
        // convert to external encoding before hex conversion
        do {
          charArrayWriter.write(c);
          /*
           * If this character represents the start of a Unicode
           * surrogate pair, then pass in two characters. It's not
           * clear what should be done if a bytes reserved in the
           * surrogate pairs range occurs outside of a legal
           * surrogate pair. For now, just treat it as if it were
           * any other character.
           */
          if (c >= 0xD800 && c <= 0xDBFF) {
            /*
              System.out.println(Integer.toHexString(c)
              + " is high surrogate");
            */
            if ((i + 1) < s.length()) {
              int d = (int) s.charAt(i + 1);
              /*
                System.out.println("\tExamining "
                + Integer.toHexString(d));
              */
              if (d >= 0xDC00 && d <= 0xDFFF) {
                /*
                  System.out.println("\t"
                  + Integer.toHexString(d)
                  + " is low surrogate");
                */
                charArrayWriter.write(d);
                i++; // SUPPRESS CHECKSTYLE ModifiedControlVariable
              }
            }
          }
          i++; // SUPPRESS CHECKSTYLE ModifiedControlVariable
        } while (i < s.length() && !DONT_NEED_ENCODING.get((c = (int) s.charAt(i)))); // SUPPRESS CHECKSTYLE
                                                                                      // InnerAssignment

        charArrayWriter.flush();
        ByteBuffer ba = charset.encode(CharBuffer.wrap(charArrayWriter.toCharArray()));
        while (ba.hasRemaining()) {
          byte b = ba.get();
          out.append('%');
          char ch = Character.forDigit((b >> 4) & 0xF, 16);
          // converting to use uppercase letter as part of
          // the hex value if ch is a letter.
          if (Character.isLetter(ch)) {
            ch -= CASE_DIFF;
          }
          out.append(ch);
          ch = Character.forDigit(b & 0xF, 16);
          if (Character.isLetter(ch)) {
            ch -= CASE_DIFF;
          }
          out.append(ch);
        }
        charArrayWriter.reset();
        needToChange = true;
      }
    }

    return (needToChange ? out : s);
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
   * @param s the {@code String} to decode
   * @param enc   The name of a supported
   *    <a href="../lang/package-summary.html#charenc">character
   *    encoding</a>.
   * @return the newly decoded {@code String}
   * @see java.net.URLDecoder#decode(java.lang.String, java.lang.String)
   */
  public static @Nonnull String decode(@Nonnull String s, @Nonnull Charset enc) {
    return String.valueOf(decode((CharSequence) s, enc));
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
   * @param s the {@code CharSequence} to decode
   * @param enc   The name of a supported
   *    <a href="../lang/package-summary.html#charenc">character
   *    encoding</a>.
   * @return the newly decoded {@linkplain CharSequence}. Note that if a conversion was required, the
   *    returned {@linkplain CharSequence} is shared thread local and that the caller is required to copy it.
   * @see java.net.URLDecoder#decode(java.lang.String, java.lang.String)
   */
  public static @Nonnull CharSequence decode(@Nonnull CharSequence s, @Nonnull Charset enc) {
    boolean needToChange = false;
    int numChars = s.length();
    StringBuilder sb = STRING_BUILDER.get();
    sb.setLength(0);
    sb.ensureCapacity(numChars > 500 ? numChars / 2 : numChars);
    int i = 0;

    char c;
    byte[] bytes = null;
    while (i < numChars) {
      c = s.charAt(i);
      switch (c) {
        case '+':
          sb.append(' ');
          i++;
          needToChange = true;
          break;
        case '%': {
          /*
           * Starting with this instance of %, process all
           * consecutive substrings of the form %xy. Each
           * substring %xy will yield a byte. Convert all
           * consecutive  bytes obtained this way to whatever
           * character(s) they represent in the provided
           * encoding.
           */

          // (numChars-i)/3 is an upper bound for the number
          // of remaining bytes
          if (bytes == null) {
            bytes = new byte[(numChars - i) / 3];
          }
          int pos = 0;

          while (((i + 2) < numChars) && (c == '%')) {
            int v = parseHex(s, i + 1);
            if (v < 0) {
              throw new IllegalArgumentException("URLDecoder: Illegal hex characters in escape (%) pattern");
            }
            bytes[pos++] = (byte) v;
            i += 3;
            if (i < numChars) {
              c = s.charAt(i);
            }
          }

          // A trailing, incomplete byte encoding such as
          // "%x" will cause an exception to be thrown

          if ((i < numChars) && (c == '%')) {
            throw new IllegalArgumentException("URLDecoder: Incomplete trailing escape (%) pattern");
          }

          append(sb, ByteBuffer.wrap(bytes, 0, pos), enc);
        }
          needToChange = true;
          break;
        default:
          sb.append(c);
          i++;
          break;
      }
    }

    return (needToChange ? sb : s);
  }

  private static final ThreadLocal<Map<Charset, CharsetDecoder>> DECODER_CACHE = ThreadLocal.withInitial(HashMap::new);

  private static void append(StringBuilder sb, ByteBuffer in, Charset enc) {
    if (in.remaining() == 0) {
      return;
    }

    // Fast path for US_ASCII
    if (enc == StandardCharsets.US_ASCII) {
      sb.ensureCapacity(in.remaining());
      while (in.hasRemaining()) {
        sb.append((char) in.get());
      }
      return;
    }

    CharBuffer out = CHAR_BUFFER_THREAD_LOCAL.get();
    out.clear();

    // Java 9+ does not let us access the sun.* package so we must maintain our own thread local.
    /* we are using this internal implementation to avoid constructing new CharsetDecoder instances */
    // CharsetDecoder decoder = sun.nio.cs.ThreadLocalCoders.decoderFor(enc);
    CharsetDecoder decoder = DECODER_CACHE.get().computeIfAbsent(enc, Charset::newDecoder);
    decoder.reset();

    sb.ensureCapacity((int) (in.remaining() * decoder.averageCharsPerByte()));

    decoder.onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT);

    while (true) {
      CoderResult cr = in.hasRemaining() ? decoder.decode(in, out, true) : CoderResult.UNDERFLOW;

      if (cr.isUnderflow()) {
        cr = decoder.flush(out);
      }

      out.flip();
      sb.append(out, 0, out.length());
      out.clear();

      if (cr.isUnderflow()) {
        return;
      }

      if (cr.isMalformed()) {
        throw new IllegalArgumentException("URLCodec: Malformed input");
      }

      if (cr.isUnmappable()) {
        throw new IllegalArgumentException("URLCodec: Unmappable character");
      }

      assert cr.isOverflow();
    }
  }

  private static final short[] UNHEX_HIGH_NIBBLE = { -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
      -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
      -1, -1, -1, 0, 0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80, 0x90, -1, -1, -1, -1, -1, -1, -1, 0xa0, 0xb0, 0xc0,
      0xd0, 0xe0, 0xf0, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
      -1, -1, 0xa0, 0xb0, 0xc0, 0xd0, 0xe0, 0xf0, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
      -1, -1, -1, -1, -1, -1, -1, -1 };

  private static final short[] UNHEX_LOW_NIBBLE = { -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
      -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
      -1, -1, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1, -1, -1, -1, -1, -1, -1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1,
      -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 10, 11, 12, 13, 14, 15, -1,
      -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1 };

  /**
   * Table-driven hex to binary conversion is fast and does not require any heap.
   *
   * In order to convert a simple ASCII Hexadecimal value for a byte into binary form,
   * it could be done with simple operations and two small Look-Up Tables. One table for
   * the Most Significant Nibble (4-bits), represented by the first Hexadecimal digit,
   * another table for the Least Significant Nibble, represented by the second Hexadecimal digit
   * and the result of the conversion was simply the OR product from the two tables.
   *
   * For all values which are not a valid Hexadecimal ASCII digit, the value is set to -1 which
   * ensures that the OR product of the two tables would always result in a -1 value if in the
   * place of either digit, an invalid character was used.
  
   * @param s CharSequence
   * @param pos position of first (high) nibble
   * @return hex byte ({@literal 0 - 0xff}) or invalid ({@literal -1}).
   */
  private static int parseHex(CharSequence s, int pos) {
    char highNibble = s.charAt(pos);
    char lowNibble = s.charAt(pos + 1);
    return ((highNibble | lowNibble) & ~0xff) == 0
        ? UNHEX_HIGH_NIBBLE[0xff & highNibble] | UNHEX_LOW_NIBBLE[0xff & lowNibble]
        : -1;
  }
}

package com.linkedin.alpini.base.misc;

import com.linkedin.alpini.base.hash.HashFunction;
import com.linkedin.alpini.base.hash.JenkinsHashFunction;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.text.DateFormatSymbols;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PrimitiveIterator;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public enum HeaderUtils {
  SINGLETON; // Effective Java, Item 3 - Enum singleton

  public static final String HTTP_DATE_FORMAT = "EEE, dd MMM yyyy HH:mm:ss zzz";
  public static final String HTTP_DATE_GMT_TIMEZONE = "GMT";

  private static final DateFormatSymbols DATE_FORMAT_SYMBOLS = DateFormatSymbols.getInstance(Locale.US);
  private static final ThreadLocal<SimpleDateFormat> DATE_FORMAT_INSTANCE = ThreadLocal.withInitial(() -> {
    SimpleDateFormat dateFormatter = new SimpleDateFormat(HTTP_DATE_FORMAT, DATE_FORMAT_SYMBOLS);
    dateFormatter.setTimeZone(TimeZone.getTimeZone(HTTP_DATE_GMT_TIMEZONE));
    return dateFormatter;
  });

  private static final char[] HEX_CHAR =
      { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

  public interface ContentType {
    boolean isMultipart();

    String type();

    String subType();

    Iterator<Map.Entry<String, String>> parameters();
  }

  /**
   * From <a href="https://www.w3.org/Protocols/rfc1341/4_Content-Type.html">RFC1341</a>,
   * a token must be formed from one or more characters except space, control characters of tspecials.
   */
  private static boolean isToken(String token) {
    PrimitiveIterator.OfInt it = token.codePoints().iterator();
    if (!it.hasNext()) {
      return false;
    }
    do {
      int codePoint = it.nextInt();
      if (!isTokenCharacter(codePoint)) {
        return false;
      }
    } while (it.hasNext());
    return true;
  }

  private static boolean isTokenCharacter(int codePoint) {
    if (codePoint < 0x21 || codePoint > 0x7e) {
      return false;
    }
    switch (codePoint) {
      case '(':
      case ')':
      case '<':
      case '>':
      case '[':
      case ']':
      case '@':
      case ',':
      case ';':
      case ':':
      case '\\':
      case '"':
      case '/':
      case '?':
      case '=':
      case '.':
        return false;
      default:
    }
    return true;
  }

  /**
   * From <a href="https://www.w3.org/Protocols/rfc1341/4_Content-Type.html">RFC1341</a>,
   * a quoted value may contain any character except control characters or {@literal '"'}.
   */
  public static boolean isQuotableParam(String value) {
    PrimitiveIterator.OfInt it = value.codePoints().iterator();
    int prev = 0;
    while (it.hasNext()) {
      int codepoint = it.nextInt();
      if (prev == 10 && !(codepoint == '\t' || codepoint == ' ')) {
        return false;
      }
      if (codepoint == '"' || codepoint == '\\' || codepoint > 0x7f) {
        return false;
      }
      prev = codepoint;
    }
    return prev != 10;
  }

  private static String paramToken(String token) {
    if (isToken(token)) {
      return token;
    }
    throw new IllegalArgumentException("Parameter name is not a token: " + token);
  }

  public static String buildContentType(String type, String subType, Collection<Map.Entry<String, String>> parameters) {
    return buildContentType(type, subType, parameters != null ? parameters.iterator() : Collections.emptyIterator());
  }

  public static String buildContentType(String type, String subType, Iterator<Map.Entry<String, String>> parameters) {
    if (!isToken(Objects.requireNonNull(type, "type")) || !isToken(Objects.requireNonNull(subType, "subType"))) {
      throw new IllegalArgumentException("Not a valid token");
    }
    if (parameters.hasNext()) {
      List<Map.Entry<String, String>> params = CollectionUtil.stream(parameters).collect(Collectors.toList());
      if (!params.isEmpty()) {
        int estimate = params.stream().mapToInt(e -> e.getKey().length() + e.getValue().length() + 5).sum();
        StringBuilder builder = new StringBuilder(type.length() + 1 + subType.length() + estimate);

        builder.append(type).append('/').append(subType);
        params.forEach(e -> {
          builder.append("; ").append(paramToken(e.getKey())).append('=');
          if (isToken(e.getValue())) {
            builder.append(e.getValue());
          } else if (isQuotableParam(e.getValue())) {
            builder.append('"').append(e.getValue()).append('"');
          } else {
            throw new IllegalArgumentException(
                "Parameter value for " + e.getKey() + " contains illegal characters: " + e.getValue());
          }
        });

        return builder.toString();
      }
    }

    return type + "/" + subType;
  }

  public static ContentType parseContentType(String contentType) {
    int slash = contentType.indexOf('/');
    if (slash < 0) {
      throw new IllegalArgumentException("Invalid content type: " + contentType);
    }
    boolean multipart = contentType.startsWith("multipart/");
    int semiColon = contentType.indexOf(';', slash);
    if (semiColon < 0) {
      return new ContentType() {
        @Override
        public boolean isMultipart() {
          return multipart;
        }

        @Override
        public String type() {
          return contentType.substring(0, slash);
        }

        @Override
        public String subType() {
          return contentType.substring(slash + 1);
        }

        @Override
        public Iterator<Map.Entry<String, String>> parameters() {
          return Collections.<Map.Entry<String, String>>emptyList().iterator();
        }

        @Override
        public String toString() {
          return contentType;
        }
      };
    } else {
      return new ContentType() {
        @Override
        public boolean isMultipart() {
          return multipart;
        }

        @Override
        public String type() {
          return contentType.substring(0, slash);
        }

        @Override
        public String subType() {
          return contentType.substring(slash + 1, semiColon).trim();
        }

        /**
         * From <a href="https://www.w3.org/Protocols/rfc1341/4_Content-Type.html">RFC1341</a>,
         * a parameter is semicolon separated and is the attribute name which is a token, followed
         * by an {@literal '='}, and then the value which may either be a valid token or a valid
         * quoted string.
         */
        @Override
        public Iterator<Map.Entry<String, String>> parameters() {
          PrimitiveIterator.OfInt it = contentType.codePoints().iterator();
          return new Iterator<Map.Entry<String, String>>() {
            private Map.Entry<String, String> found;
            private int codepoint;
            private final StringBuilder key = new StringBuilder();
            private final StringBuilder value = new StringBuilder();

            @Override
            public boolean hasNext() {
              while (found == null && it.hasNext()) {
                while (codepoint != ';' && it.hasNext()) {
                  codepoint = it.nextInt();
                }
                if (it.hasNext()) {
                  codepoint = it.nextInt();
                  while (Character.isWhitespace(codepoint) && it.hasNext()) {
                    codepoint = it.nextInt();
                  }
                  while (isTokenCharacter(codepoint) && it.hasNext()) {
                    key.append((char) codepoint);
                    codepoint = it.nextInt();
                  }
                  if (codepoint == '=' && it.hasNext()) {
                    codepoint = it.nextInt();
                    if (codepoint == '"' && it.hasNext()) {
                      codepoint = it.nextInt();
                      while (codepoint != '"' && it.hasNext()) {
                        value.append((char) codepoint);
                        codepoint = it.nextInt();
                      }
                      if (codepoint == '"' && isQuotableParam(makeFound().getValue())) {
                        return true;
                      }
                    } else if (isTokenCharacter(codepoint)) {
                      do {
                        value.append((char) codepoint);
                        if (!it.hasNext()) {
                          break;
                        }
                        codepoint = it.nextInt();
                      } while (isTokenCharacter(codepoint));
                      makeFound();
                      return true;
                    }
                  }
                  clear();
                }
              }
              return found != null;
            }

            private Map.Entry<String, String> makeFound() {
              Map.Entry<String, String> found = ImmutableMapEntry.make(key.toString(), value.toString());
              clear();
              this.found = found;
              return found;
            }

            private void clear() {
              key.setLength(0);
              value.setLength(0);
              found = null;
            }

            @Override
            public Map.Entry<String, String> next() {
              if (hasNext()) {
                Map.Entry<String, String> next = found;
                found = null;
                return next;
              }
              throw new NoSuchElementException();
            }
          };
        }

        @Override
        public String toString() {
          return contentType;
        }
      };
    }
  }

  public static String formatDate(Date date) {
    if (date != null) {
      return DATE_FORMAT_INSTANCE.get().format(date);
    } else {
      return null;
    }
  }

  /**
   * Builds a new String, filtering out non-ASCII characters which are likely to
   * cause problems.
   * @param value Status message.
   * @return filtered message.
   */
  public static @Nonnull CharSequence sanitizeStatusMessage(@Nonnull CharSequence value) {
    StringBuilder sb = new StringBuilder(value.length());
    for (PrimitiveIterator.OfInt it = value.chars().iterator(); it.hasNext();) {
      char c = (char) it.nextInt();
      if (c == '\r' || c == '\n') {
        break;
      }
      if (c >= ' ' && c <= 0x7f) {
        sb.append(c);
      } else {
        sb.append(' ');
      }
    }
    return sb;
  }

  /**
   * Cleans the status message and if no cleanup is required, returns the same String object.
   * @param value Status message.
   * @return filtered message.
   */
  public static @Nonnull CharSequence cleanStatusMessage(CharSequence value) {
    if (value == null) {
      return "null";
    }
    for (int i = 0; i < value.length(); i++) {
      int c = 0xff & value.charAt(i);
      if (c < ' ' || c > 0x7f) {
        return sanitizeStatusMessage(value);
      }
    }
    return value;
  }

  /**
   * Check if the character is prohibited in a HTTP header value.
   */
  public static boolean isProhibitedCharacter(int c) {
    return c == 0 || c == 0x0b || c == '\f' || c == '%' || c > 0x7f || c == '\r' || c == '\n';
  }

  public static @Nonnull StringBuilder appendHex(@Nonnull StringBuilder sb, int c) {
    return sb.append(HEX_CHAR[0x0f & (c >> 4)]).append(HEX_CHAR[0x0f & c]);
  }

  /**
   * Builds a new String, filtering out values and performing fixup to provide
   * a value which may be set as a HTTP header value. The implementation of
   * this method is based upon the value validation code in Netty 3.7.1
   * @param value header value.
   * @return filtered value.
   */
  public static @Nonnull CharSequence sanitizeHeaderValue(@Nonnull CharSequence value) {
    return sanitizeHeaderValue(value.codePoints().toArray());
  }

  private static @Nonnull CharSequence sanitizeHeaderValue(@Nonnull int[] codePoints) {
    CharsetEncoder encoder = StandardCharsets.UTF_8.newEncoder()
        .onMalformedInput(CodingErrorAction.REPLACE)
        .onUnmappableCharacter(CodingErrorAction.REPLACE);
    ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[(int) (encoder.maxBytesPerChar() + 1f)]);
    StringBuilder sb = new StringBuilder((int) (codePoints.length * encoder.averageBytesPerChar() * 1.2f + 1f));

    for (int c: codePoints) {
      if (isProhibitedCharacter(c)) {
        if (c <= 0x7f) {
          appendHex(sb.append('%'), c);
        } else {
          encoder.encode(CharBuffer.wrap(Character.toChars(c)), byteBuffer, true);
          byteBuffer.flip();
          while (byteBuffer.hasRemaining()) {
            int ch = 0xff & byteBuffer.get();
            appendHex(sb.append('%'), ch);
          }
          byteBuffer.clear();
          encoder.reset();
        }
      } else {
        sb.append((char) c);
      }
    }
    return sb;
  }

  /**
   * Checks to ensure that a provided header value may be used as-is or returns a new
   * string which has been appropiately filtered. The implementation of this method is
   * based upon the value validation code in Netty 3.7.1
   * @param value header value.
   * @return filtered value.
   */
  public static @Nonnull CharSequence cleanHeaderValue(CharSequence value) {
    if (value == null || value.length() == 0) {
      return "";
    }

    // 0 - the previous character was neither CR nor LF
    // 1 - the previous character was CR
    // 2 - the previous character was LF
    int state = 0;

    final int[] codePoints = value.codePoints().toArray();
    for (int c: codePoints) {
      // Check the absolutely prohibited characters.
      if (c < ' ' && c != '\r' && c != '\n' && c != '\t') {
        return sanitizeHeaderValue(codePoints);
      }

      // check if character is not ASCII
      if (0x7f < c) {
        return sanitizeHeaderValue(codePoints);
      }

      // Check the CRLF (HT | SP) pattern
      state: switch (state) {
        case 0:
          switch (c) {
            case '\r':
              state = 1;
              break state;
            case '\n':
              state = 2;
              break state;
            default:
              break;
          }
          break;
        case 1:
          if (c == '\n') {
            state = 2;
            break;
          }
          return sanitizeHeaderValue(codePoints);
        case 2:
          switch (c) {
            case '\t':
            case ' ':
              state = 0;
              break state;
            default:
              break;
          }
          return sanitizeHeaderValue(codePoints);
        default:
          break;
      }
    }

    if (state != 0) {
      return sanitizeHeaderValue(codePoints);
    }
    return value;
  }

  public static StringBuilder escapeDelimiters(StringBuilder buffer, CharSequence text) {
    int length = text.length();

    for (int x = 0; x < length; x++) {
      int c = text.charAt(x) & 0xff;

      if (isDelimiter(c)) {
        return escapeDelimiters(buffer, text, x);
      }
    }

    return buffer.append(text);
  }

  private static boolean isDelimiter(int c) {
    return (c == '[' || c == ']' || c == '(' || c == ')' || c == '{' || c == '}' || c == ',');
  }

  private static StringBuilder escapeDelimiters(StringBuilder buffer, CharSequence text, int startIndex) {
    int length = text.length();
    buffer.ensureCapacity(length * 2);

    buffer.append(text, 0, startIndex);

    text.chars().skip(startIndex).forEach(c -> {
      if (isDelimiter(c & 0xff)) {
        appendHex(buffer.append('%'), c);
      } else {
        buffer.append((char) c);
      }
    });
    return buffer;
  }

  /**
   * Static factory to retrieve a type 4 (pseudo randomly generated) UUID.
   *
   * The {@code UUID} is generated using a cryptographically weak pseudo
   * random number generator - the {@link ThreadLocalRandom} generator.
   *
   * @return  A randomly generated {@code UUID}
   */
  public static UUID randomWeakUUID() {
    ThreadLocalRandom ng = ThreadLocalRandom.current();

    byte[] randomBytes = new byte[16];
    ng.nextBytes(randomBytes);
    randomBytes[6] &= 0x0f; /* clear version        */
    randomBytes[6] |= 0x40; /* set to version 4     */
    randomBytes[8] &= 0x3f; /* clear variant        */
    randomBytes[8] |= 0x80; /* set to IETF variant  */

    return fromBytes(randomBytes);
  }

  /**
   * Static factory to retrieve a type 4 (pseudo-random) {@code UUID} based on
   * the specified byte array after transformation with the Jenkins hash function.
   *
   * @param  name
   *         A byte array to be used to construct a {@code UUID}
   *
   * @return  A {@code UUID} generated from the specified array
   */
  public static UUID nameUUIDFromBytes(byte[] name) {
    // Jenkins is an "ok" hash, not as good as MD5 as required by IETF Version 3 UUIDs but meh. It's faster.
    HashFunction function = new JenkinsHashFunction();
    byte[] bytes = new byte[16];
    ByteBuffer tmp = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
    tmp.putLong(0, function.hash(ByteBuffer.wrap(name)));

    byte[] tmp2 = new byte[name.length + 2];
    tmp2[0] = 1;
    tmp2[1] = (byte) -name.length;
    System.arraycopy(name, 0, tmp2, 2, name.length);

    tmp.putLong(8, function.hash(ByteBuffer.wrap(tmp2)));

    bytes[6] &= 0x0f; /* clear version        */
    bytes[6] |= 0x40; /* set to version 4     */
    bytes[8] &= 0x3f; /* clear variant        */
    bytes[8] |= 0x80; /* set to IETF variant  */

    return fromBytes(bytes);
  }

  private static UUID fromBytes(byte[] bytes) {
    ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
    return new UUID(buf.getLong(0), buf.getLong(8));
  }
}

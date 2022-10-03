package com.linkedin.alpini.netty4.compression;

import io.netty.channel.ChannelHandler;
import io.netty.handler.codec.compression.SnappyFrameDecoder;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.util.AsciiString;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class CompressionUtils {
  private static final Logger LOG = LogManager.getLogger(CompressionUtils.class);

  public static final String SNAPPY_ENCODING = "snappy";

  public static final String X_SNAPPY_ENCODING = "x-snappy";

  public static final String SNAPPY_FRAMED_ENCODING = "x-snappy-framed";

  public static final AsciiString SNAPPY = AsciiString.of(SNAPPY_ENCODING);

  public static final AsciiString X_SNAPPY = AsciiString.of(X_SNAPPY_ENCODING);

  /**
   * @see <a href="http://snappy.googlecode.com/svn/trunk/framing_format.txt">x-snappy-framed specification</a>
   */
  public static final AsciiString SNAPPY_FRAMED = AsciiString.of(SNAPPY_FRAMED_ENCODING);

  public static final AsciiString GZIP = HttpHeaderValues.GZIP;
  public static final AsciiString X_GZIP = HttpHeaderValues.X_GZIP;
  public static final AsciiString DEFLATE = HttpHeaderValues.DEFLATE;
  public static final AsciiString X_DEFLATE = HttpHeaderValues.X_DEFLATE;

  private CompressionUtils() {
  }

  public static ChannelHandler newSnappyDecoder() {
    return new SnappyDecoder();
  }

  public static ChannelHandler newSnappyFramedDecoder() {
    return new SnappyFrameDecoder();
  }

  private static final float ACCEPTS_THRESHOLD = 0.1f;

  // We cannot use repeated matches because Java's Regexp doesn't support storing repeated matches
  private static final Pattern ENCODING_REGEXP =
      Pattern.compile("(?:(?:\\A|,)\\s*([^;,]+)|;\\s*([^=;,]+?)(?:\\s*=\\s*([^;,]+)))");

  /**
   * Test if the specific encoding is permitted by the provided list of encodings
   * as used in HTTP {@code Accepts-Encoding} header.
   * @param encoding specific encoding name.
   * @param acceptableEncodings Encoding string as used in HTTP {@code Accepts-Encoding} header.
   * @return {@code true} if the specific encoding is permitted by the {@code acceptableEncodings} list.
   */
  public static boolean isCompatibleEncoding(CharSequence encoding, CharSequence acceptableEncodings) {
    boolean compatible = true;
    if (encoding != null) {
      if (acceptableEncodings == null) {
        return HttpHeaderValues.IDENTITY.contentEquals(encoding);
      }

      String accepts = null;
      Matcher matcher = ENCODING_REGEXP.matcher(acceptableEncodings);
      boolean match;
      boolean acceptable = false;

      while ((match = matcher.find()) || accepts != null) { // SUPPRESS CHECKSTYLE InnerAssignment
        if (!match || matcher.group(1) != null) {
          if (acceptable) {
            return true;
          }
          accepts = null;
          if (match) {
            accepts = matcher.group(1).trim();
            acceptable = encoding.equals(accepts);
            compatible &= acceptable;
          }
        } else if (acceptable && "q".equals(matcher.group(2))) {
          try {
            return Float.parseFloat(matcher.group(3).trim()) >= ACCEPTS_THRESHOLD;
          } catch (Throwable ex) {
            return false;
          }
        }
      }
    }
    return compatible;
  }

  /**
   * Test if the specified {@code encoding} names a specific supported encoding.
   * @param encoding encoding name
   * @return {@code true} if {@code encoding} matches a specific supported implementation.
   */
  public static boolean isSupportedSpecificEncoding(CharSequence encoding) {
    return SNAPPY.contentEquals(encoding) || SNAPPY_FRAMED.contentEquals(encoding) || GZIP.contentEquals(encoding)
        || DEFLATE.contentEquals(encoding) || HttpHeaderValues.IDENTITY.contentEquals(encoding);
  }

  /**
   * Test if the specified {@code encoding} names an encoding which may be streamed.
   * @param encoding encoding name
   * @return {@code true} if {@code encoding} matches a known streaming encoding.
   */
  public static boolean isStreamingEncoding(CharSequence encoding) {
    return SNAPPY_FRAMED.contentEquals(encoding) || GZIP.contentEquals(encoding) || DEFLATE.contentEquals(encoding)
        || HttpHeaderValues.IDENTITY.contentEquals(encoding);
  }

  /**
   * Test if the supplied {@code Accepts-Encoding} string matches any specific
   * supported encoding.
   * @param encoding Encoding string as used in HTTP {@code Accepts-Encoding} header.
   * @return {@code true} if {@code encoding} matches a supported encoding.
   */
  public static boolean isSupportedEncoding(CharSequence encoding) {
    if (encoding != null) {
      String accepts = null;
      Matcher matcher = ENCODING_REGEXP.matcher(encoding);
      boolean match;
      boolean acceptable = false;

      while ((match = matcher.find()) || accepts != null) { // SUPPRESS CHECKSTYLE InnerAssignment
        if (!match || matcher.group(1) != null) {
          if (acceptable) {
            return true;
          }
          accepts = null;
          if (match) {
            accepts = matcher.group(1).trim();
            acceptable = isSupportedSpecificEncoding(accepts);
          }
        } else if (acceptable && "q".equals(matcher.group(2))) {
          try {
            acceptable = Float.parseFloat(matcher.group(3).trim()) >= ACCEPTS_THRESHOLD;
          } catch (Throwable ex) {
            acceptable = false;
          }
        }
      }
    }
    return false;
  }
}

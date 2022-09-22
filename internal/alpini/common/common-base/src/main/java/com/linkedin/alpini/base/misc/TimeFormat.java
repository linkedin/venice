/*
 * $Id$
 */
package com.linkedin.alpini.base.misc;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * This utility class provides methods to format and parse a timespan in a human readable form. The timespan is formatted broken down in days,
 * hours, minutes, seconds, and milliseconds. The time span 5 minutes and 23 seconds is displayed simply as "5m23s" instead of the millisecond value
 * "323000". A larger value might be displayed as "3d17h27m33s15ms".
 *
 * @author Jemiah Westerman &lt;jwesterman@linkedin.com&gt;
 * @version $Revision$
 */
public enum TimeFormat {
  SINGLETON; // Effective Java, Item 3 - Enum singleton

  private static final long[] DURATION_MILLIS = { 1, 1000, 60 * 1000, 60 * 60 * 1000, 24 * 60 * 60 * 1000 };
  private static final String[] DURATION_DISPLAY = { "ms", "s", "m", "h", "d" };
  private static final Pattern PARSE_PATTERN =
      Pattern.compile("(-)?([0-9]+)?([a-z]+)?([0-9]+)?([a-z]+)?([0-9]+)?([a-z]+)?([0-9]+)?([a-z]+)?([0-9]+)?([a-z]+)?");
  private static final String DATE_FORMAT = "yyyy/MM/dd HH:mm:ss.SSS";
  private static final ThreadLocal<DateFormat> THREAD_LOCAL_DATE_FORMAT =
      ThreadLocal.withInitial(() -> new SimpleDateFormat(DATE_FORMAT));

  /**
   * Parse the given String date and time to a millisecond value. The string must be in the form returned by
   * {@link #formatDatetime(long)} or {@link #formatDatetime(Date)}.
   * @param s date time to parse - like "2012/04/05 14:38:03.311"
   * @return the date and time, in milliseconds
   * @throws IllegalArgumentException if s is invalid
   */
  public static long parseDatetimeToMillis(String s) throws IllegalArgumentException {
    return parseDatetimeToDate(s).getTime();
  }

  /**
   * Parse the given String date and time to a millisecond value. The string must be in the form returned by
   * {@link #formatDatetime(long)} or {@link #formatDatetime(Date)}.
   * @param s date time to parse - like "2012/04/05 14:38:03.311"
   * @return the date and time
   * @throws IllegalArgumentException if s is invalid
   */
  public static Date parseDatetimeToDate(String s) throws IllegalArgumentException {
    if (s == null) {
      throw new IllegalArgumentException();
    }
    DateFormat dateFormat = getDateFormatInstance();
    try {
      return dateFormat.parse(s);
    } catch (ParseException ex) {
      throw new IllegalArgumentException(s);
    }
  }

  /**
   * Format the given date in a human readable form.
   * @param date the date
   * @return a human readable date, such as "2012/04/05 14:38:03.311".
   */
  public static String formatDatetime(Date date) {
    DateFormat dateFormat = getDateFormatInstance();
    return dateFormat.format(date);
  }

  /**
   * Format the given date in a human readable form.
   * @param datetime the date and time, in milliseconds
   * @return a human readable date, such as "2012/04/05 14:38:03.311".
   */
  public static String formatDatetime(long datetime) {
    return formatDatetime(new Date(datetime));
  }

  /**
   * Parse the given String timespan to a millisecond value. The string must be in the form returned by
   * {@link #formatTimespan(long)}. In particular,
   * zero time should be represented as "0ms", not as "0" or "".
   *
   * @param s timespan to parse - like 3d17h27m33s15ms
   * @return the number of milliseconds
   * @throws IllegalArgumentException if the timespan is invalid
   */
  public static long parseTimespanToMillis(String s) throws IllegalArgumentException {
    if (s == null || s.length() == 0) {
      throw new IllegalArgumentException("null");
    }

    Matcher m = PARSE_PATTERN.matcher(s);
    if (m.matches()) {
      long millis = 0;
      for (int i = 2; i < m.groupCount(); i += 2) {
        // Groups should always be in pairs - a numeric value and a string display. For example "1ms" becomes "1" "ms".
        // If either part is null, then the input string was bad.
        String value = m.group(i);
        String display = m.group(i + 1);
        if (value == null && display == null) {
          break;
        } else if (value == null || display == null) {
          throw new IllegalArgumentException(s);
        }

        // Look for the display characters ("s", "ms", etc.) in the DURATION_DISPLAY array. If we don't find it, then it
        // was invalid.
        // If we do find it, then parse value and add the proper number of milliseconds to the running millis
        boolean isValid = false;
        for (int j = 0; j < DURATION_DISPLAY.length; j++) {
          if (DURATION_DISPLAY[j].equals(display)) {
            try {
              millis += DURATION_MILLIS[j] * Integer.parseInt(value);
            } catch (NumberFormatException ex) {
              throw new IllegalArgumentException(s);
            }
            isValid = true;
            break;
          }
        }
        if (!isValid) {
          throw new IllegalArgumentException(s);
        }
      }

      // negative symbol?
      if (m.group(1) != null) {
        millis *= -1;
      }
      return millis;
    } else {
      // regex did not match
      throw new IllegalArgumentException(s);
    }
  }

  /**
   * Format the given number of milliseconds in a more human readable form.
   * @param millis number of milliseconds
   * @return a human readable duration, such as "3d17h27m33s15ms".
   */
  public static String formatTimespan(long millis) {
    StringBuilder sb = new StringBuilder();
    formatTimespan(millis, sb);
    return sb.toString();
  }

  /**
   * Format the given number of milliseconds in a more human readable form and append it to the given StringBuilder.
   * @param millis number of milliseconds
   * @param sb append the formatted timestamp here
   */
  public static void formatTimespan(long millis, StringBuilder sb) {
    // Zero is a special case. Normally we don't display the 0 values (1 second is just "1s", not "1s0ms").
    // But for zero time, we actually want to display "0ms" instead of "".
    if (millis == 0) {
      sb.append("0").append(DURATION_DISPLAY[0]);
      return;
    }

    if (millis < 0) {
      // Negative duration?
      millis *= -1;
      sb.append("-");
    }

    // Build the pretty-print string
    for (int i = DURATION_MILLIS.length - 1; i >= 0; i--) {
      if (millis >= DURATION_MILLIS[i]) {
        long v = millis / DURATION_MILLIS[i];
        sb.append(v).append(DURATION_DISPLAY[i]);
        millis -= v * DURATION_MILLIS[i];
      }
    }
  }

  /**
   * Returns a DateFormat instance which is cached in a ThreadLocal.
   */
  private static DateFormat getDateFormatInstance() {
    return THREAD_LOCAL_DATE_FORMAT.get();
  }
}

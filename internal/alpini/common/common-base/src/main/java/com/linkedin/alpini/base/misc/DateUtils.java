package com.linkedin.alpini.base.misc;

import java.text.DateFormatSymbols;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import javax.annotation.Nonnull;


public enum DateUtils {
  SINGLETON; // Effective Java, Item 3 - Enum singleton

  private static final DateFormatSymbols DATE_FORMAT_SYMBOLS = DateFormatSymbols.getInstance(Locale.US);
  private static final TimeZone HTTP_DATE_GMT_TIMEZONE = TimeZone.getTimeZone("GMT");
  private static final Date DEFAULT_TWO_DIGIT_YEAR_START = get2DigitYearStart();

  public static final String RFC1123_DATE_FORMAT = "EEE, dd MMM yyyy HH:mm:ss zzz";
  public static final String RFC1036_DATE_FORMAT = "EEE, dd-MMM-yy HH:mm:ss zzz";
  public static final String ASCTIME_DATE_FORMAT = "EEE MMM d HH:mm:ss yyyy";

  private static final ThreadLocal<SimpleDateFormat> RFC1123_FORMATTER = dateFormatFor(RFC1123_DATE_FORMAT);
  private static final ThreadLocal<SimpleDateFormat> RFC1036_FORMATTER = dateFormatFor(RFC1036_DATE_FORMAT);
  private static final ThreadLocal<SimpleDateFormat> ASCTIME_FORMATTER = dateFormatFor(ASCTIME_DATE_FORMAT);

  private static final List<ThreadLocal<SimpleDateFormat>> PARSE_LIST =
      Arrays.asList(RFC1123_FORMATTER, RFC1036_FORMATTER, ASCTIME_FORMATTER);

  private static @Nonnull Date get2DigitYearStart() {
    final Calendar calendar = Calendar.getInstance();
    calendar.setTimeZone(HTTP_DATE_GMT_TIMEZONE);
    calendar.set(2000, Calendar.JANUARY, 1, 0, 0, 0);
    calendar.set(Calendar.MILLISECOND, 0);
    return calendar.getTime();
  }

  private static @Nonnull ThreadLocal<SimpleDateFormat> dateFormatFor(@Nonnull String dateFormatString) {
    return ThreadLocal.withInitial(() -> {
      SimpleDateFormat dateFormat = new SimpleDateFormat(dateFormatString, DATE_FORMAT_SYMBOLS);
      dateFormat.setTimeZone(HTTP_DATE_GMT_TIMEZONE);
      dateFormat.set2DigitYearStart(DEFAULT_TWO_DIGIT_YEAR_START);
      return dateFormat;
    });
  }

  public static @Nonnull String getRFC1123Date(long timeMillis) {
    return getRFC1123Date(new Date(timeMillis));
  }

  public static @Nonnull String getRFC1123Date(@Nonnull Date date) {
    return formatDate(RFC1123_FORMATTER, date);
  }

  public static @Nonnull String getRFC1036Date(long timeMillis) {
    return getRFC1036Date(new Date(timeMillis));
  }

  public static @Nonnull String getRFC1036Date(@Nonnull Date date) {
    return formatDate(RFC1036_FORMATTER, date);
  }

  public static @Nonnull String getAnsiCDate(long timeMillis) {
    return getAnsiCDate(new Date(timeMillis));
  }

  public static @Nonnull String getAnsiCDate(@Nonnull Date date) {
    return formatDate(ASCTIME_FORMATTER, date);
  }

  private static @Nonnull String formatDate(
      @Nonnull ThreadLocal<SimpleDateFormat> localDateFormat,
      @Nonnull Date date) {
    SimpleDateFormat dateFormat = localDateFormat.get();
    dateFormat.setTimeZone(HTTP_DATE_GMT_TIMEZONE);
    return dateFormat.format(date);
  }

  private static String trimDate(String dateValue) {
    // trim single quotes around date if present
    if (dateValue.length() > 1 && dateValue.startsWith("'") && dateValue.endsWith("'")) {
      dateValue = dateValue.substring(1, dateValue.length() - 1);
    }
    return dateValue.trim();
  }

  public static Date parseRFC1123Date(@Nonnull String dateValue) {
    dateValue = trimDate(dateValue);
    final ParsePosition pos = new ParsePosition(0);
    final Date result = RFC1123_FORMATTER.get().parse(dateValue, pos);
    if (pos.getIndex() != dateValue.length()) {
      throw new IllegalArgumentException("Not an RFC1123 date. " + dateValue);
    }
    return result;
  }

  public static Date parseDate(@Nonnull String dateValue) {
    dateValue = trimDate(dateValue);
    for (final ThreadLocal<SimpleDateFormat> dateFormat: PARSE_LIST) {
      final SimpleDateFormat dateParser = dateFormat.get();
      dateParser.setTimeZone(HTTP_DATE_GMT_TIMEZONE);
      final ParsePosition pos = new ParsePosition(0);
      final Date result = dateParser.parse(dateValue, pos);
      if (pos.getIndex() == 0) {
        continue;
      }
      if (pos.getIndex() != dateValue.length()) {
        break;
      }
      return result;
    }

    throw new IllegalArgumentException("Not an RFC1123/RFC1036/ASCTIME date. " + dateValue);
  }

  /**
   * Splits a target string according to some substring (non-regex), which may be escaped in the target string.
   *
   * E.g. escapedSplit("\\", ":", "urn\\:li\\:test:val") returns ["urn\\:li\\:test", "val"]
   *
   * @param escape
   *  The escape sequence (if present in target, it must itself also be escaped)
   * @param split
   *  The substring on which to tokenize.
   * @param target
   *  The string to split
   * @return
   *  An array of tokens, split on a substring
   */
  public static String[] escapedSplit(String escape, String split, String target) {
    if (split == null || "".equals(split) || split.length() > target.length()) {
      throw new IllegalArgumentException("Invalid split string: " + split);
    }

    List<String> tokens = new ArrayList<>();

    StringBuilder acc = new StringBuilder();

    int i = 0;
    while (i < target.length()) {
      boolean isSplit =
          (i + split.length() <= target.length()) && target.substring(i, i + split.length()).equals(split);
      boolean isEscaped = (i - escape.length() >= 0) && target.substring(i - escape.length(), i).equals(escape);
      boolean escapeIsEscaped = (i - escape.length() * 2 >= 0)
          && target.substring(i - escape.length() * 2, i - escape.length()).equals(escape);

      if (isSplit && (!isEscaped || escapeIsEscaped)) {
        tokens.add(acc.toString());
        acc.setLength(0);
        i += split.length();
      } else {
        acc.append(target.charAt(i));
        i++;
      }
    }

    if (acc.length() > 0 || target.endsWith(split)) {
      tokens.add(acc.toString());
    }

    return tokens.toArray(new String[tokens.size()]);
  }
}

package com.linkedin.alpini.base.misc;

import io.netty.util.internal.PlatformDependent;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.util.StringBuilderFormattable;


public class LowerCaseAsciiCharSequence implements CharSequence, StringBuilderFormattable, Comparable<CharSequence> {
  private final CharSequence _charSequence;

  public LowerCaseAsciiCharSequence(@Nonnull CharSequence charSequence) {
    _charSequence = charSequence;
  }

  @Nonnull
  public static CharSequence toLowerCase(@Nonnull CharSequence charSequence) {
    for (int i = charSequence.length() - 1; i > 0; i--) {
      byte b = ByteBufAsciiString.c2b(charSequence.charAt(i));
      if (b >= 'A' && b <= 'Z') {
        return new LowerCaseAsciiCharSequence(charSequence);
      }
    }
    return charSequence;
  }

  @Override
  public int length() {
    return _charSequence.length();
  }

  @Override
  public char charAt(int index) {
    return ByteBufAsciiString.toLowerCase(_charSequence.charAt(index));
  }

  @Override
  public CharSequence subSequence(int start, int end) {
    CharSequence sub = _charSequence.subSequence(start, end);
    if (sub.length() < 1) {
      return "";
    }
    return new LowerCaseAsciiCharSequence(sub);
  }

  @Override
  public boolean equals(Object o) {
    return this == o || (o instanceof CharSequence && ByteBufAsciiString.contentEquals((CharSequence) o, this));
  }

  @Override
  public int hashCode() {
    return PlatformDependent.hashCodeAscii(this);
  }

  @Override
  @Nonnull
  public String toString() {
    StringBuilder sb = Msg.stringBuilder();
    formatTo(sb);
    return sb.toString();
  }

  @Override
  public void formatTo(StringBuilder buffer) {
    buffer.append(this);
  }

  @Override
  public int compareTo(@Nonnull CharSequence o) {
    if (this == o) {
      return 0;
    }

    int result;
    int length1 = length();
    int length2 = o.length();
    int minLength = Math.min(length1, length2);
    for (int i = 0; i < minLength; i++) {
      result = charAt(i) - o.charAt(i);
      if (result != 0) {
        return result;
      }
    }
    return length1 - length2;
  }
}

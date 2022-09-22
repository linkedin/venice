package com.linkedin.alpini.base.misc;

import static io.netty.util.internal.MathUtil.*;

import com.linkedin.alpini.netty4.misc.NettyUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.util.AsciiString;
import io.netty.util.ByteProcessor;
import io.netty.util.ReferenceCounted;
import io.netty.util.internal.InternalThreadLocalMap;
import io.netty.util.internal.PlatformDependent;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import javax.annotation.Nonnull;


/**
 * A string implementation, similar to {@linkplain AsciiString}, which is backed by a {@linkplain ByteBuf}
 * instead of a {@literal byte[]} array.
 */
public final class ByteBufAsciiString implements CharSequence, ReferenceCounted, Comparable<CharSequence> {
  public static final ByteBufAsciiString EMPTY_STRING = new ByteBufAsciiString(Unpooled.EMPTY_BUFFER, 0, "");
  private static final char MAX_CHAR_VALUE = 255;

  public static final int INDEX_NOT_FOUND = -1;

  private final ByteBuf _buf;
  private transient int _hash;
  private transient String _string;

  private ByteBufAsciiString(ByteBuf buf) {
    _buf = buf;
  }

  private ByteBufAsciiString(ByteBuf buf, int hash, String string) {
    _buf = buf;
    _hash = hash;
    _string = string;
  }

  public ByteBufAsciiString(@Nonnull ByteBufAllocator alloc, @Nonnull byte[] bytes) {
    _buf = alloc.buffer(bytes.length);
    _buf.writeBytes(bytes);
  }

  public ByteBufAsciiString(@Nonnull ByteBufAllocator alloc, @Nonnull CharSequence charSequence) {
    _buf = ByteBufUtil.writeAscii(alloc, charSequence);
    _string = charSequence instanceof String ? (String) charSequence : null;
  }

  /**
   * Create a copy of {@code value} into this instance assuming ASCII encoding.
   * The copy will start at index {@code start} and copy {@code length} bytes.
   */
  public ByteBufAsciiString(@Nonnull ByteBufAllocator alloc, @Nonnull CharSequence value, int start, int length) {
    if (isOutOfBounds(start, length, value.length())) {
      throw new IndexOutOfBoundsException(
          "expected: " + "0 <= start(" + start + ") <= start + length(" + length + ") <= " + "value.length("
              + value.length() + ')');
    }

    _buf = alloc.buffer(length);
    for (int i = 0, j = start; i < length; i++, j++) {
      _buf.writeByte(c2b(value.charAt(j)));
    }
  }

  /**
   * Create a copy of {@code value} into this instance assuming ASCII encoding.
   */
  public ByteBufAsciiString(@Nonnull ByteBufAllocator alloc, @Nonnull char[] value) {
    this(alloc, value, 0, value.length);
  }

  /**
   * Create a copy of {@code value} into this instance assuming ASCII encoding.
   * The copy will start at index {@code start} and copy {@code length} bytes.
   */
  public ByteBufAsciiString(@Nonnull ByteBufAllocator alloc, @Nonnull char[] value, int start, int length) {
    if (isOutOfBounds(start, length, value.length)) {
      throw new IndexOutOfBoundsException(
          "expected: " + "0 <= start(" + start + ") <= start + length(" + length + ") <= " + "value.length("
              + value.length + ')');
    }

    _buf = alloc.buffer(length);
    for (int i = 0, j = start; i < length; i++, j++) {
      _buf.writeByte(c2b(value[j]));
    }
  }

  /**
   * Create a copy of {@code value} into this instance using the encoding type of {@code charset}.
   */
  public ByteBufAsciiString(@Nonnull ByteBufAllocator alloc, @Nonnull char[] value, Charset charset) {
    this(alloc, value, charset, 0, value.length);
  }

  /**
   * Create a copy of {@code value} into a this instance using the encoding type of {@code charset}.
   * The copy will start at index {@code start} and copy {@code length} bytes.
   */
  public ByteBufAsciiString(
      @Nonnull ByteBufAllocator alloc,
      @Nonnull char[] value,
      Charset charset,
      int start,
      int length) {
    this(alloc, CharBuffer.wrap(value, start, length), charset, length);
  }

  /**
   * Create a copy of {@code value} into this instance using the encoding type of {@code charset}.
   */
  public ByteBufAsciiString(@Nonnull ByteBufAllocator alloc, @Nonnull CharSequence value, Charset charset) {
    this(alloc, value, charset, 0, value.length());
  }

  /**
   * Create a copy of {@code value} into this instance using the encoding type of {@code charset}.
   * The copy will start at index {@code start} and copy {@code length} bytes.
   */
  public ByteBufAsciiString(
      @Nonnull ByteBufAllocator alloc,
      @Nonnull CharSequence value,
      Charset charset,
      int start,
      int length) {
    this(alloc, CharBuffer.wrap(value, start, start + length), charset, length);
  }

  private ByteBufAsciiString(@Nonnull ByteBufAllocator alloc, @Nonnull CharBuffer cbuf, Charset charset, int length) {
    _buf = ByteBufUtil.encodeString(alloc, cbuf, charset);
  }

  public ByteBufAsciiString(byte[] bytes, int offset, int length, boolean copy) {
    _buf = copy ? Unpooled.copiedBuffer(bytes, offset, length) : Unpooled.wrappedBuffer(bytes, offset, length);
  }

  public static ByteBufAsciiString readBytes(ByteBuf byteBuf, int length) {
    return new ByteBufAsciiString(byteBuf.readBytes(length));
  }

  public static ByteBufAsciiString readRetainedSlice(ByteBuf byteBuf, int length) {
    return new ByteBufAsciiString(byteBuf.readRetainedSlice(length));
  }

  public static ByteBufAsciiString read(ByteBuf byteBuf, int length) {
    return new ByteBufAsciiString(NettyUtils.read(byteBuf, length));
  }

  public static ByteBufAsciiString copy(ByteBuf byteBuf, int index, int length) {
    return new ByteBufAsciiString(byteBuf.copy(index, length));
  }

  public static ByteBufAsciiString slice(ByteBuf byteBuf, int index, int length) {
    return new ByteBufAsciiString(byteBuf.slice(index, length));
  }

  /**
   * Iterates over the readable bytes of this buffer with the specified {@code processor} in ascending order.
   *
   * @return {@code -1} if the processor iterated to or beyond the end of the readable bytes.
   *         The last-visited index If the {@link ByteProcessor#process(byte)} returned {@code false}.
   */
  public int forEachByte(ByteProcessor visitor) {
    return forEachByte0(0, length(), visitor);
  }

  /**
   * Iterates over the specified area of this buffer with the specified {@code processor} in ascending order.
   * (i.e. {@code index}, {@code (index + 1)},  .. {@code (index + length - 1)}).
   *
   * @return {@code -1} if the processor iterated to or beyond the end of the specified area.
   *         The last-visited index If the {@link ByteProcessor#process(byte)} returned {@code false}.
   */
  public int forEachByte(int index, int length, ByteProcessor visitor) {
    if (isOutOfBounds(index, length, length())) {
      throw new IndexOutOfBoundsException(
          "expected: " + "0 <= index(" + index + ") <= start + length(" + length + ") <= " + "length(" + length()
              + ')');
    }
    return forEachByte0(index, length, visitor);
  }

  private int forEachByte0(int index, int length, ByteProcessor visitor) {
    int ret = _buf.forEachByte(_buf.readerIndex() + index, length, visitor);
    return ret >= 0 ? ret - _buf.readerIndex() : -1;
  }

  /**
   * Iterates over the readable bytes of this buffer with the specified {@code processor} in descending order.
   *
   * @return {@code -1} if the processor iterated to or beyond the beginning of the readable bytes.
   *         The last-visited index If the {@link ByteProcessor#process(byte)} returned {@code false}.
   */
  public int forEachByteDesc(ByteProcessor visitor) {
    return forEachByteDesc0(0, length(), visitor);
  }

  /**
   * Iterates over the specified area of this buffer with the specified {@code processor} in descending order.
   * (i.e. {@code (index + length - 1)}, {@code (index + length - 2)}, ... {@code index}).
   *
   * @return {@code -1} if the processor iterated to or beyond the beginning of the specified area.
   *         The last-visited index If the {@link ByteProcessor#process(byte)} returned {@code false}.
   */
  public int forEachByteDesc(int index, int length, ByteProcessor visitor) {
    if (isOutOfBounds(index, length, length())) {
      throw new IndexOutOfBoundsException(
          "expected: " + "0 <= index(" + index + ") <= start + length(" + length + ") <= " + "length(" + length()
              + ')');
    }
    return forEachByteDesc0(index, length, visitor);
  }

  private int forEachByteDesc0(int index, int length, ByteProcessor visitor) {
    int ret = _buf.forEachByteDesc(index, length, visitor);
    return ret >= 0 ? ret - _buf.readerIndex() : -1;
  }

  /**
   * Determine if this instance has 0 length.
   */
  public boolean isEmpty() {
    return !_buf.isReadable();
  }

  @Override
  public int length() {
    return _buf.readableBytes();
  }

  public byte byteAt(int index) {
    // We must do a range check here to enforce the access does not go outside our sub region of the array.
    // We rely on the array access itself to pick up the array out of bounds conditions
    if (index < 0 || index >= length()) {
      throw new IndexOutOfBoundsException("index: " + index + " must be in the range [0," + length() + ")");
    }
    return _buf.getByte(_buf.readerIndex() + index);
  }

  /**
   * Converts this string to a byte array.
   */
  public byte[] toByteArray() {
    return toByteArray(0, length());
  }

  /**
   * Converts a subset of this string to a byte array.
   * The subset is defined by the range [{@code start}, {@code end}).
   */
  public byte[] toByteArray(int start, int end) {
    return ByteBufUtil.getBytes(_buf, start + _buf.readerIndex(), end - start);
  }

  @Override
  public char charAt(int index) {
    return b2c(byteAt(index));
  }

  /**
   * Determines if this {@code String} contains the sequence of characters in the {@code CharSequence} passed.
   *
   * @param cs the character sequence to search for.
   * @return {@code true} if the sequence of characters are contained in this string, otherwise {@code false}.
   */
  public boolean contains(CharSequence cs) {
    return indexOf(cs) >= 0;
  }

  /**
   * Compares the specified string to this string using the ASCII values of the characters. Returns 0 if the strings
   * contain the same characters in the same order. Returns a negative integer if the first non-equal character in
   * this string has an ASCII value which is less than the ASCII value of the character at the same position in the
   * specified string, or if this string is a prefix of the specified string. Returns a positive integer if the first
   * non-equal character in this string has a ASCII value which is greater than the ASCII value of the character at
   * the same position in the specified string, or if the specified string is a prefix of this string.
   *
   * @param string the string to compare.
   * @return 0 if the strings are equal, a negative integer if this string is before the specified string, or a
   *         positive integer if this string is after the specified string.
   * @throws NullPointerException if {@code string} is {@code null}.
   */
  @Override
  public int compareTo(@Nonnull CharSequence string) {
    if (this == string) {
      return 0;
    }

    int result;
    int length1 = length();
    int length2 = string.length();
    int minLength = Math.min(length1, length2);
    for (int i = 0, j = _buf.readerIndex(); i < minLength; i++, j++) {
      result = b2c(_buf.getByte(j)) - string.charAt(i);
      if (result != 0) {
        return result;
      }
    }

    return length1 - length2;
  }

  /**
   * Concatenates this string and the specified string.
   *
   * @param strings the strings to concatenate
   * @return a new string which is the concatenation of this string and the specified string.
   */
  public ByteBufAsciiString concat(CharSequence... strings) {
    ByteBufAsciiString thisString = this;
    ByteBuf newValue = null;
    for (CharSequence string: strings) {
      if (string.length() < 1) {
        continue;
      }
      if (newValue == null) {
        int newLen = length() + Stream.of(strings).mapToInt(CharSequence::length).sum();
        if (thisString.isEmpty()) {
          if (string.getClass() == ByteBufAsciiString.class) {
            thisString = (ByteBufAsciiString) string;
            continue;
          }
          newValue = UnpooledByteBufAllocator.DEFAULT.buffer(newLen);
        } else {
          ByteBuf buf = thisString._buf;
          newValue = buf.alloc().buffer(newLen);
          newValue.writeBytes(buf, buf.readerIndex(), buf.readableBytes());
        }
      }

      if (string.getClass() == ByteBufAsciiString.class) {
        ByteBuf buf = ((ByteBufAsciiString) string)._buf;
        newValue.writeBytes(buf, buf.readerIndex(), buf.readableBytes());
      } else if (string.getClass() == AsciiString.class) {
        AsciiString asciiString = (AsciiString) string;
        newValue.writeBytes(asciiString.array(), asciiString.arrayOffset(), asciiString.length());
      } else {
        for (int i = 0; i < string.length(); i++) {
          newValue.writeByte(c2b(string.charAt(i)));
        }
      }
    }
    if (newValue == null) {
      return thisString;
    }
    return new ByteBufAsciiString(newValue);
  }

  /**
   * Compares the specified string to this string to determine if the specified string is a suffix.
   *
   * @param suffix the suffix to look for.
   * @return {@code true} if the specified string is a suffix of this string, {@code false} otherwise.
   * @throws NullPointerException if {@code suffix} is {@code null}.
   */
  public boolean endsWith(CharSequence suffix) {
    int suffixLen = suffix.length();
    return regionMatches(length() - suffixLen, suffix, 0, suffixLen);
  }

  /**
   * Compares the specified string to this string ignoring the case of the characters and returns true if they are
   * equal.
   *
   * @param string the string to compare.
   * @return {@code true} if the specified string is equal to this string, {@code false} otherwise.
   */
  public boolean contentEqualsIgnoreCase(CharSequence string) {
    if (string == null || string.length() != length()) {
      return false;
    }

    if (string.getClass() == ByteBufAsciiString.class) {
      ByteBufAsciiString rhs = (ByteBufAsciiString) string;
      for (int i = _buf.readerIndex(), j = rhs._buf.readerIndex(); i < length(); ++i, ++j) {
        if (!equalsIgnoreCase(_buf.getByte(i), rhs._buf.getByte(j))) {
          return false;
        }
      }
      return true;
    }

    for (int i = _buf.readerIndex(), j = 0; i < length(); ++i, ++j) {
      if (!equalsIgnoreCase(b2c(_buf.getByte(i)), string.charAt(j))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Copies a range of characters into a new string.
   * @param start the offset of the first character (inclusive).
   * @return a new string containing the characters from start to the end of the string.
   * @throws IndexOutOfBoundsException if {@code start < 0} or {@code start > length()}.
   */
  public ByteBufAsciiString subSequence(int start) {
    return subSequence(start, length());
  }

  /**
   * Copies a range of characters into a new string.
   * @param start the offset of the first character (inclusive).
   * @param end The index to stop at (exclusive).
   * @return a new string containing the characters from start to the end of the string.
   * @throws IndexOutOfBoundsException if {@code start < 0} or {@code start > length()}.
   */
  @Override
  public ByteBufAsciiString subSequence(int start, int end) {
    return subSequence(start, end, true);
  }

  /**
   * Either copy or share a subset of underlying sub-sequence of bytes.
   * @param start the offset of the first character (inclusive).
   * @param end The index to stop at (exclusive).
   * @param copy If {@code true} then a copy of the underlying storage will be made.
   * If {@code false} then the underlying storage will be shared.
   * @return a new string containing the characters from start to the end of the string.
   * @throws IndexOutOfBoundsException if {@code start < 0} or {@code start > length()}.
   */
  public ByteBufAsciiString subSequence(int start, int end, boolean copy) {
    if (isOutOfBounds(start, end - start, length())) {
      throw new IndexOutOfBoundsException(
          "expected: 0 <= start(" + start + ") <= end (" + end + ") <= length(" + length() + ')');
    }

    if (start == 0 && end == length()) {
      return this;
    }

    if (end == start) {
      return EMPTY_STRING;
    }

    if (copy) {
      return copy(_buf, start + _buf.readerIndex(), end - start);
    } else {
      return slice(_buf, start + _buf.readerIndex(), end - start);
    }
  }

  /**
   * Searches in this string for the first index of the specified string. The search for the string starts at the
   * beginning and moves towards the end of this string.
   *
   * @param string the string to find.
   * @return the index of the first character of the specified string in this string, -1 if the specified string is
   *         not a substring.
   * @throws NullPointerException if {@code string} is {@code null}.
   */
  public int indexOf(CharSequence string) {
    return indexOf(string, 0);
  }

  /**
   * Searches in this string for the index of the specified string. The search for the string starts at the specified
   * offset and moves towards the end of this string.
   *
   * @param subString the string to find.
   * @param start the starting offset.
   * @return the index of the first character of the specified string in this string, -1 if the specified string is
   *         not a substring.
   * @throws NullPointerException if {@code subString} is {@code null}.
   */
  public int indexOf(CharSequence subString, int start) {
    final int thisLen = length();
    final int subCount = subString.length();
    if (start < 0) {
      start = 0;
    }
    if (subCount <= 0) {
      return start < thisLen ? start : thisLen;
    }
    if (subCount > thisLen - start) {
      return INDEX_NOT_FOUND;
    }

    final char firstChar = subString.charAt(0);
    if (firstChar > MAX_CHAR_VALUE) {
      return INDEX_NOT_FOUND;
    }
    final byte firstCharAsByte = c2b0(firstChar);
    final int len = _buf.readerIndex() + thisLen - subCount;
    for (int i = start + _buf.readerIndex(); i <= len; ++i) {
      if (_buf.getByte(i) == firstCharAsByte) {
        int o1 = i;
        int o2 = 0;
        while (++o2 < subCount && b2c(_buf.getByte(++o1)) == subString.charAt(o2)) {
          // Intentionally empty
        }
        if (o2 == subCount) {
          return i - _buf.readerIndex();
        }
      }
    }
    return INDEX_NOT_FOUND;
  }

  /**
   * Searches in this string for the index of the specified char {@code ch}.
   * The search for the char starts at the specified offset {@code start} and moves towards the end of this string.
   *
   * @param ch the char to find.
   * @param start the starting offset.
   * @return the index of the first occurrence of the specified char {@code ch} in this string,
   * -1 if found no occurrence.
   */
  public int indexOf(char ch, int start) {
    if (start < 0) {
      start = 0;
    }

    final int thisLen = length();

    if (ch > MAX_CHAR_VALUE) {
      return -1;
    }

    return forEachByte(start, thisLen - start, new ByteProcessor.IndexOfProcessor((byte) ch));
  }

  /**
   * Searches in this string for the last index of the specified string. The search for the string starts at the end
   * and moves towards the beginning of this string.
   *
   * @param string the string to find.
   * @return the index of the first character of the specified string in this string, -1 if the specified string is
   *         not a substring.
   * @throws NullPointerException if {@code string} is {@code null}.
   */
  public int lastIndexOf(CharSequence string) {
    // Use count instead of count - 1 so lastIndexOf("") answers count
    return lastIndexOf(string, length());
  }

  /**
   * Searches in this string for the index of the specified string. The search for the string starts at the specified
   * offset and moves towards the beginning of this string.
   *
   * @param subString the string to find.
   * @param start the starting offset.
   * @return the index of the first character of the specified string in this string , -1 if the specified string is
   *         not a substring.
   * @throws NullPointerException if {@code subString} is {@code null}.
   */
  public int lastIndexOf(CharSequence subString, int start) {
    final int subCount = subString.length();
    start = Math.min(start, length() - subCount);
    if (start < 0) {
      return INDEX_NOT_FOUND;
    }
    if (subCount == 0) {
      return start;
    }

    final char firstChar = subString.charAt(0);
    if (firstChar > MAX_CHAR_VALUE) {
      return INDEX_NOT_FOUND;
    }
    final byte firstCharAsByte = c2b0(firstChar);
    final int readerIndex = _buf.readerIndex();
    for (int i = readerIndex + start; i >= readerIndex; --i) {
      if (_buf.getByte(i) == firstCharAsByte) {
        int o1 = i;
        int o2 = 0;
        while (++o2 < subCount && b2c(_buf.getByte(++o1)) == subString.charAt(o2)) {
          // Intentionally empty
        }
        if (o2 == subCount) {
          return i - readerIndex;
        }
      }
    }
    return INDEX_NOT_FOUND;
  }

  /**
   * Compares the specified string to this string and compares the specified range of characters to determine if they
   * are the same.
   *
   * @param thisStart the starting offset in this string.
   * @param string the string to compare.
   * @param start the starting offset in the specified string.
   * @param length the number of characters to compare.
   * @return {@code true} if the ranges of characters are equal, {@code false} otherwise
   * @throws NullPointerException if {@code string} is {@code null}.
   */
  public boolean regionMatches(int thisStart, CharSequence string, int start, int length) {
    if (string == null) {
      throw new NullPointerException("string");
    }

    if (start < 0 || string.length() - start < length) {
      return false;
    }

    final int thisLen = length();
    if (thisStart < 0 || thisLen - thisStart < length) {
      return false;
    }

    if (length <= 0) {
      return true;
    }

    final int thatEnd = start + length;
    for (int i = start, j = thisStart + _buf.readerIndex(); i < thatEnd; i++, j++) {
      if (b2c(_buf.getByte(j)) != string.charAt(i)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Compares the specified string to this string and compares the specified range of characters to determine if they
   * are the same. When ignoreCase is true, the case of the characters is ignored during the comparison.
   *
   * @param ignoreCase specifies if case should be ignored.
   * @param thisStart the starting offset in this string.
   * @param string the string to compare.
   * @param start the starting offset in the specified string.
   * @param length the number of characters to compare.
   * @return {@code true} if the ranges of characters are equal, {@code false} otherwise.
   * @throws NullPointerException if {@code string} is {@code null}.
   */
  public boolean regionMatches(boolean ignoreCase, int thisStart, CharSequence string, int start, int length) {
    if (!ignoreCase) {
      return regionMatches(thisStart, string, start, length);
    }

    if (string == null) {
      throw new NullPointerException("string");
    }

    final int thisLen = length();
    if (thisStart < 0 || length > thisLen - thisStart) {
      return false;
    }
    if (start < 0 || length > string.length() - start) {
      return false;
    }

    thisStart += _buf.readerIndex();
    final int thisEnd = thisStart + length;
    while (thisStart < thisEnd) {
      if (!equalsIgnoreCase(b2c(_buf.getByte(thisStart++)), string.charAt(start++))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Copies this string replacing occurrences of the specified character with another character.
   *
   * @param oldChar the character to replace.
   * @param newChar the replacement character.
   * @return a new string with occurrences of oldChar replaced by newChar.
   */
  public ByteBufAsciiString replace(char oldChar, char newChar) {
    if (oldChar > MAX_CHAR_VALUE) {
      return this;
    }

    final int index;
    final byte oldCharByte = c2b(oldChar);
    index = forEachByte(new ByteProcessor.IndexOfProcessor(oldCharByte));
    if (index == -1) {
      return this;
    }

    final byte newCharByte = c2b(newChar);
    final int length = length();
    ByteBuf buffer = _buf.alloc().buffer(length);
    for (int i = 0, j = _buf.readerIndex(); i < length; i++, j++) {
      byte b = _buf.getByte(j);
      if (b == oldCharByte) {
        b = newCharByte;
      }
      buffer.writeByte(b);
    }

    return new ByteBufAsciiString(buffer);
  }

  /**
   * Compares the specified string to this string to determine if the specified string is a prefix.
   *
   * @param prefix the string to look for.
   * @return {@code true} if the specified string is a prefix of this string, {@code false} otherwise
   * @throws NullPointerException if {@code prefix} is {@code null}.
   */
  public boolean startsWith(CharSequence prefix) {
    return startsWith(prefix, 0);
  }

  /**
   * Compares the specified string to this string to determine if the specified string is a prefix.
   *
   * @param prefix the string to look for.
   * @return {@code true} if the specified string is a prefix of this string, {@code false} otherwise
   * @throws NullPointerException if {@code prefix} is {@code null}.
   */
  public static boolean startsWith(CharSequence string, CharSequence prefix) {
    return startsWith(string, prefix, 0);
  }

  /**
   * Compares the specified string to this string to determine if the specified string is a prefix.
   *
   * @param prefix the string to look for.
   * @return {@code true} if the specified string is a prefix of this string, {@code false} otherwise
   * @throws NullPointerException if {@code prefix} is {@code null}.
   */
  public static boolean startsWithIgnoreCase(CharSequence string, CharSequence prefix) {
    return startsWithIgnoreCase(string, prefix, 0);
  }

  /**
   * Compares the specified string to this string, starting at the specified offset, to determine if the specified
   * string is a prefix.
   *
   * @param prefix the string to look for.
   * @param start the starting offset.
   * @return {@code true} if the specified string occurs in this string at the specified offset, {@code false}
   *         otherwise.
   * @throws NullPointerException if {@code prefix} is {@code null}.
   */
  public boolean startsWith(CharSequence prefix, int start) {
    return regionMatches(start, prefix, 0, prefix.length());
  }

  /**
   * Compares the specified string to this string, starting at the specified offset, to determine if the specified
   * string is a prefix.
   *
   * @param prefix the string to look for.
   * @param start the starting offset.
   * @return {@code true} if the specified string occurs in this string at the specified offset, {@code false}
   *         otherwise.
   * @throws NullPointerException if {@code prefix} is {@code null}.
   */
  public static boolean startsWith(CharSequence string, CharSequence prefix, int start) {
    return regionMatches(string, false, start, prefix, 0, prefix.length());
  }

  public static boolean startsWithIgnoreCase(CharSequence string, CharSequence prefix, int start) {
    return regionMatches(string, true, start, prefix, 0, prefix.length());
  }

  /**
   * Converts the characters in this string to lowercase, using the default Locale.
   *
   * @return a new string containing the lowercase characters equivalent to the characters in this string.
   */
  public ByteBufAsciiString toLowerCase() {
    boolean lowercased = true;
    int i;
    int j;
    final int len = length() + _buf.readerIndex();
    for (i = _buf.readerIndex(); i < len; ++i) {
      byte b = _buf.getByte(i);
      if (b >= 'A' && b <= 'Z') {
        lowercased = false;
        break;
      }
    }

    // Check if this string does not contain any uppercase characters.
    if (lowercased) {
      return this;
    }

    final int length = length();
    final ByteBuf newValue = _buf.alloc().buffer(length);
    for (i = 0, j = _buf.readerIndex(); i < length; ++i, ++j) {
      newValue.writeByte(toLowerCase(_buf.getByte(j)));
    }

    return new ByteBufAsciiString(newValue);
  }

  /**
   * Converts the characters in this string to uppercase, using the default Locale.
   *
   * @return a new string containing the uppercase characters equivalent to the characters in this string.
   */
  public ByteBufAsciiString toUpperCase() {
    boolean uppercased = true;
    int i;
    int j;
    final int len = length() + _buf.readerIndex();
    for (i = _buf.readerIndex(); i < len; ++i) {
      byte b = _buf.getByte(i);
      if (b >= 'a' && b <= 'z') {
        uppercased = false;
        break;
      }
    }

    // Check if this string does not contain any lowercase characters.
    if (uppercased) {
      return this;
    }

    final int length = length();
    final ByteBuf newValue = _buf.alloc().buffer(length);
    for (i = 0, j = _buf.readerIndex(); i < length; ++i, ++j) {
      newValue.writeByte(toUpperCase(_buf.getByte(j)));
    }

    return new ByteBufAsciiString(newValue);
  }

  /**
   * Copies this string removing white space characters from the beginning and end of the string, and tries not to
   * copy if possible.
   *
   * @param c The {@link CharSequence} to trim.
   * @return a new string with characters {@code <= \\u0020} removed from the beginning and the end.
   */
  public static CharSequence trim(CharSequence c) {
    if (c.getClass() == ByteBufAsciiString.class) {
      return ((ByteBufAsciiString) c).trim();
    }
    return AsciiString.trim(c);
  }

  /**
   * Duplicates this string removing white space characters from the beginning and end of the
   * string, without copying.
   *
   * @return a new string with characters {@code <= \\u0020} removed from the beginning and the end.
   */
  public ByteBufAsciiString trim() {
    int start = _buf.readerIndex();
    final int last = start + length() - 1;
    final int first = start;
    int end = last;

    while (start <= end && _buf.getByte(start) <= ' ') {
      start++;
    }
    while (end >= start && _buf.getByte(end) <= ' ') {
      end--;
    }
    if (start == first && end == last) {
      return this;
    }
    return slice(_buf, start, end - start + 1);
  }

  /**
   * Compares a {@code CharSequence} to this {@code String} to determine if their contents are equal.
   *
   * @param a the character sequence to compare to.
   * @return {@code true} if equal, otherwise {@code false}
   */
  public boolean contentEquals(CharSequence a) {
    if (a == null || a.length() != length()) {
      return false;
    }
    if (this == a) {
      return true;
    }
    if (a.getClass() == ByteBufAsciiString.class) {
      return equals((ByteBufAsciiString) a);
    }
    for (int i = _buf.readerIndex(), j = 0; j < a.length(); ++i, ++j) {
      if (b2c(_buf.getByte(i)) != a.charAt(j)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Determines whether this string matches a given regular expression.
   *
   * @param expr the regular expression to be matched.
   * @return {@code true} if the expression matches, otherwise {@code false}.
   * @throws java.util.regex.PatternSyntaxException if the syntax of the supplied regular expression is not valid.
   * @throws NullPointerException if {@code expr} is {@code null}.
   */
  public boolean matches(String expr) {
    return Pattern.matches(expr, this);
  }

  /**
   * Splits this string using the supplied regular expression {@code expr}. The parameter {@code max} controls the
   * behavior how many times the pattern is applied to the string.
   *
   * @param expr the regular expression used to divide the string.
   * @param max the number of entries in the resulting array.
   * @return an array of Strings created by separating the string along matches of the regular expression.
   * @throws NullPointerException if {@code expr} is {@code null}.
   * @throws java.util.regex.PatternSyntaxException if the syntax of the supplied regular expression is not valid.
   * @see Pattern#split(CharSequence, int)
   */
  public ByteBufAsciiString[] split(String expr, int max) {
    return split(Pattern.compile(expr), max);
  }

  public ByteBufAsciiString[] split(@Nonnull Pattern pattern) {
    return split(pattern, 0);
  }

  public ByteBufAsciiString[] split(@Nonnull Pattern pattern, int limit) {
    int index = 0;
    boolean matchLimited = limit > 0;
    final List<ByteBufAsciiString> matchList = InternalThreadLocalMap.get().arrayList();
    Matcher m = pattern.matcher(this);

    // Add segments before each match found
    while (m.find()) {
      if (!matchLimited || matchList.size() < limit - 1) {
        if (index == 0 && index == m.start() && m.start() == m.end()) {
          // no empty leading substring included for zero-width match
          // at the beginning of the input char sequence.
          continue;
        }
        ByteBufAsciiString match = subSequence(index, m.start(), false);
        matchList.add(match);
        index = m.end();
      } else if (matchList.size() == limit - 1) { // last one
        ByteBufAsciiString match = subSequence(index, length(), false);
        matchList.add(match);
        index = m.end();
      }
    }

    // If no match was found, return this
    if (index == 0) {
      return new ByteBufAsciiString[] { this };
    }

    // Add remaining segment
    if (!matchLimited || matchList.size() < limit) {
      matchList.add(subSequence(index, length(), false));
    }

    // Construct result
    int resultSize = matchList.size();
    if (limit == 0) {
      while (resultSize > 0 && matchList.get(resultSize - 1).isEmpty()) {
        resultSize--;
      }
    }
    return matchList.subList(0, resultSize).toArray(new ByteBufAsciiString[0]);
  }

  /**
   * Splits the specified {@link String} with the specified delimiter..
   */
  public ByteBufAsciiString[] split(char delim) {
    final List<ByteBufAsciiString> res = InternalThreadLocalMap.get().arrayList();

    int start = 0;
    final int length = length();
    for (int i = start; i < length; i++) {
      if (charAt(i) == delim) {
        if (start == i) {
          res.add(EMPTY_STRING);
        } else {
          res.add(slice(_buf, start + _buf.readerIndex(), i - start));
        }
        start = i + 1;
      }
    }

    if (start == 0) { // If no delimiter was found in the value
      res.add(this);
    } else {
      if (start != length) {
        // Add the last element if it's not empty.
        res.add(slice(_buf, start + _buf.readerIndex(), length - start));
      } else {
        // Truncate trailing empty elements.
        for (int i = res.size() - 1; i >= 0; i--) {
          if (res.get(i).isEmpty()) {
            res.remove(i);
          } else {
            break;
          }
        }
      }
    }

    return res.toArray(new ByteBufAsciiString[0]);
  }

  /**
   * {@inheritDoc}
   * <p>
   * Provides a case-insensitive hash code for Ascii like byte strings.
   */
  @Override
  public int hashCode() {
    int h = _hash;
    if (h == 0) {
      if (_buf.hasArray()) {
        h = PlatformDependent
            .hashCodeAscii(_buf.array(), _buf.arrayOffset() + _buf.readerIndex(), _buf.readableBytes());
      } else {
        h = PlatformDependent.hashCodeAscii(this);
      }
      _hash = h;
    }
    return h;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || obj.getClass() != ByteBufAsciiString.class) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    return equals((ByteBufAsciiString) obj);
  }

  private boolean equals(ByteBufAsciiString other) {
    return length() == other.length() && hashCode() == other.hashCode()
        && ByteBufUtil.equals(_buf, _buf.readerIndex(), other._buf, other._buf.readerIndex(), length());
  }

  @Nonnull
  public AsciiString toAsciiString() {
    if (isEmpty()) {
      return AsciiString.EMPTY_STRING;
    }
    if (_buf.hasArray()) {
      return new AsciiString(_buf.array(), _buf.arrayOffset() + _buf.readerIndex(), _buf.readableBytes(), true);
    } else {
      byte[] bytes = new byte[length()];
      _buf.getBytes(_buf.readerIndex(), bytes, 0, bytes.length);
      return new AsciiString(bytes, 0, bytes.length, false);
    }
  }

  /**
   * Translates the entire byte string to a {@link String}.
   * @see #toString(int)
   */
  @Override
  @Nonnull
  public String toString() {
    String cache = _string;
    if (cache == null) {
      cache = toString(0);
      _string = cache;
    }
    return cache;
  }

  /**
   * Translates the entire byte string to a {@link String} using the {@code charset} encoding.
   * @see #toString(int, int)
   */
  public String toString(int start) {
    return toString(start, length());
  }

  /**
   * Translates the [{@code start}, {@code end}) range of this byte string to a {@link String}.
   */
  @SuppressWarnings("deprecation")
  public String toString(int start, int end) {
    int length = end - start;
    if (length == 0) {
      return "";
    }

    if (isOutOfBounds(start, length, length())) {
      throw new IndexOutOfBoundsException(
          "expected: " + "0 <= start(" + start + ") <= srcIdx + length(" + length + ") <= srcLen(" + length() + ')');
    }

    final String str;
    if (_buf.hasArray()) {
      str = new String(_buf.array(), 0, start + _buf.arrayOffset() + _buf.readerIndex(), length);
    } else {
      str = _buf.toString(start + _buf.readerIndex(), length, StandardCharsets.US_ASCII);
    }
    return str;
  }

  /**
   * Determine if {@code collection} contains {@code value} and using
   * {@link #contentEqualsIgnoreCase(CharSequence, CharSequence)} to compare values.
   * @param collection The collection to look for and equivalent element as {@code value}.
   * @param value The value to look for in {@code collection}.
   * @return {@code true} if {@code collection} contains {@code value} according to
   * {@link #contentEqualsIgnoreCase(CharSequence, CharSequence)}. {@code false} otherwise.
   * @see #contentEqualsIgnoreCase(CharSequence, CharSequence)
   */
  public static boolean containsContentEqualsIgnoreCase(Collection<CharSequence> collection, CharSequence value) {
    for (CharSequence v: collection) {
      if (contentEqualsIgnoreCase(value, v)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Determine if {@code a} contains all of the values in {@code b} using
   * {@link #contentEqualsIgnoreCase(CharSequence, CharSequence)} to compare values.
   * @param a The collection under test.
   * @param b The values to test for.
   * @return {@code true} if {@code a} contains all of the values in {@code b} using
   * {@link #contentEqualsIgnoreCase(CharSequence, CharSequence)} to compare values. {@code false} otherwise.
   * @see #contentEqualsIgnoreCase(CharSequence, CharSequence)
   */
  public static boolean containsAllContentEqualsIgnoreCase(Collection<CharSequence> a, Collection<CharSequence> b) {
    for (CharSequence v: b) {
      if (!containsContentEqualsIgnoreCase(a, v)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns {@code true} if the content of both {@link CharSequence}'s are equals. This only supports 8-bit ASCII.
   */
  public static boolean contentEquals(CharSequence a, CharSequence b) {
    if (a == null || b == null) {
      return a == b;
    }

    if (a.getClass() == ByteBufAsciiString.class) {
      return ((ByteBufAsciiString) a).contentEquals(b);
    }

    if (b.getClass() == ByteBufAsciiString.class) {
      return ((ByteBufAsciiString) b).contentEquals(a);
    }

    return AsciiString.contentEquals(a, b);
  }

  /**
   * Returns the case-insensitive hash code of the specified string. Note that this method uses the same hashing
   * algorithm with {@link #hashCode()} so that you can put both {@link AsciiString}s and arbitrary
   * {@link CharSequence}s into the same headers.
   */
  public static int hashCode(CharSequence value) {
    if (value == null) {
      return 0;
    }
    if (value.getClass() == ByteBufAsciiString.class) {
      return value.hashCode();
    }
    return AsciiString.hashCode(value);
  }

  /**
   * Determine if {@code a} contains {@code b} in a case sensitive manner.
   */
  public static boolean contains(CharSequence a, CharSequence b) {
    if (a instanceof ByteBufAsciiString) {
      return ((ByteBufAsciiString) a).contains(b);
    }
    return AsciiString.contains(a, b);
  }

  /**
   * Determine if {@code a} contains {@code b} in a case insensitive manner.
   */
  public static boolean containsIgnoreCase(CharSequence a, CharSequence b) {
    return AsciiString.containsIgnoreCase(a, b);
  }

  /**
   * Returns {@code true} if both {@link CharSequence}'s are equals when ignore the case. This only supports 8-bit
   * ASCII.
   */
  public static boolean contentEqualsIgnoreCase(CharSequence a, CharSequence b) {
    if (a == null || b == null) {
      return a == b;
    }

    if (a.getClass() == ByteBufAsciiString.class) {
      return ((ByteBufAsciiString) a).contentEqualsIgnoreCase(b);
    }
    if (b.getClass() == ByteBufAsciiString.class) {
      return ((ByteBufAsciiString) b).contentEqualsIgnoreCase(a);
    }

    return AsciiString.contentEqualsIgnoreCase(a, b);
  }

  /**
   * This methods make regionMatches operation correctly for any chars in strings
   * @param cs the {@code CharSequence} to be processed
   * @param ignoreCase specifies if case should be ignored.
   * @param csStart the starting offset in the {@code cs} CharSequence
   * @param string the {@code CharSequence} to compare.
   * @param start the starting offset in the specified {@code string}.
   * @param length the number of characters to compare.
   * @return {@code true} if the ranges of characters are equal, {@code false} otherwise.
   */
  public static boolean regionMatches(
      final CharSequence cs,
      final boolean ignoreCase,
      final int csStart,
      final CharSequence string,
      final int start,
      final int length) {
    if (cs == null || string == null) {
      return false;
    }

    if (cs instanceof ByteBufAsciiString) {
      return ((ByteBufAsciiString) cs).regionMatches(ignoreCase, csStart, string, start, length);
    }

    return AsciiString.regionMatches(cs, ignoreCase, csStart, string, start, length);
  }

  /**
   * This is optimized version of regionMatches for string with ASCII chars only
   * @param cs the {@code CharSequence} to be processed
   * @param ignoreCase specifies if case should be ignored.
   * @param csStart the starting offset in the {@code cs} CharSequence
   * @param string the {@code CharSequence} to compare.
   * @param start the starting offset in the specified {@code string}.
   * @param length the number of characters to compare.
   * @return {@code true} if the ranges of characters are equal, {@code false} otherwise.
   */
  public static boolean regionMatchesAscii(
      final CharSequence cs,
      final boolean ignoreCase,
      final int csStart,
      final CharSequence string,
      final int start,
      final int length) {
    if (cs == null || string == null) {
      return false;
    }

    if (cs instanceof ByteBufAsciiString) {
      return ((ByteBufAsciiString) cs).regionMatches(ignoreCase, csStart, string, start, length);
    }

    return AsciiString.regionMatchesAscii(cs, ignoreCase, csStart, string, start, length);
  }

  /**
   * <p>Case in-sensitive find of the first index within a CharSequence
   * from the specified position.</p>
   *
   * <p>A {@code null} CharSequence will return {@code -1}.
   * A negative start position is treated as zero.
   * An empty ("") search CharSequence always matches.
   * A start position greater than the string length only matches
   * an empty search CharSequence.</p>
   *
   * <pre>
   * AsciiString.indexOfIgnoreCase(null, *, *)          = -1
   * AsciiString.indexOfIgnoreCase(*, null, *)          = -1
   * AsciiString.indexOfIgnoreCase("", "", 0)           = 0
   * AsciiString.indexOfIgnoreCase("aabaabaa", "A", 0)  = 0
   * AsciiString.indexOfIgnoreCase("aabaabaa", "B", 0)  = 2
   * AsciiString.indexOfIgnoreCase("aabaabaa", "AB", 0) = 1
   * AsciiString.indexOfIgnoreCase("aabaabaa", "B", 3)  = 5
   * AsciiString.indexOfIgnoreCase("aabaabaa", "B", 9)  = -1
   * AsciiString.indexOfIgnoreCase("aabaabaa", "B", -1) = 2
   * AsciiString.indexOfIgnoreCase("aabaabaa", "", 2)   = 2
   * AsciiString.indexOfIgnoreCase("abc", "", 9)        = -1
   * </pre>
   *
   * @param str  the CharSequence to check, may be null
   * @param searchStr  the CharSequence to find, may be null
   * @param startPos  the start position, negative treated as zero
   * @return the first index of the search CharSequence (always &ge; startPos),
   *  -1 if no match or {@code null} string input
   */
  public static int indexOfIgnoreCase(final CharSequence str, final CharSequence searchStr, int startPos) {
    if (str == null || searchStr == null) {
      return INDEX_NOT_FOUND;
    }
    if (startPos < 0) {
      startPos = 0;
    }
    int searchStrLen = searchStr.length();
    final int endLimit = str.length() - searchStrLen + 1;
    if (startPos > endLimit) {
      return INDEX_NOT_FOUND;
    }
    if (searchStrLen == 0) {
      return startPos;
    }
    for (int i = startPos; i < endLimit; i++) {
      if (regionMatches(str, true, i, searchStr, 0, searchStrLen)) {
        return i;
      }
    }
    return INDEX_NOT_FOUND;
  }

  /**
   * <p>Case in-sensitive find of the first index within a CharSequence
   * from the specified position. This method optimized and works correctly for ASCII CharSequences only</p>
   *
   * <p>A {@code null} CharSequence will return {@code -1}.
   * A negative start position is treated as zero.
   * An empty ("") search CharSequence always matches.
   * A start position greater than the string length only matches
   * an empty search CharSequence.</p>
   *
   * <pre>
   * AsciiString.indexOfIgnoreCase(null, *, *)          = -1
   * AsciiString.indexOfIgnoreCase(*, null, *)          = -1
   * AsciiString.indexOfIgnoreCase("", "", 0)           = 0
   * AsciiString.indexOfIgnoreCase("aabaabaa", "A", 0)  = 0
   * AsciiString.indexOfIgnoreCase("aabaabaa", "B", 0)  = 2
   * AsciiString.indexOfIgnoreCase("aabaabaa", "AB", 0) = 1
   * AsciiString.indexOfIgnoreCase("aabaabaa", "B", 3)  = 5
   * AsciiString.indexOfIgnoreCase("aabaabaa", "B", 9)  = -1
   * AsciiString.indexOfIgnoreCase("aabaabaa", "B", -1) = 2
   * AsciiString.indexOfIgnoreCase("aabaabaa", "", 2)   = 2
   * AsciiString.indexOfIgnoreCase("abc", "", 9)        = -1
   * </pre>
   *
   * @param str  the CharSequence to check, may be null
   * @param searchStr  the CharSequence to find, may be null
   * @param startPos  the start position, negative treated as zero
   * @return the first index of the search CharSequence (always &ge; startPos),
   *  -1 if no match or {@code null} string input
   */
  public static int indexOfIgnoreCaseAscii(final CharSequence str, final CharSequence searchStr, int startPos) {
    if (str == null || searchStr == null) {
      return INDEX_NOT_FOUND;
    }
    if (startPos < 0) {
      startPos = 0;
    }
    int searchStrLen = searchStr.length();
    final int endLimit = str.length() - searchStrLen + 1;
    if (startPos > endLimit) {
      return INDEX_NOT_FOUND;
    }
    if (searchStrLen == 0) {
      return startPos;
    }
    for (int i = startPos; i < endLimit; i++) {
      if (regionMatchesAscii(str, true, i, searchStr, 0, searchStrLen)) {
        return i;
      }
    }
    return INDEX_NOT_FOUND;
  }

  /**
   * <p>Finds the first index in the {@code CharSequence} that matches the
   * specified character.</p>
   *
   * @param cs  the {@code CharSequence} to be processed, not null
   * @param searchChar the char to be searched for
   * @param start  the start index, negative starts at the string start
   * @return the index where the search char was found,
   * -1 if char {@code searchChar} is not found or {@code cs == null}
   */
  // -----------------------------------------------------------------------
  public static int indexOf(final CharSequence cs, final char searchChar, int start) {
    if (cs instanceof String) {
      return ((String) cs).indexOf(searchChar, start);
    } else if (cs instanceof ByteBufAsciiString) {
      return ((ByteBufAsciiString) cs).indexOf(searchChar, start);
    }
    if (cs == null) {
      return INDEX_NOT_FOUND;
    }
    return AsciiString.indexOf(cs, searchChar, start);
  }

  private static boolean equalsIgnoreCase(byte a, byte b) {
    return a == b || toLowerCase(a) == toLowerCase(b);
  }

  private static boolean equalsIgnoreCase(char a, char b) {
    return a == b || toLowerCase(a) == toLowerCase(b);
  }

  private static byte toLowerCase(byte b) {
    return isUpperCase(b) ? (byte) (b + 32) : b;
  }

  static char toLowerCase(char c) {
    return isUpperCase(c) ? (char) (c + 32) : c;
  }

  private static byte toUpperCase(byte b) {
    return isLowerCase(b) ? (byte) (b - 32) : b;
  }

  private static boolean isLowerCase(byte value) {
    return value >= 'a' && value <= 'z';
  }

  public static boolean isUpperCase(byte value) {
    return value >= 'A' && value <= 'Z';
  }

  public static boolean isUpperCase(char value) {
    return value >= 'A' && value <= 'Z';
  }

  public static byte c2b(char c) {
    return (byte) ((c > MAX_CHAR_VALUE) ? '?' : c);
  }

  public static byte c2b0(char c) {
    return (byte) c;
  }

  public static char b2c(byte b) {
    return (char) (b & 0xFF);
  }

  @Override
  public int refCnt() {
    return _buf.refCnt();
  }

  @Override
  public ByteBufAsciiString retain() {
    _buf.retain();
    return this;
  }

  @Override
  public ByteBufAsciiString retain(int increment) {
    _buf.retain(increment);
    return this;
  }

  @Override
  public ReferenceCounted touch() {
    _buf.touch();
    return this;
  }

  @Override
  public ReferenceCounted touch(Object hint) {
    _buf.touch(hint);
    return this;
  }

  @Override
  public boolean release() {
    return _buf.release();
  }

  @Override
  public boolean release(int decrement) {
    return _buf.release(decrement);
  }
}

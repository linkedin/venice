package com.linkedin.alpini.base.misc;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.annotation.Nonnull;


/**
 * A class to encapsulate time values along with their measurement units
 * Works for both timestamps as well as time durations.
 * Similar to org.xeril.clock.Timespan but supports nanosecond resolution as well.
 * @author sdas
 *
 */

public final class TimeValue implements Cloneable, Comparable<TimeValue> {
  private static final Supplier<ObjectMapper> JACKSON_MAPPER = SimpleJsonMapper::getObjectMapper;
  private long _rawValue;
  private TimeUnit _unit;

  public TimeValue() {
    _rawValue = -1;
    _unit = null;
  }

  public TimeValue(long rawValue, @Nonnull TimeUnit unit) {
    _unit = Objects.requireNonNull(unit, "unit");
    setRawValue(rawValue);
  }

  public void setRawValue(long rawValue) {
    _rawValue = rawValue;
  }

  /**
   * Get the raw value for this TimeValue using the TimeUnit used during creation
   * @return the raw value for this TimeValue
   */
  public long getRawValue() {
    return _rawValue;
  }

  /**
   * Get the raw value for this TimeValue using the TimeUnit supplied.
   * @param unit      The TimeUnit to be converted to a raw value.
   * @return          The raw value for this TimeValue, in the TimeUnit supplied.
   */
  public long getRawValue(@Nonnull TimeUnit unit) {
    return (unit.convert(getRawValue(), getUnit()));
  }

  public void setUnit(TimeUnit unit) {
    _unit = unit;
  }

  public TimeUnit getUnit() {
    return _unit;
  }

  /**
   * Parse a TimeValue from a serialized string
   * @param timevalueSerialized       A Serialized TimeValue represented as a String.
   * @return                          The TimeValue parsed from the inputted string.
   * @throws JsonMappingException  When there is a problem parsing the input, such as an invalid string.
   */
  public static TimeValue parse(String timevalueSerialized) throws IOException {
    TimeValue parsedTimestamp = JACKSON_MAPPER.get().readValue(timevalueSerialized, TimeValue.class);
    if (parsedTimestamp._rawValue == -1 || parsedTimestamp._unit == null) {
      throw new JsonMappingException("Exception while parsing timevalue");
    }
    return parsedTimestamp;
  }

  /**
   * Emits a json serialized string that represents this TimeValue.
   */
  @Override
  public String toString() {
    try {
      return JACKSON_MAPPER.get().writeValueAsString(this);
    } catch (Exception e) {
      // Should not happen. We know this class can be serialized to JSON.
      throw new Error(e);
    }
  }

  /**
   * Converts this TimeValue to another TimeValue with a different TimeUnit.
   * Useful if you know which unit you want to get values in,
   * irrespective of which unit they were measured in.
   *
   * @param unit   The TimeUnit to be converted.
   * @return       The converted value
   */
  public @Nonnull TimeValue convertTo(@Nonnull TimeUnit unit) {
    if (Objects.requireNonNull(unit, "unit") == getUnit()) {
      return this;
    } else {
      return new TimeValue(getRawValue(unit), unit);
    }
  }

  private static TimeUnit commonUnit(TimeUnit thisUnit, TimeUnit otherUnit) {
    return (thisUnit.ordinal() < otherUnit.ordinal()) ? otherUnit : thisUnit;
  }

  /**
   * Computes the difference between this timevalue and the supplied timevalue.
   * The difference is not absolute, so it is the caller's responsibility to ensure
   * that (this) is newer than the timevalue passed in.
   * @param timevalue  The other TimeValue needed for the difference operation.
   * @return           The difference between this time and the time provided timevalue.
   */
  public @Nonnull TimeValue difference(@Nonnull TimeValue timevalue) {
    if (getUnit() == timevalue.getUnit()) {
      return new TimeValue(getRawValue() - timevalue.getRawValue(), getUnit());
    } else {
      // Using the least accurate measurement to compute difference
      // Warning: Uses ordinal of TimeUnit to determine resolution level
      TimeUnit commonUnit = commonUnit(getUnit(), timevalue.getUnit());
      long high = getRawValue(commonUnit);
      long low = timevalue.getRawValue(commonUnit);
      return new TimeValue(high - low, commonUnit);
    }
  }

  public @Nonnull TimeValue add(TimeValue other) {
    if (other == null || other._unit == null) {
      return this;
    }
    if (getUnit() == other.getUnit()) {
      return new TimeValue(getRawValue() + other.getRawValue(), getUnit());
    } else {
      // Using the least accurate measurement to compute difference
      // Warning: Uses ordinal of TimeUnit to determine resolution level
      TimeUnit commonUnit = commonUnit(getUnit(), other.getUnit());
      long value1 = getRawValue(commonUnit);
      long value2 = other.getRawValue(commonUnit);
      return new TimeValue(value1 + value2, commonUnit);
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    long rawValue = getRawValue();
    result = prime * result + (int) (rawValue ^ (rawValue >>> 32));
    result = prime * result + getUnit().hashCode();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    TimeValue other = (TimeValue) obj;
    return getRawValue() == other.getRawValue() && getUnit() == other.getUnit();
  }

  @Override
  public int compareTo(@Nonnull TimeValue o) {
    return Long.signum(difference(o).getRawValue());
  }

  @Override
  public TimeValue clone() {
    try {
      return (TimeValue) super.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }
}

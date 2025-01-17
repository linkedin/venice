package com.linkedin.venice.sql;

import java.sql.JDBCType;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public class ColumnDefinition {
  @Nonnull
  private final String name;
  @Nonnull
  private final JDBCType type;
  private final boolean nullable;
  @Nullable
  private final IndexType indexType;
  @Nullable
  private final String defaultValue;
  @Nullable
  private final String extra;
  private final int jdbcIndex;

  public ColumnDefinition(@Nonnull String name, @Nonnull JDBCType type, int jdbcIndex) {
    this(name, type, true, null, jdbcIndex);
  }

  public ColumnDefinition(
      @Nonnull String name,
      @Nonnull JDBCType type,
      boolean nullable,
      @Nullable IndexType indexType,
      int jdbcIndex) {
    this(name, type, nullable, indexType, null, null, jdbcIndex);
  }

  public ColumnDefinition(
      @Nonnull String name,
      @Nonnull JDBCType type,
      boolean nullable,
      @Nullable IndexType indexType,
      @Nullable String defaultValue,
      @Nullable String extra,
      int jdbcIndex) {
    this.name = Objects.requireNonNull(name);
    this.type = Objects.requireNonNull(type);
    this.nullable = nullable;
    this.indexType = indexType;
    this.defaultValue = defaultValue;
    this.extra = extra;
    this.jdbcIndex = jdbcIndex;
    if (this.jdbcIndex < 1) {
      throw new IllegalArgumentException("The jdbcIndex must be at least 1");
    }
  }

  @Nonnull
  public String getName() {
    return name;
  }

  @Nonnull
  public JDBCType getType() {
    return type;
  }

  public boolean isNullable() {
    return nullable;
  }

  @Nullable
  public IndexType getIndexType() {
    return indexType;
  }

  @Nullable
  public String getDefaultValue() {
    return defaultValue;
  }

  @Nullable
  public String getExtra() {
    return extra;
  }

  public int getJdbcIndex() {
    return jdbcIndex;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ColumnDefinition that = (ColumnDefinition) o;

    return this.nullable == that.nullable && this.jdbcIndex == that.jdbcIndex && this.name.equals(that.name)
        && this.type == that.type && this.indexType == that.indexType && Objects.equals(defaultValue, that.defaultValue)
        && Objects.equals(extra, that.extra);
  }

  @Override
  public int hashCode() {
    return this.name.hashCode();
  }
}

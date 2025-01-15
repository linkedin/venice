package com.linkedin.venice.sql;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public class TableDefinition {
  @Nonnull
  private final String name;
  @Nonnull
  private final List<ColumnDefinition> columns;
  @Nonnull
  private final List<ColumnDefinition> primaryKeyColumns;

  public TableDefinition(@Nonnull String name, @Nonnull List<ColumnDefinition> columns) {
    this.name = Objects.requireNonNull(name);
    this.columns = Collections.unmodifiableList(columns);
    if (this.columns.isEmpty()) {
      throw new IllegalArgumentException("columns cannot be empty!");
    }
    for (int i = 0; i < this.columns.size(); i++) {
      ColumnDefinition columnDefinition = this.columns.get(i);
      if (columnDefinition == null) {
        throw new IllegalArgumentException(
            "The columns list must be densely populated, but found a null entry at index: " + i);
      }
      int expectedJdbcIndex = i + 1;
      if (columnDefinition.getJdbcIndex() != expectedJdbcIndex) {
        throw new IllegalArgumentException(
            "The columns list must be populated in the correct order, column '" + columnDefinition.getName()
                + "' found at index " + i + " but its JDBC index is " + columnDefinition.getJdbcIndex() + " (expected "
                + expectedJdbcIndex + ").");
      }
    }
    List<ColumnDefinition> pkColumns = new ArrayList<>();
    for (ColumnDefinition columnDefinition: this.columns) {
      if (columnDefinition.getIndexType() == IndexType.PRIMARY_KEY) {
        pkColumns.add(columnDefinition);
      }
    }
    this.primaryKeyColumns = pkColumns.isEmpty() ? Collections.emptyList() : Collections.unmodifiableList(pkColumns);
  }

  @Nonnull
  public String getName() {
    return name;
  }

  @Nonnull
  public List<ColumnDefinition> getColumns() {
    return columns;
  }

  @Nonnull
  ColumnDefinition getColumnByJdbcIndex(int index) {
    if (index < 1 || index > this.columns.size()) {
      throw new IndexOutOfBoundsException("Invalid index. The valid range is: 1.." + this.columns.size());
    }
    return this.columns.get(index - 1);
  }

  @Nullable
  ColumnDefinition getColumnByName(@Nonnull String columnName) {
    Objects.requireNonNull(columnName);
    for (int i = 0; i < this.columns.size(); i++) {
      ColumnDefinition columnDefinition = this.columns.get(i);
      if (columnDefinition.getName().equals(columnName)) {
        return columnDefinition;
      }
    }
    return null;
  }

  @Nonnull
  public List<ColumnDefinition> getPrimaryKeyColumns() {
    return primaryKeyColumns;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TableDefinition that = (TableDefinition) o;

    return this.name.equals(that.name) && this.columns.equals(that.columns)
        && this.primaryKeyColumns.equals(that.primaryKeyColumns);
  }

  @Override
  public int hashCode() {
    return this.name.hashCode();
  }
}

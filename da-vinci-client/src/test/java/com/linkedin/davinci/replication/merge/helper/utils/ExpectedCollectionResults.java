package com.linkedin.davinci.replication.merge.helper.utils;

import java.util.List;
import java.util.Map;


/**
 * A POJO containing collection field names and their corresponding expected collection field values. A "collection"
 * refers to either a Map or a List.
 *
 * @param <T1>
 * @param <T2>
 */
public class ExpectedCollectionResults<T1, T2> {
  private final String mapFieldName;
  private final String listFieldName;
  private final Map<String, T1> expectedMap;
  private final List<T2> expectedList;

  public static <T2> ExpectedCollectionResults createExpectedListResult(String listFieldName, List<T2> expectedList) {
    return new ExpectedCollectionResults<Void, T2>(null, null, listFieldName, expectedList);
  }

  public static <T1> ExpectedCollectionResults createExpectedMapResult(
      String mapFieldName,
      Map<String, T1> expectedMap) {
    return new ExpectedCollectionResults<T1, Void>(mapFieldName, expectedMap, null, null);
  }

  private ExpectedCollectionResults(
      String mapFieldName,
      Map<String, T1> expectedMap,
      String listFieldName,
      List<T2> expectedList) {
    this.mapFieldName = mapFieldName;
    this.expectedMap = expectedMap;
    this.listFieldName = listFieldName;
    this.expectedList = expectedList;
  }

  public String getMapFieldName() {
    return mapFieldName;
  }

  public String getListFieldName() {
    return listFieldName;
  }

  public Map<String, T1> getExpectedMap() {
    return expectedMap;
  }

  public List<T2> getExpectedList() {
    return expectedList;
  }
}

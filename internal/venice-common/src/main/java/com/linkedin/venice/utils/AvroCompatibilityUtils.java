package com.linkedin.venice.utils;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.util.Utf8;


public class AvroCompatibilityUtils {
  /**
   * Backport compareTo() method from SpecificRecordBase.
   */
  public static boolean compare(Object o1, Object o2) {
    return compareSpecificData(o1, o2, ((SpecificRecordBase) o1).getSchema()) == 0;
  }

  /**
   * Backport compare() method implementation from SpecificData in avro v1.7
   */
  public static int compareSpecificData(Object o1, Object o2, Schema s) {
    switch (s.getType()) {
      case ENUM:
        if (o1 instanceof Enum)
          return ((Enum) o1).ordinal() - ((Enum) o2).ordinal();
      default:
        return compareGenericData(o1, o2, s);
    }
  }

  /**
   * Backport compare() method implementation from GenericData in avro v1.7
   */
  public static int compareGenericData(Object o1, Object o2, Schema s) {
    if (o1 == o2)
      return 0;
    switch (s.getType()) {
      case RECORD:
        for (Schema.Field f: s.getFields()) {
          if (f.order() == Schema.Field.Order.IGNORE)
            continue; // ignore this field
          int pos = f.pos();
          String name = f.name();
          int compare = compareGenericData(getField(o1, name, pos), getField(o2, name, pos), f.schema());
          if (compare != 0) // not equal
            return f.order() == Schema.Field.Order.DESCENDING ? -compare : compare;
        }
        return 0;
      case ENUM:
        return s.getEnumOrdinal(o1.toString()) - s.getEnumOrdinal(o2.toString());
      case ARRAY:
        Collection a1 = (Collection) o1;
        Collection a2 = (Collection) o2;
        Iterator e1 = a1.iterator();
        Iterator e2 = a2.iterator();
        Schema elementType = s.getElementType();
        while (e1.hasNext() && e2.hasNext()) {
          int compare = compareGenericData(e1.next(), e2.next(), elementType);
          if (compare != 0)
            return compare;
        }
        return e1.hasNext() ? 1 : (e2.hasNext() ? -1 : 0);
      case MAP:
        Map map1 = (Map) o1;
        Map map2 = (Map) o2;
        if (map1.size() != map2.size())
          return map1.size() - map2.size();
        Set<Map.Entry> set = map1.entrySet();
        for (Map.Entry entry: set) {
          Object value2 = map2.get(entry.getKey());
          if (value2 == null) {
            return -1;
          }
          int compare = compareGenericData(entry.getValue(), value2, s.getValueType());
          if (compare != 0)
            return compare;
        }
        return 0;
      case UNION:
        int i1 = GenericData.get().resolveUnion(s, o1);
        int i2 = GenericData.get().resolveUnion(s, o2);
        return (i1 == i2) ? compareGenericData(o1, o2, s.getTypes().get(i1)) : i1 - i2;
      case NULL:
        return 0;
      case STRING:
        Utf8 u1 = o1 instanceof Utf8 ? (Utf8) o1 : new Utf8(o1.toString());
        Utf8 u2 = o2 instanceof Utf8 ? (Utf8) o2 : new Utf8(o2.toString());
        return u1.compareTo(u2);
      default:
        return ((Comparable) o1).compareTo(o2);
    }
  }

  /**
   * Backport getField() method implementation from GenericData in avro v1.7
   */
  public static Object getField(Object record, String name, int position) {
    return ((IndexedRecord) record).get(position);
  }
}

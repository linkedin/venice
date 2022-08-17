package com.linkedin.venice.client.store.predicate;

import java.util.Arrays;
import java.util.List;
import org.apache.avro.generic.GenericRecord;


public class AndPredicate implements Predicate {
  Predicate[] predicates;

  AndPredicate(Predicate... predicates) {
    this.predicates = predicates;
  }

  @Override
  public boolean evaluate(GenericRecord genericRecord) {
    for (Predicate predicate: predicates) {
      if (!predicate.evaluate(genericRecord)) {
        return false;
      }
    }

    return true;
  }

  public List<Predicate> getChildPredicates() {
    return Arrays.asList(predicates);
  }
}

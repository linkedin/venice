package com.linkedin.davinci.predicate;

import java.util.Arrays;
import java.util.List;
import org.apache.avro.generic.GenericRecord;


class AndPredicate implements Predicate{

  Predicate[] predicates;

  public AndPredicate(Predicate... predicates){
    this.predicates = predicates;
  }

  @Override
  public boolean evaluate(GenericRecord genericRecord) {
    for (Predicate predicate : predicates) {
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

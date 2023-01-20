package com.linkedin.venice.persona;

import java.util.HashSet;
import java.util.Set;


/**
 * A class to represent a Persona, a relationship between stores and a quota system that is enforced.
 * This class is created to be serialized into ZooKeeper.
 */
public class Persona {
  String name;
  long quotaNumber;
  Set<String> storesToEnforce;
  Set<String> owners;

  public Persona(String name, long quotaNumber, Set<String> storesToEnforce, Set<String> owners) {
    this.name = name;
    this.quotaNumber = quotaNumber;
    this.storesToEnforce = storesToEnforce;
    this.owners = owners;
  }

  public Persona(Persona persona) {
    this.name = persona.getName();
    this.quotaNumber = persona.getQuotaNumber();
    this.storesToEnforce = new HashSet<>(persona.getStoresToEnforce());
    this.owners = new HashSet<>(persona.getOwners());
  }

  public Persona() {
  }

  public String getName() {
    return name;
  }

  public long getQuotaNumber() {
    return quotaNumber;
  }

  public Set<String> getStoresToEnforce() {
    return storesToEnforce;
  }

  public Set<String> getOwners() {
    return owners;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setQuotaNumber(long quotaNumber) {
    this.quotaNumber = quotaNumber;
  }

  public void setStoresToEnforce(Set<String> storesToEnforce) {
    this.storesToEnforce = storesToEnforce;
  }

  public void setOwners(Set<String> owners) {
    this.owners = owners;
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof Persona) {
      return equals((Persona) other);
    }
    return false;
  }

  boolean equals(Persona persona) {
    return this.name.equals(persona.name) && this.quotaNumber == persona.quotaNumber
        && this.storesToEnforce.equals(persona.storesToEnforce) && this.owners.equals(persona.owners);
  }

  @Override
  public int hashCode() {
    int result = 1;
    result = result * 31 + this.name.hashCode();
    result = result * 31 + Long.hashCode(quotaNumber);
    result = result * 31 + this.storesToEnforce.hashCode();
    result = result * 31 + this.owners.hashCode();
    return result;
  }
}

package com.linkedin.venice.authorization;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;


/**
 * Represents an Access Control Binding. A {@link Resource} has a list of associated {@link AceEntry} .
 * It allows for existence of duplicate aceEntry.
 */
public class AclBinding {
  private final Resource resource;
  List<AceEntry> aceEntryList;

  /**
   * Creates an Access Control Binding for a given resource with empty AceEntries.
   *
   * @param resource the resource for which this AclBinding is created.
   */
  public AclBinding(Resource resource) {
    this.resource = resource;
    aceEntryList = new LinkedList<>();
  }

  public Resource getResource() {
    return resource;
  }

  /**
   * Returns an unmodifiable list of existing ace entries.
   * @return
   */
  public Collection<AceEntry> getAceEntries() {
    return Collections.unmodifiableCollection(aceEntryList);
  }

  /**
   * Add a list of {@link AceEntry} to the existing list of Aces.
   * @param aceEntries a list of entries to be added.
   */
  public void addAceEntries(Collection<AceEntry> aceEntries) {
    aceEntryList.addAll(aceEntries);
  }

  /**
   * Add one {@link AceEntry} to the existing list of Aces.
   * @param aceEntry The entry to be added.
   */
  public void addAceEntry(AceEntry aceEntry) {
    aceEntryList.add(aceEntry);
  }

  /**
   * Remove one {@link AceEntry} from the existing list of Aces. If there are multiple matching entries only one will be removed.
   * @param aceEntry The entry to be removed.
   */
  public void removeAceEntry(AceEntry aceEntry) {
    aceEntryList.remove(aceEntry);
  }

  /**
   * count the number of AceEntries in the AclBinding
   * @return
   */
  public int countAceEntries() {
    return aceEntryList.size();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{").append(resource).append(", [");
    for (AceEntry ace: aceEntryList) {
      sb.append(ace);
    }
    sb.append("]}");
    return sb.toString();
  }

  @Override
  public int hashCode() {
    int result = 1;
    result = result * 31 + resource.hashCode();
    for (AceEntry aceEntry: aceEntryList) {
      result = result * 31 + aceEntry.hashCode();
    }

    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AclBinding other = (AclBinding) o;
    if (!resource.equals(other.resource)) {
      return false;
    }
    if (aceEntryList.size() != other.aceEntryList.size()) {
      return false;
    }
    for (AceEntry aceEntry: aceEntryList) {
      if (!other.aceEntryList.contains(aceEntry)) {
        return false;
      }
    }

    return true;
  }
}

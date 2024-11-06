package com.linkedin.venice.utils.consistency;

import java.util.List;


public final class DiffValidationUtils {
  private DiffValidationUtils() {
    // not called
  }

  /**
   * This method determines if a given record in a snapshot has been lost.  It determines this by looking at the records
   * ReplicationMetadata and comparing it to the highwatermark of the colo which did not have this record.  If the colo
   * which does not have this record has advanced beyond the offset vector held by the individual record, then we know that
   * this record has been passed by.
   *
   * This function should not be used on any record which holds a tombstone (as the colo which is farther ahead will have
   * purged out a record which currently holds a tombstone).
   * @param existingValueOffsetVector
   * @param nonExistentValuePartitionOffsetWatermark
   * @return True if the record seems to be missing.
   */
  static public boolean isRecordMissing(
      List<Long> existingValueOffsetVector,
      List<Long> nonExistentValuePartitionOffsetWatermark) {
    if (!hasOffsetAdvanced(existingValueOffsetVector, nonExistentValuePartitionOffsetWatermark)) {
      return false;
    }
    return true;
  }

  /**
   * This method determines that if given the values for two equal keys in a snapshot of venice data, have the records diverged.
   * It does this, by determining if each colo has received all data which is pertinent to coming to the current resolution
   * held by each snapshot.  In other words, does each colo have all the same information to come to the same conclusion,
   * and do these conclusions diverge?
   *
   * This method DOES NOT catch divergence for 'missing' records.  Missing records should be determined with {
   * @link #isRecordMissing(List, List)}
   *
   * @param firstValueChecksum                  A checksum or hash that represents the value of the first record in this comparison.
   *                                            The firstValueChecksum should correspond to the secondValueChecksum with the same key.
   * @param secondValueChecksum                 A checksum or hash that represents the value of the second record in this comparison.
   *                                            The secondValueChecksum should correspond to the firstValueChecksum with the same key.
   * @param firstValuePartitionOffsetWatermark  A list of offsets which give the highwatermark of remote consumption for the
   *                                            snapshot of the partition which held the firstValueChecksum.  The list
   *                                            should be ordered with offsets that correspond to colo id's exactly as it's
   *                                            presented in the ReplicationMetadata of venice records.
   * @param secondValuePartitionOffsetWatermark A list of offsets which give the highwatermark of remote consumption for the
   *                                            snapshot of the partition which held the secondValueChecksum.  The list
   *                                            should be ordered with offsets that correspond to colo id's exactly as it's
   *                                            presented in the ReplicationMetadata of venice records.
   * @param firstValueOffsetVector              A list of offsets pulled from the ReplicationMetadata of the first record
   * @param secondValueOffsetVector             A list of offsets pulled from the ReplicationMetadata of the second record
   * @return                                    True if the data seems to have diverged
   */
  static public boolean doRecordsDiverge(
      String firstValueChecksum,
      String secondValueChecksum,
      List<Long> firstValuePartitionOffsetWatermark,
      List<Long> secondValuePartitionOffsetWatermark,
      List<Long> firstValueOffsetVector,
      List<Long> secondValueOffsetVector) {

    // Sanity check, it's possible metadata wasn't emitted, so give this a pass if any of these are blank
    if (firstValuePartitionOffsetWatermark.isEmpty() || secondValuePartitionOffsetWatermark.isEmpty()
        || firstValueOffsetVector.isEmpty() || secondValueOffsetVector.isEmpty()) {
      return false;
    }

    // If the values are the same, then these records do not diverge
    if (firstValueChecksum.equals(secondValueChecksum)) {
      return false;
    }

    // First, we need to determine if enough information has been broad casted. That is, the PartitionOffsetWatermarks
    // for both values need to be greater then all component vector parts of the individual value offsets. E.g.:
    // all entries in secondValuePartitionOffsetWatermark must be greater then all entries in firstValueOffsetVector and
    // all the entries in the firstValuePartitionOffsetWatermark must be greater then the secondValueOffsetVector.
    // we need not compare secondValueOffsetVector to secondValuePartitionOffsetWatermark
    if (!hasOffsetAdvanced(firstValueOffsetVector, secondValuePartitionOffsetWatermark)) {
      return false;
    }

    if (!hasOffsetAdvanced(secondValueOffsetVector, firstValuePartitionOffsetWatermark)) {
      return false;
    }

    // At this time we know the following
    // 1) the values are different
    // 2) Both colos have received enough information to have seen all the pertinent events that caused
    // the rows to converge in this way.
    // These records should have converged, but have not. Therefore, they have diverged!

    return true;
  }

  /**
   * Checks to see if an offset vector has advanced completely beyond some base offset vector or not.
   *
   * @param baseOffset      The vector to compare against.
   * @param advancedOffset  The vector has should be advanced along.
   * @return                True if the advancedOffset vector has grown beyond the baseOffset
   */
  static public boolean hasOffsetAdvanced(List<Long> baseOffset, List<Long> advancedOffset) {
    if (baseOffset.size() > advancedOffset.size()) {
      // the baseoffset has more entries then the advanced one, meaning that it's seen entries from more colos
      // meaning that it's automatically further along then the second argument. We break early to avoid any
      // array out of bounds exception
      return false;
    }
    for (int i = 0; i < baseOffset.size(); i++) {
      if (advancedOffset.get(i) < baseOffset.get(i)) {
        return false;
      }
    }
    return true;
  }
}

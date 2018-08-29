package com.linkedin.venice.kafka.validation;

import com.linkedin.venice.offsets.OffsetRecord;

/**
 * This interface is used by the {@link ProducerTracker} in order to return
 * a closure after performing some data validation logic. There are two
 * motivations for this functional interface:
 *
 * 1. The ProducerTracker API should be designed to have an all or nothing
 *    policy for mutating an {@link OffsetRecord}. This makes it easier to
 *    reason about the state of an OffsetRecord, since it represents exactly
 *    one offset. For example, it should not be possible to update the
 *    segment but fail to update the sequence number (both should be updated).
 *    By not taking in an OffsetRecord as a parameter, but rather returning a
 *    closure, the ProducerTracker can apply all of its changes to the
 *    OffsetRecord only as part of the execution of that closure.
 * 2. It gives more control to the caller of the ProducerTracker. The design
 *    of the API gives the caller the option of applying the change to the
 *    OffsetRecord or not, as it sees fit.
 */
public interface OffsetRecordTransformer {
  OffsetRecord transform(OffsetRecord offsetRecord);
}

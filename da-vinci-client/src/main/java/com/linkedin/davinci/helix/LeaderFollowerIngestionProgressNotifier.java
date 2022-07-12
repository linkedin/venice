package com.linkedin.davinci.helix;

/**
 * This class used to override {@link #catchUpVersionTopicOffsetLag(String, int)} to
 * release the latch when the resource is the current version. We remove the logic
 * due to a potential race condition between latch being released and push being
 * completed.
 *
 * As for now, it's a placeholder for any future modification for L/F state model.
 * TODO: remove the class when every store is running in L/F model!
 */
public class LeaderFollowerIngestionProgressNotifier extends StateModelIngestionProgressNotifier {
}

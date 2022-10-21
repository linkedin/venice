----------------------- MODULE MCAbstractVeniceLeaderFollower -----------------
EXTENDS AbstractVeniceLeaderFollower

CONSTANT MAX_WRITES

NotAllWritesTransmitted ==
    Len(realTimeTopic) <= MAX_WRITES

  ====
----------------------- MODULE MCAbstractVeniceLeaderFollower -----------------
EXTENDS AbstractVeniceLeaderFollower

CONSTANT MAX_WRITES

AllWritesTransmitted ==
    Len(realTimeTopic) <= MAX_WRITES

Terminating ==
  /\ AllWritesTransmitted

  ====
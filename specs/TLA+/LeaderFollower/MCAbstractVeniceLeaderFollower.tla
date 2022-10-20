----------------------- MODULE MCAbstractVeniceLeaderFollower -----------------
EXTENDS AbstractVeniceLeaderFollower

AllWritesTransmitted ==
    Len(realTimeTopic) <= MAX_WRITES

AllVersionPushesTried ==
    Len(versions) <= MAX_VERSION_PUSHES

Terminating ==
  /\ AllWritesTransmitted
  /\ AllVersionPushesTried

  ====
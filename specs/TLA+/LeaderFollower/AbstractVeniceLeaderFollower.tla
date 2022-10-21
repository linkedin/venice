----------------------- MODULE AbstractVeniceLeaderFollower -----------------
(***************************************************************************)
(* This module describes the behavior of the Venice Leader follower model  *)
(* in abstract terms for a single partition.  Given at least one client    *)
(* writer, a venice leader should be at some future state process all      *)
(* writes to a given partition and persist them to a queue to be consumed  *)
(* by a set of follower nodes in such a way that all replicas become       *)
(* consistent at some state. The only concrete detail we model here is     *)
(* that the transmission channel between a client and venice is that the   *)
(* channel is asynchronous and non destructive.                            *)
(***************************************************************************)

EXTENDS Integers, Sequences
CONSTANTS KEYS, VALUES, N_NODES

(***************************************************************************)
(* Variables which define the spec and parts of the architecture:          *)
(*                                                                         *)
(* realTimeTopic: A queue of writes transmitted to Venice directly from    *)
(*                clients as key value pairs.  This queue is partitioned   *)
(*                in production, and this spec models a single partition.  *)
(*                                                                         *)
(* versionTopic: A queue of committed writes which is a tuple of key,      *)
(*               value, and metadata for the system.  In this basic spec,  *)
(*               it's the offset of the write in the realTimetopic which   *)
(*               corresponds to this write in the version topic. We also   *)
(*               use this variable to signify the 'current' version in     *)
(*               Venice, which is to say, it's the version currently       *)
(*               serving client reads and having client writes added to    *)
(*               it. Similar to the realTimeTopic, this versionTopic is    *)
(*               also partitioned.                                         *)
(*                                                                         *)
(* nodes: A set of replica serving nodes in Venice.                        *)
(*                                                                         *)
(***************************************************************************)

VARIABLE realTimeTopic, versionTopic, nodes

vars == <<realTimeTopic, versionTopic, nodes>>

(***************************************************************************)
(* Each replica serving node in Venice has a unique identifier, and for a  *)
(* given replica an assigned state of either 'LEADER' or 'FOLLOWER'.       *)
(***************************************************************************)
nodeIds == 1..N_NODES
LEADER == "LEADER"
FOLLOWER == "FOLLOWER"

(***************************************************************************)
(* PROPERTY:                                                               *)
(*                                                                         *)
(* It's alright when we have all writes. The only property of this spec is *)
(* that all replicas will eventually become consistent with each other.    *)
(***************************************************************************)

ReplicasConsistent ==
    \A n1, n2 \in nodeIds:
    nodes[n1].persistedRecords = nodes[n2].persistedRecords

EventuallyConsistent == <>[]ReplicasConsistent

(***************************************************************************)
(* All writes to Venice are asynchronous. A client writes to a queue that  *)
(* we call a 'realTimeTopic' which is then consumed by Venice and applied  *)
(***************************************************************************)

ClientProducesToVenice ==
    /\ \E <<k, v>> \in KEYS \X VALUES :
            realTimeTopic' = Append(realTimeTopic, <<k, v>>)
    /\ UNCHANGED <<nodes, versionTopic>>

(***************************************************************************)
(* For each write, we only retain the last value for a given key when      *)
(* applying the write to a replica.                                        *)
(***************************************************************************)

SetValueOnReplica(nodeId, k, v) ==
    {<<kp, vp>> \in nodes[nodeId].persistedRecords: kp \notin {k}} \union {<<k,v>>}

(***************************************************************************)
(* This abstract implementation assumes that the a replica is able to      *)
(* consume a record from a topic, apply it to it's local state, and        *)
(* optionally produce in a single discrete step. This may not be the case  *)
(* in reality as a node state might change before it can accomplish all    *)
(* these steps.  A refinement of this spec should look to override these   *)
(* methods in order to simulate situations where interleaving events in    *)
(* between those steps simulates edge cases we're worried about.           *)
(*                                                                         *)
(* When a leader consumes from the realTimeTopic, it applies the event to  *)
(* it's local state, produces the event to the versionTopic with the       *)
(* offset of the event in the realTimeTopic which triggered this event,    *)
(* and then updates it's highwatermark states with how far along it is in  *)
(* consuming this realTimeTopic partition.                                 *)
(***************************************************************************)
RealTimeConsume(nodeId) ==
    /\ IF nodes[nodeId].rtOffset <= Len(realTimeTopic)
        THEN
            nodes' = [nodes EXCEPT
            ![nodeId].rtOffset = nodes[nodeId].rtOffset+1,
                ![nodeId].persistedRecords = SetValueOnReplica(nodeId,
                                            realTimeTopic[nodes[nodeId].rtOffset][1],
                                            realTimeTopic[nodes[nodeId].rtOffset][2])
                ]
            /\ versionTopic' = Append(versionTopic,
                <<realTimeTopic[nodes[nodeId].rtOffset][1],
                realTimeTopic[nodes[nodeId].rtOffset][2],
                nodes[nodeId].rtOffset>>)
        ELSE UNCHANGED vars
    /\ UNCHANGED <<realTimeTopic>>

(***************************************************************************)
(* Both a leader or a follower may consume from a versionTopic.  When      *)
(* consuming from the version topic, a participating node applies the      *)
(* incoming write locally, and persists the offset of the lase message it  *)
(* consumed.  It also uses the realTimeTopic offset in the versionTopic    *)
(* message to update it's realTimeTopic offset checkpoint so as to avoid   *)
(* doing duplicate work should the node become leader at a later state.    *)
(***************************************************************************)
VersionTopicConsume(nodeId) ==
    /\ IF nodes[nodeId].vtOffset <= Len(versionTopic)
        THEN
            nodes' = [nodes EXCEPT
            ![nodeId].vtOffset = nodes[nodeId].vtOffset+1,
                ![nodeId].persistedRecords = SetValueOnReplica(nodeId,
                                            versionTopic[nodes[nodeId].vtOffset][1],
                                            versionTopic[nodes[nodeId].vtOffset][2])
                ]
        ELSE UNCHANGED vars
    /\ UNCHANGED <<realTimeTopic, versionTopic>>

(***************************************************************************)
(* Leaders have two potential internal states.  They are either 'catching  *)
(* up' or 'consuming rt'. Catchup is defined to mean that the leader needs *)
(* to consume whatever is in the VT that it hasn't already from previously *)
(* being a follower node.  This is important because unless the leader     *)
(* wants to consume the entirety of the realtime topic every time it's     *)
(* promoted, it needs to make sure it ingests whatever was populated in    *)
(* order to become eventually consistent.  Once it's caught up, a leader   *)
(* will consume events out of the realtime topic and into the version      *)
(* topic.                                                                  *)
(***************************************************************************)
LeaderConsume ==
    /\ \E leaderNodeId \in {x \in DOMAIN nodes: nodes[x].state = LEADER}:
        IF nodes[leaderNodeId].vtOffset <= Len(versionTopic)
        THEN VersionTopicConsume(leaderNodeId)
        ELSE RealTimeConsume(leaderNodeId)

(***************************************************************************)
(* Followers are relatively simple.  They consume data out of the          *)
(* versionTopic and apply it to their local state.                        *)
(***************************************************************************)
FollowerConsume ==
    /\ \E followerNodeId \in {x \in DOMAIN nodes: nodes[x].state = FOLLOWER}:
        VersionTopicConsume(followerNodeId)

(***************************************************************************)
(* Leader promotion/demotion are not discrete in reality. A refinement can *)
(* override these methods and put in some extra states to simulate cases   *)
(* of catchup or multiple leaders.                                         *)
(***************************************************************************)
ChangeReplicaState(node, newState) ==
    /\ nodes' = [nodes EXCEPT ![node].state = newState]
    /\ UNCHANGED <<realTimeTopic, versionTopic>>

PromoteLeader ==
    IF {x \in DOMAIN nodes: nodes[x].state = LEADER} = {}
    THEN \E leaderNodeId \in {x \in DOMAIN nodes: nodes[x].state = FOLLOWER}:
        ChangeReplicaState(leaderNodeId, LEADER)
    ELSE FALSE

DemoteLeader ==
    /\ \E followerNodeId \in {x \in DOMAIN nodes: nodes[x].state = LEADER}:
        ChangeReplicaState(followerNodeId, FOLLOWER)


Next ==
    \/ ClientProducesToVenice
    \/ LeaderConsume
    \/ FollowerConsume
    \/ DemoteLeader
    \/ PromoteLeader

Init ==
  /\ realTimeTopic = <<>>
  /\ versionTopic = <<>>
  /\ nodes = [i \in nodeIds |->
    [ state |-> FOLLOWER,
    rtOffset |-> 1,
    vtOffset |-> 1,
    persistedRecords |-> {}]]

Spec == Init /\ [][Next]_vars /\ SF_vars(FollowerConsume) /\ WF_vars(LeaderConsume)

====
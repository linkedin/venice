----------------------------- MODULE leapfrog -------------------------------
(***************************************************************************)
(* LeapFrog is an algorithm that is meant to detect data divergence of a   *)
(* pair of active replicas for a datastore. It is able to take advantage   *)
(* of the efficiencies of batch processing of incremental changes in       *)
(* either a streaming environment, or, a map reduce environment. It treats *)
(* the upstream datastore (for the most part) as a black box. It does not  *)
(* make assumptions about any conflict resolution strategy or consensus    *)
(* protocols which might be employed, therefore, it does not make          *)
(* determinations on the correctness of those strategies. This algorithm   *)
(* is only capable of making judgements on the consistency of COMMITTED    *)
(* data of replicas.                                                       *)
(*                                                                         *)
(* COMMITED data is defined as data which has undergone conflict           *)
(* resolution/concensus and been written to a write ahead log (WAL)        *)
(*                                                                         *)
(* We also assume that active replicas of the store leverage a WAL where   *)
(* committed data is recorded.  Events written to this WAL have a          *)
(* coordinate system where events in a WAL can be causally compared (given *)
(* two events, we can determine which event got committed before the       *)
(* other) and we can trace where these writes originated from. As an       *)
(* example, given an entry in a WAL by looking at the entries metadata we  *)
(* can determine the order in which it was committed to the attached       *)
(* replica relative to other commits, and we can determine it's ordering   *)
(* relative to events committed from it's original source replica.         *)
(***************************************************************************)

EXTENDS Naturals, Sequences

(***************************************************************************)
(* We first declare the following inputs to represent our replicating      *)
(* environment:                                                            *)
(***************************************************************************)
(***************************************************************************)
(* COLOS: A set of active replicas that are applying updates               *)
(***************************************************************************)
(***************************************************************************)
(* KEYS: The keyspace for updates being applied to active replicas         *)
(***************************************************************************)
(***************************************************************************)
(* VALUES: The set of values to write for those keys                       *)
(***************************************************************************)
(***************************************************************************)
(* MAX_WRITES: An upper bound on the number of events we write (just to    *)
(*             keep runtime of the model checker within reason)            *)
(***************************************************************************)

CONSTANTS COLOS, KEYS, VALUES, MAX_WRITES

(***************************************************************************)
(* Global Variables                                                        *)
(***************************************************************************)
(* A pair of collections to represent the WAL's for the active replicas we *)
(* mean to compare. Each collection is an ordered list of updates to a     *)
(* given replica of the upstream store.                                    *)
(* This variable is a function with a domain of coloA and coloB meant to   *)
(* represent the two replicas we're comparing.                             *)
VARIABLE WALs

(* A set of monotonically increasing counters for each event committed by  *)
(* each replica.  In the Venice implementation, this corresponds to the    *)
(* incrementing offsets given to messages in the kafka real time topic. In *)
(* MysQL it would correspond to binlog GTID's                              *)
VARIABLE coloCounters

(* A boolean not used for implementation, but used in this specification.  *)
(* This is meant do denote that in a given run, data was not symmetrically *)
(* delivered to a replica, therefore, the checker MUST flag this.  If it's *)
(* false, the checker MUST NOT flag this.                                  *)
VARIABLE dataDiverges

(* A boolean meant to represent the flag the consistency checker will mark *)
(* once it detects that data has diverged.                                 *)
VARIABLE divergenceDetected

(* A pair of key value stores meant to simulate the replicas we mean to    *)
(* compare.  Each update delivered will merge the record and record        *)
(* metadata and add an event to it's respective WAL.                       *)
VARIABLE keyValueStores

(* In the implementation of this algorithm, we perform a stepwise          *)
(* computation of data in a WAL at a given time.  To get equivalent        *)
(* behavior, we'll increment a token per destination replica in order to   *)
(* represent a snapshot in time of the pair of WALs. We'll advance this    *)
(* token in one of the two comparingReplicas with each check.              *)
VARIABLE leapFrogIndexes

(* As leap frog scans WAL entries, it maintains a pair of vectors where    *)
(* each component part of the vector is the high water mark of a given WAL *)
(* coordinate for a given active replica that has been thus far scanned.   *)
(* watermark of a source replica write.                                    *)
VARIABLE leapFrogHighWaterMarks

(* A pair of maps keep the last update in a given WAL to each key as       *)
(* of the position of the last leapFrogIndexes.  Functionally, it's a way  *)
(* to track intermediate states for each key in a replica.                 *)
VARIABLE leapFrogLatestUpdates

(* The combined key space including control keys (KEYS \union controlKey)  *)
VARIABLE KEY_SPACE

(* A simple counter of submitted writes meant to track termination in TLC  *)
VARIABLE WRITES

(* This spec assumes that we are comparing two replicas at a time.         *)
comparingReplicas == {"coloA", "coloB"}

(* These are special keys/values that we leverage to deal with the edge    *)
(* case of a dropped write being the last write to one of a pair of        *)
(* replicas. It's assumed that there will always be another write in most  *)
(* production use cases, but in order to account for all scenarios in TLC  *)
(* we must account for this by writing a new event to each replica as a    *)
(* kind of 'capstone' write when initiating a comparison.  In a production *)
(* environment it is likely not needed to impelemnt this as part of the    *)
(* the consistency checker.  Automatic replica heartbeats which exist in   *)
(* most replicating sytems can fill the same role and catch such cases as  *)
(* a completely broken replicating pipeline.                               *)
controlKey == "controlKey"
controlValue == "controlValue"

(* This is a special value that is meant to be temporary.  It is           *)
(* committed, and then overriden and is meant to simulate a system where   *)
(* there is an eventually consistent model in play (data may temporarily   *)
(* diverge but eventually will become consistent).  Usually this means     *)
(* that we'll see asymmetric WAL entries (one commit in one colo, and two  *)
(* commits in another).  Such an event should not be flagged as an         *)
(* an inconsistency as it's part of normal operation.                      *)
temporaryValue == "temporaryRecord"

(* Default value placeholder for initialization                            *)
null == "null"

(* Enumeration of variables for TLC state tracking                         *)
vars ==
    <<WALs, coloCounters, dataDiverges, divergenceDetected, keyValueStores,
    KEY_SPACE, leapFrogLatestUpdates, leapFrogIndexes,
    leapFrogHighWaterMarks, WRITES>>

(* Variables which only change when we run leapfrog comparison             *)
leapfrogVars == <<leapFrogLatestUpdates, leapFrogIndexes,
            leapFrogHighWaterMarks, divergenceDetected>>
----
(***************************************************************************)
(*               Properties:                                               *)
(***************************************************************************)
(* Live: If data diverges, then we should detect it eventually and not     *)
(* flip the state back.                                                    *)
Live ==
    dataDiverges ~> []divergenceDetected

(* Safe: we should never detect divergence when there is none at any state *)
Safe ==
    [](divergenceDetected => dataDiverges)
----
(**************************************************************************)
(* KV Store Helper functions                                              *)
(**************************************************************************)
(* These Helpers describe a key value store with the following behaviors: *)
(*                                                                        *)
(* Updates/Inserts to a key are accompanied by metadata which designates  *)
(* a source replica and a monotonically increasing counter with values    *)
(* that are not reused across writes from a given replica. When a key is  *)
(* updated, metadata for that update is merged with the existing record   *)
(* maintaining the highest counter value from each active replica that    *)
(* has applied an update on this record.                                  *)
(*                                                                        *)
(* This merge behavior is described with MergeRowReplicaCounters which   *)
(* returns the union of DOMAIN of two functions and keeps the greater of  *)
(* each RANGE value                                                       *)
(* Ex: if r1=[lor1 |-> 20, lva1 |-> 10]                                   *)
(*        r2=[lor1 |-> 9, lva1 |-> 15, ltx1 |-> 5]                        *)
(*                                                                        *)
(*     Then return [lor1 |-> 20, lva1 |-> 15, ltx1 |-> 5]                 *)
(**************************************************************************)
MergeRowReplicaCounters(r1, r2) ==
    LET D1 == DOMAIN r1
        D2 == DOMAIN r2 IN
    [k \in (D1 \cup D2) |->
        IF k \in D1 THEN
            IF k \in D2
            THEN IF r1[k] > r2[k] THEN r1[k] ELSE r2[k]
            ELSE r1[k]
        ELSE r2[k]
    ]

(**************************************************************************)
(* Merge together two KV records based on the semantic that a new update  *)
(* should have it's value overwrite from the previous one, and then we    *)
(* merge the metadata of the new value with the old one with              *)
(**************************************************************************)
MergeKVRecords(r1, r2) ==
    [value |-> r2.value, replicaCounters |->
    MergeRowReplicaCounters(r1.replicaCounters, r2.replicaCounters)]

(**************************************************************************)
(* Build a record which conforms to the structure we expect when parsing  *)
(* WAL events                                                             *)
(**************************************************************************)
BuildWALRecord(newKey, newValue, newreplicaCounters) ==
    [key |-> newKey, value |-> newValue, replicaCounters |-> newreplicaCounters]

(***************************************************************************)
(* Write a replicated event to each replica.  This is used to simulate an  *)
(* update getting applied to an active replica and then replicating to     *)
(* the two active replicas we are comparing                                *)
(***************************************************************************)
SymmetricWrite(sourceColo, newKey, newValue) ==
    /\ keyValueStores' = [ keyValueStores EXCEPT
        !.coloA[newKey].value = newValue,
        !.coloA[newKey].replicaCounters[sourceColo] = coloCounters[sourceColo],
        !.coloB[newKey].value = newValue,
        !.coloB[newKey].replicaCounters[sourceColo] = coloCounters[sourceColo]]
    /\ WALs' = [WALs EXCEPT
                !.coloA = Append(WALs.coloA, [key |-> newKey,
                    value |-> keyValueStores.coloA[newKey].value',
                    replicaCounters |-> keyValueStores.coloA[newKey].replicaCounters']),
                !.coloB = Append(WALs.coloB, [key |-> newKey,
                    value |-> keyValueStores.coloB[newKey].value',
                    replicaCounters |-> keyValueStores.coloB[newKey].replicaCounters'])]
    /\ coloCounters' = [coloCounters EXCEPT
        ![sourceColo] = coloCounters[sourceColo] + 1]
    /\ UNCHANGED dataDiverges

(***************************************************************************)
(* This is where things go wrong.  This write is meant to simulate a bug   *)
(* in the consistency model of this store and results in divergent data.   *)
(* We model this as a situation where a write is applied to only one       *)
(* but it does not replicate.                                              *)
(***************************************************************************)
AsymmetricWrite(sourceColo, newKey, newValue) ==
    /\ keyValueStores' = [ keyValueStores EXCEPT
        !.coloA[newKey].value = "EVILPOISONPILLVALUE",
        !.coloA[newKey].replicaCounters[sourceColo] = coloCounters[sourceColo]]
    /\ WALs' = [WALs EXCEPT
                !.coloA = Append(WALs.coloA, [key |-> newKey,
                value |-> keyValueStores.coloA[newKey].value',
                replicaCounters |-> keyValueStores.coloA[newKey].replicaCounters'])]
    /\ coloCounters' = [coloCounters EXCEPT
        ![sourceColo] = coloCounters[sourceColo] + 1]
    /\ dataDiverges'=TRUE

(**************************************************************************)
(* This function applies two conflicting writes to the same key to two    *)
(* replicas, and then immediately overrides one of the keys in one of the *)
(* repolicas with the write from the other replica.  The effect of this   *)
(* is that data converges for the two replicas,  but, there is an         *)
(* asymmetric history of writes for the two replicas recorded in the two  *)
(* WAL's. This scenario is typical of data systems which utilize async    *)
(* replication and eventual consistency.  Leap Frog should observe these  *)
(* differences, but recognize that they don't mean that the replicas      *)
(* reached a state of permanent inconsistency.  Rather, it treats this as *)
(* normal operation, and does not flag it.                                *)
(**************************************************************************)
EventuallyConsistentWrite(sourceColo, conflictingColo, newKey, newValue) ==
    /\ keyValueStores' = [ keyValueStores EXCEPT
        !.coloA[newKey].value = newValue,
        !.coloA[newKey].replicaCounters[sourceColo] = coloCounters[sourceColo],
        !.coloA[newKey].replicaCounters[conflictingColo] = coloCounters[conflictingColo],
        !.coloB[newKey].value = newValue,
        !.coloB[newKey].replicaCounters[sourceColo] = coloCounters[sourceColo]]
    /\ WALs' = [WALs EXCEPT
        !.coloA = WALs.coloA \o
            <<BuildWALRecord(newKey, temporaryValue,
                MergeRowReplicaCounters(keyValueStores.coloA[newKey].replicaCounters,
                    [b \in {conflictingColo} |-> coloCounters[conflictingColo]]))
            ,BuildWALRecord(newKey, newValue, keyValueStores.coloA[newKey].replicaCounters')>>,
        !.coloB = WALs.coloB \o
            <<BuildWALRecord(newKey, newValue, keyValueStores.coloB[newKey].replicaCounters')>>]
    /\ coloCounters' = [coloCounters EXCEPT
        ![sourceColo] = coloCounters[sourceColo] + 1]
    /\ UNCHANGED dataDiverges

(***************************************************************************)
(* Write an event to each replica that increments the counter from each    *)
(* source replica.  A production equivalent would be a set of heartbeats   *)
(* from all active source replicas getting written to the pair of replicas *)
(* that we're comparing                                                    *)
(***************************************************************************)
SymmetricWriteWithFullVector(newKey, newValue) ==
    /\ keyValueStores' = [ keyValueStores EXCEPT
        !.coloA[newKey].value = newValue,
        !.coloA[newKey].replicaCounters = coloCounters,
        !.coloB[newKey].value = newValue,
        !.coloB[newKey].replicaCounters = coloCounters]
    /\ WALs' = [WALs EXCEPT
                !.coloA = Append(WALs.coloA, [key |-> newKey,
                    value |-> keyValueStores.coloA[newKey].value',
                    replicaCounters |-> keyValueStores.coloA[newKey].replicaCounters']),
                !.coloB = Append(WALs.coloB, [key |-> newKey,
                    value |-> keyValueStores.coloB[newKey].value',
                    replicaCounters |-> keyValueStores.coloB[newKey].replicaCounters'])]
    /\ coloCounters' = [k \in COLOS |-> coloCounters[k] + 1]
    /\ UNCHANGED dataDiverges

----
(**************************************************************************)
(* LeapFrog functions                                                     *)
(**************************************************************************)
(* These functions describe the core steps of the leap frog algorithm     *)
(**************************************************************************)

(**************************************************************************)
(* Based on the leap frog state variables, determine if divergence is     *)
(* detected. Records diverge iff ALL of the following criteria are met:   *)
(**************************************************************************)
(* 1. Records share a key.  Records which don't share a key are not       *)
(*    comparable.                                                         *)
(* 2. Values are not the same.  If values are the same for a key, then    *)
(*    they don't diverge. An implementation should define value as the    *)
(*    user data as well as any metadata which may will effect system      *)
(*    behavior.  Things like timestamps and TTL or whatever metadata      *)
(*    which might effect future rounds of conflict resolution).           *)
(* 3. If the high watermark of observed offsets for all writers to the    *)
(*    first replica is greater then the offsets recorded for all writers  *)
(*    have touched this record in the second replica.  Another way to put *)
(*    it, the first replica has seen at least all events which have       *)
(*    touched this record in the second replica.                          *)
(* 3. If the high watermark of observed offsets for all writers to the    *)
(*    second repolica is greater then the offsets recorded for all all    *)
(*    writers that have touched this record in the first repolica.        *)
(*    Another way to put it, the second repolica has seen at least all    *)
(*    events which have touched this record in the first replica.         *)
(**************************************************************************)
DoRecordsDiverge(keyA, keyB, valueA, valueB, HWMreplicaCountersA,
                HWMreplicaCountersB, keyreplicaCountersA, keyreplicaCountersB) ==
    /\ keyA = keyB
    /\ valueA # valueB
    /\ \A x \in (DOMAIN HWMreplicaCountersA):
        HWMreplicaCountersA[x]  <= keyreplicaCountersB[x]
    /\ \A x \in (DOMAIN HWMreplicaCountersA):
        HWMreplicaCountersB[x]  <= keyreplicaCountersA[x]


(**************************************************************************)
(* At a given step, update all states.  This includes adjusting the       *)
(* observed high water marks for each WAL at the current WAL position as  *)
(* well as the state of those keys at that position.  We also increment   *)
(* the index at which we've advanced in a given WAL based on which        *)
(* high water mark has advanced completely ahead of the other.  That is   *)
(* if one high watermark for a given replica is ahead for all components  *)
(* then we advance the token of the one that is behind.  Otherwise we     *)
(* advance the other.  We don't advance any index beyond the length of    *)
(* either WAL                                                             *)
(**************************************************************************)
UpdateLeapFrogStates ==
    /\ leapFrogHighWaterMarks' = [leapFrogHighWaterMarks EXCEPT
        !.coloA = MergeRowReplicaCounters(leapFrogHighWaterMarks.coloA,
            WALs.coloA[leapFrogIndexes.coloA].replicaCounters),
        !.coloB = MergeRowReplicaCounters(leapFrogHighWaterMarks.coloB,
            WALs.coloB[leapFrogIndexes.coloB].replicaCounters)]
    /\ leapFrogLatestUpdates' = [leapFrogLatestUpdates EXCEPT
        !.coloA[WALs.coloA[leapFrogIndexes.coloA].key] =
            [value |-> WALs.coloA[leapFrogIndexes.coloA].value,
            replicaCounters |-> WALs.coloA[leapFrogIndexes.coloA].replicaCounters],
        !.coloB[WALs.coloB[leapFrogIndexes.coloB].key] =
            [value |-> WALs.coloB[leapFrogIndexes.coloB].value,
            replicaCounters |-> WALs.coloB[leapFrogIndexes.coloB].replicaCounters]]
    /\ IF \A n \in DOMAIN leapFrogHighWaterMarks.coloA':
        leapFrogHighWaterMarks.coloA[n]' > leapFrogHighWaterMarks.coloB[n]'
       THEN leapFrogIndexes' =
        [leapFrogIndexes EXCEPT !.coloB =
            IF leapFrogIndexes.coloB >= Len(WALs.coloB)
            THEN leapFrogIndexes.coloB
            ELSE leapFrogIndexes.coloB + 1]
       ELSE leapFrogIndexes' =
        [leapFrogIndexes EXCEPT !.coloA =
            IF leapFrogIndexes.coloA >= Len(WALs.coloA)
            THEN leapFrogIndexes.coloA
            ELSE leapFrogIndexes.coloA + 1]


(**************************************************************************)
(* Optional for production environments, delivers a capstone write to all *)
(* replicas                                                               *)
(**************************************************************************)
DeliverControlWrite ==
    SymmetricWriteWithFullVector(controlKey, controlValue)

(**************************************************************************)
(* Run the comparison for a given step.  Here we deliver our control      *)
(* write to all replicas, update our states, and then check if we have    *)
(* detected divergence and flag it.                                       *)
(**************************************************************************)
LeapFrogCompare ==
    /\ DeliverControlWrite
    /\ divergenceDetected' =
        \E n \in KEY_SPACE : DoRecordsDiverge(n, n,
            leapFrogLatestUpdates.coloA[n].value,
            leapFrogLatestUpdates.coloB[n].value,
            leapFrogHighWaterMarks.coloA,
            leapFrogHighWaterMarks.coloB,
            leapFrogLatestUpdates.coloA[n].replicaCounters,
            leapFrogLatestUpdates.coloB[n].replicaCounters)
    /\ UpdateLeapFrogStates
    /\ UNCHANGED <<KEY_SPACE, WRITES, dataDiverges>>

----
(**************************************************************************)
(* Control Functions                                                      *)
(**************************************************************************)

(* Selects a colo from the set of colos to compete with a given colo      *)
SelectCompeteingColo(colo) ==
    CHOOSE otherColo \in COLOS \ {colo} : TRUE

(**************************************************************************)
(* Write a new random record to a random colo and randomly choose it to   *)
(* to be a symmetric write, an asymmetric write (bug) or an eventually    *)
(* consistent write                                                       *)
(**************************************************************************)
DeliverWrite ==
    /\ WRITES' = WRITES + 1
    /\ \E <<n, k, v>> \in COLOS \X KEYS \X VALUES :
        /\ \/ SymmetricWrite(n, k, v)
           \/ AsymmetricWrite(n, k, v)
           \/ EventuallyConsistentWrite(n, SelectCompeteingColo(n), k, v)
    /\ UNCHANGED KEY_SPACE
    /\ UNCHANGED leapfrogVars

Next ==
    \/ DeliverWrite
    \/ LeapFrogCompare

Init ==
    /\ WRITES = 0
    /\ KEY_SPACE = KEYS \union {controlKey}
    /\ coloCounters = [i \in COLOS |-> 1]
    /\ dataDiverges = FALSE
    /\ divergenceDetected = FALSE
    /\ WALs = [i \in comparingReplicas |-> <<[key |-> controlKey,
        value |-> controlValue, replicaCounters |-> [k \in COLOS |-> 0]]>>]
    /\ leapFrogHighWaterMarks = [i \in comparingReplicas |-> [k \in COLOS |-> 0]]
    /\ leapFrogIndexes = [i \in comparingReplicas |-> 1]
    /\ leapFrogLatestUpdates = [colo \in comparingReplicas |-> [ j \in KEY_SPACE |-> [value |->
        null, replicaCounters |-> [k \in COLOS |-> 0]]] ]
    /\ keyValueStores = [colo \in comparingReplicas |-> [ j \in KEY_SPACE |-> [value |->
        null, replicaCounters |-> [k \in COLOS |-> 0]]] ]

Spec == Init /\ [][Next]_vars /\ WF_vars(LeapFrogCompare)

====
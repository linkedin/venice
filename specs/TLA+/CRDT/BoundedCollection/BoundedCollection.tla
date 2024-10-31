---------------------------- MODULE BoundedCollection ------------------------------
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS WRITES, MAX_ELEMENTS, SETS, DELETES

VARIABLE databaseResult, pendingOperations, expectedResult, tombstones,
          databaseRowTimestamp

vars == <<databaseResult, pendingOperations, expectedResult, tombstones,
          databaseRowTimestamp>>

\* ***********************************************************************
\* Utility Functions
\* ***********************************************************************
\* These are utility functions for computing an expected result based on 
\* the configured operations. We give preference to an order of operations 
\* where we consume the writes, the deletes, and then the set operations. 
\* The spec requires these operations to be commutative, so giving this 
\* preference is legitimate since all permutations of the order of these 
\* operations should yield the same result.

\* A set operation clears the list at a certain timestamp.
FilterSets(entries) == 
    { x \in entries : \A y \in SETS : x[2] > y[2] }

\* This removes entries from a set if there is a corresponding delete 
\* operation for the key of that entry with a higher timestamp.
FilterDeletes(entries) == 
    { x \in entries : \A y \in DELETES : ~(x[1] = y[1] /\ x[2] <= y[2]) }

\* This returns the set of all writes for all keys with the highest timestamp.
RetainLargestWrites == 
    { x \in WRITES : \A y \in WRITES : ~(x[1] = y[1]) \/ x[2] >= y[2] }

\* This returns the top MAX_ELEMENTS of all writes for all keys with the 
\* highest timestamp and value (aka highest DCR result).
FilterWritesWithMaxSize(writes) == 
    { x \in writes : Cardinality({ y \in writes : 
      y[2] > x[2] \/ (y[2] = x[2] /\ y[1] > x[1]) }) < MAX_ELEMENTS }

\* We have two definitions of ComputeExpectedResult based on if this array
\* has a bound or not. A bound that is zero or less means there is no bound
\* on this collection
ComputeExpectedResult == 
    IF MAX_ELEMENTS > 0
    THEN FilterSets(FilterDeletes(FilterWritesWithMaxSize(RetainLargestWrites)))
    ELSE FilterSets(FilterDeletes(RetainLargestWrites))

\* ***********************************************************************
\* ***********************************************************************

\* Return the set of elements in databaseResults which have a timestamp
\* that falls below 'time'
Filter(time) == { x \in databaseResult : x[2] >= time }

\* Return the set which is either the union of 'element' and databaseResult or
\* databaseResult if the insertion should be screened.  An insertion is
\* screened if there exists a tombstone for that element that has a timestamp
\* higher then the timestamp associated to 'element'.
Insert(element) ==
    IF \E tombstone \in tombstones : tombstone[1] = element[1] /\ 
       tombstone[2] >= element[2]
    THEN databaseResult
    ELSE { element } \union databaseResult

\* Return the set which is the result of Insert(element) retaining the top N
\* elements where N is MAX_ELEMENTS and entries are ranked by first timestamp,
\* and then numeric value (aka Venice DCR)
InsertWithMaxSize(element) ==
    IF ~(Cardinality(databaseResult) = MAX_ELEMENTS)
    THEN Insert(element)
    ELSE FilterWritesWithMaxSize(Insert(element))

\* Return the set which is the result of removing 'element' from database
\* result
Remove(element) == 
    databaseResult \ { x \in databaseResult : x[1] = element[1] /\ 
    x[2] <= element[2] }

\* Take a random operations from the list of pending operations and apply it to
\* the database. Operations which fall below the row timestamp (aka, last set) 
\* are screened out; deletes are added to a list of tombstones which 
\* apply additional screening for incoming operations.

\* An operation is a sequence with the following format:
\* << value, timestamp, op >> with the exception of sets which only contain
\* <<<<>>, timestamp>>
ApplyOperation ==
    \E x \in pendingOperations :
        /\ pendingOperations' = pendingOperations \ { x }
        /\  IF x[2] > databaseRowTimestamp
            THEN IF Len(x) = 2
                 THEN 
                    /\ databaseRowTimestamp' = x[2]
                    /\ databaseResult' = Filter(x[2])
                    /\ UNCHANGED << expectedResult, tombstones >>
                 ELSE IF x[3] = "+"
                      THEN 
                        /\ IF MAX_ELEMENTS > 0 
                           THEN databaseResult' = InsertWithMaxSize(x)
                           ELSE databaseResult' = Insert(x) 
                        /\ UNCHANGED << expectedResult, tombstones, 
                                     databaseRowTimestamp >>
                       ELSE
                            /\ databaseResult' = Remove(x)
                            /\ tombstones' = tombstones \union { x }
                            /\ UNCHANGED << expectedResult, databaseRowTimestamp >>
            ELSE UNCHANGED <<databaseResult, expectedResult, tombstones,
                            databaseRowTimestamp >>

\* At some state, once all pending writes have been applied, the databaseResult
\* should match the expected result
Live == <>[](Cardinality(pendingOperations) = 0 => expectedResult = databaseResult)

\* If we have specified a max number of elements for the set, the size of the
\* database result AND the expected result should not exceed that max
Safe ==
    /\ MAX_ELEMENTS > 0 => Cardinality(databaseResult) <= MAX_ELEMENTS 
    /\ MAX_ELEMENTS > 0 => Cardinality(expectedResult) <= MAX_ELEMENTS

Init ==
    /\ databaseResult = {}
    /\ pendingOperations = (WRITES \union DELETES) \union SETS
    /\ expectedResult = ComputeExpectedResult
    /\ tombstones = {}
    /\ databaseRowTimestamp = 0

Spec == Init /\ [][ApplyOperation]_vars

=======

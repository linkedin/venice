----------- MODULE MCleapfrog ----
EXTENDS TLC, leapfrog

(* Do not explore states that have more writes then whats been configured  *)
TerminateComparison ==
    \/ WRITES <= MAX_WRITES

(* Do not explore states where we're just running comparisons infinitely   *)
NoSuccessiveControlMessages ==
    /\ \A i,j \in 1..Len(WALs.coloA):
        (j = i + 1 /\ WALs.coloA[i].key = "controlKey") => WALs.coloA[j].key # WALs.coloA[i].key
    /\ \A i,j \in 1..Len(WALs.coloB):
        (j = i + 1 /\ WALs.coloB[i].key = "controlKey") => WALs.coloB[j].key # WALs.coloB[i].key

(* INVARIANT meant to police state explosion (possible bug)                *)
MaxDiameter == TLCGet("level") < 50
====
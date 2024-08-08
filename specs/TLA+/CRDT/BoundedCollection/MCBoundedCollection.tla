----------------------- MODULE MCBoundedCollection -----------------
EXTENDS BoundedCollection

c_WRITES == {<<1, 2, "+">>, <<2, 3, "+">>, <<3, 4, "+">>}
c_MAX_ELEMENTS == 2
c_SETS == {<<<<>>, 1>>}
c_DELETES == {<<3, 5, "-">>}

====

Imagine a simple setup with 2 storage nodes and a replication factor of 1.  Suppose this is how 10 records got placed on the two storage nodes:

  N0: 1 4 8 9
  N1: 2 3 5 6 7 10

Now suppose all of these records are past their retention period, and N1 happens to have trimmed them but N0 has not (yet).  If a client starts reading the log from the start, what do we deliver?

There are two main choices:

(1) Deliver the records still available on N0.  The client sees record 1, trim gap 2-3, record 4, trim gap 5-7, records 8 and 9, trim gap 10 ...

(2) "Eagerly" deliver a large trim gap as soon as we encounter a gap caused by trimming.  In the above example we would deliver record 1 but then a larger trim gap 2-10.

By default the LogDevice client (ClientReadStream) goes with (2).  Clients can call doNotSkipPartiallyTrimmedSections() on Reader or AsyncReader to choose (1).

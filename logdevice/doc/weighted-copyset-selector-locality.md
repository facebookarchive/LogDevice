# Weight adjustment for locality in WeightedCopySetSelector

## Context

WeightedCopySetSelector picks copysets for records, both on append path and rebuilding path, as well as in a few other places like log recovery and CheckSealRequest.

Consider cross-rack replication with racks of different sizes, and suppose that cross-rack bandwidth is limited compared to in-rack bandwidth. (If minimizing cross-rack traffic doesn't seem useful, imagine cross-region replication. Here we're just using the word "rack" to mean whatever scope we're configured to replicate across.) We want appenders to send as few copies to other racks as possible, but still satisfy the cross-rack replication requirement, i.e. replicate each record to at least the configured number of racks; let's call this number K; usually K=2 or K=3 is used. Note that the replication factor (let's call it R) can be greater than K; e.g. a typical replication requirement is "store the record on 3 spanning at least 2 racks", which corresponds to R=3, K=2. Because of chain-sending, the actual replication factor doesn't affect the number of times a record is transmitted cross-rack during an append: if we need to store a record on multiple nodes in some rack, appender will send a copy to one of these nodes, and the node will forward it to the other nodes in the same rack. Depending on the copyset, an append will send the record cross-rack K or K-1 times: if the copyset contains at least one node in the appender's rack, the record will be sent cross rack K-1 times, otherwise - K times.

So, picking a copyset that contains at least one node from appender (sequencer) rack saves us one cross-rack hop. If we care about cross-rack bandwidth, we want the copyset selector to make sure that most of the copysets contain at least one node from sequencer rack. Usually this means picking the sequencer rack more often that we otherwise would. On the other hand, we don't want this consideration to affect the overall data distribution among nodes - each node should receive, on average, the number of records proportional to the node's weight, regardless of where sequencers are. So we need to introduce a preferential treatment of the sequencer rack but make sure that its effect cancels out when averaged over a large number of logs.

Preferential treatment of sequencer rack (let's call it s) is essentially equivalent to changing weights on a per-log basis: if we pick copysets so that each contains exactly one node from s, then s receives 1/R of all our records, which means that effectively the weight of rack s is 1/R of the total weight of the nodeset. Let's make this weight adjustment explicit: when creating a copyset selector (or even when generating nodeset), let's change the weights of nodes in such a way that the total weight of nodes in the sequencer rack s is as close as possible to 1/R of the total nodeset weight.

So, we need to adjust weights on a per-log (or rather per-s) basis in such a way that, when averaged across many logs (with different s), the overall weight of each node stays the same as before the adjustment. That's the problem that this document is addressing.

## Problem statement

Let's introduce some notation:
 * s - sequencer rack (for the current log),
 * W[r] - weight of rack r in config, normalized so that sum{r} W[r] = 1,
 * W[s][r] - adjusted weight of rack r, used by copyset selectors for logs whose sequencers are in rack s,
 * S[s] - sequencer weight of rack s, normalized so that sum{s} S[s] = 1.

We need to find W[s][r] (for all racks s and r) always satisfying:
 (1) W[s][r] >= 0 for each s, r,
 (2) sum{s} W[s][r] * S[s] = W[r] for each r,  (i.e. total weights are unchanged)
and, when possible, satisfying:
 (3) W[s][r] <= (R - K + 1)/R  (otherwise any K-rack replication would violate weights),
 (4) W[s][r] >= 1/R  (i.e. we can always pick at least one node from s without violating weights),
 (5) W[s][r] is not much bigger than 1/R  (i.e. we don't often have to pick more than one node in sequencer rack).

(1)-(2) are our constraints, and (3)-(5) are what we want to kind of optimize.

Notice that there are quite a lot more degrees of freedom than constraints, so the solution, if it even exists, wouldn't be unique. We want to construct a solution that does a decent job at achieving (3)-(5) but has a simple form. Of course we could just plug this into a general purpose optimization method and get some solution; but that seems like an overkill, and it would need to be done every time a copyset selector is instantiated, or done beforehand and stored somewhere; both of these options sound inconvenient. So let's find some closed-form solution.

## Derivation

(Feel free to skip to the next section if you're not interested.)

So, given a sequencer rack s and its weight W[s] we need to add something to its weight and subtract something from weights of other racks, so that the total amount added is equal to the total amount subtracted. It's clear what we want to add to W[s]: 1/R - W[s], so that the adjusted weight is equal to 1/R as (4) and (5) say. Now we need to subtract the same total amount of weight from other racks; the only thing to decide is how to distribute this weight across the racks. The natural choice is to make this distribution independent of s. These considerations immediately give us equations:

W[s][s] = 1/R
W[s][r] = W[r] - E[r]/(1 - E[r])*D[s], r != s
D[s] = 1/R - W[s]
sum{r} E[r] = 1

where E[r] are unknowns. This seems hard to solve, so let's solve this similar system instead:

W[s][s] = 1/R
W[s][r] = W[r] + [r == s]*D[r] - E[r]*D[s]  (even if r == s)
sum{r} E[r] = 1

where [r == s] is 1 if r == s and 0 otherwise; D[r] and E[r] are unknowns. It's easy to get rid of E[r] using (2):

E[r] = S[r]*D[r]/sum{i} S[i]*D[i]

then

W[s][r] = W[r] + D[s]*([r == s] - S[r]*D[r]/sum{i} S[i]*D[i])

Now we're free to choose D[s]. Ideally we would pick them so that W[s][s] = 1/R, in other words:

1/R - W[s] = D[s]*(1 - S[s]*D[s]/sum{i} S[i]*D[i]),

but that seems hard to solve exactly. But notice that (1 - S[s]*D[s]/sum{i} S[i]*D[i]) is usually close to 1 and usually doesn't depend on s very much. Let's approximate it with a constant 1/C. Then:

D[s] = C*(1/R - W[s]),

but for convenience let's move C out of D[s] and get...

## Answer

D[s] = 1/R - W[s]
W[s][r] = W[r] + C*D[s]*([r == s] - S[r]*D[r]/sum{i} S[i]*D[i])

and we're free to choose C. This always satisfies (2). So we're down to one degree of freedom: C. Let's pick it so that (1) is always satisfied, and (3)-(5) are satisfied kinda as much as possible. Note that C = 0 corresponds to W[s][r] = W[r], i.e. no weight adjustment for locality, which satisfies (1); so (1) is always satisfiable with a sufficiently small C.

We're actually still free to choose D[s] in addition to C. 1/R - W[s] is just one simple way to pick it. Maybe in some cases it would make sense to increase or decrease it. E.g. if a rack has a big sequencer weight and a small but nonzero storage weight, we probably want to use a smaller D[s] for it; otherwise we would need a pretty small C to satisfy (1).

Some common concepts and conventions used by ldbench workers.

## Approximate probability distributions and '*-distribution' options

Approximate probability distribution is usually represented as two options: average and variation around average. The options describing the variation have suffix -distribution, e.g. --fanout-distribution.

Suppose you need to write to logs at an average rate of 1 MB/s, but different logs should have different write rate, according to some distribution. E.g. 30% of logs don't get any writes, 20% get ~1 KB/s, 40% get ~100 KB/s, 10% get ~10 MB/s. LDBench convention is to represent this sort of information as two command line options: a number and a histogram. The number is the average append throughput. The histogram defines the variation around the average. The histogram is logarithmic: bucket i tells the probability that the value is in range [2^i, 2^(i+1)), i>=0. (The distribution inside the range is uniform.) The histogram is represented as a comma-separated list of numbers, not necessarily adding up to 1.

Examples:

--fanout=3 --fanout-distribution=5,4,1 means that each log will have 3 readers on average; 50% of logs will have [X, 2*X] readers, 40% of logs will have [2*X, 4*X] readers, 10% of logs will have [4*X, 8*X] readers, where X is chosen so that the average number of readers is 3.

--fanout=3 --fanout-distribution=constant means that each log has 3 readers, period.

--fanout=3 --fanout-distribution=1,2,4,8,16,32 means, approximately, that each log will get a (uniformly) random number of readers between 0 and 6.

The -distribution options usually default to "constant", so if you don't need to simulate variance, you don't need to use the -distribution options.


## Random event sequences and '*-spikiness' options

Suppose you need to produce some writes to some logs. For each log you have number of writes per second that you want to send to this log, on average. But the writes to each log should be somewhat bursty. Maybe the bursts should be synchronized across all logs, or maybe each log should get bursts at its own random times. LDBench convention is to represent this sort of information as two command line options: the average rate of events (e.g. writes) and the "spikiness" description.

The spikiness description looks like "x%/y%/t" or "x%/y%/t/aligned", where x and y are between 0 and 100, t is a duration, e.g. "1min". This is best explained through an example:

--reader-restart-spikiness="30%/2%/14min/aligned" means that 30% of all reader restarts happen during spikes, a spike happens every 14 minutes and lasts for 2% of the 14 minutes (i.e. for ~17 seconds). "aligned" means that the spikes happen everywhere (all workers, logs, readers etc) at the same time. "30%/2%/14min" would mean that each reader gets its own sequence of spikes, randomly shifted relative to other readers, so the overall rate of restarts will be relatively smooth.

Degenerate values like "100%/0%/15min" are allowed. It means that all the events will happen in periodic bursts. The worker will process the events of each burst as fast as it can.

The -spikiness options usually default to "0%/0%/1s", meaning no spikes.

The times of events (e.g. appends or reader restarts) are generated using Poisson point process. I.e. time between events is generated as -log(x)/events_per_sec, where x is drawn from a uniform distribution in (0, 1].

Poisson process has some useful properties:
  * Union of poisson processes is a poisson process. I.e. generating and
    merging n sequences of events is equivalent to generating one sequence
    of events with events_per_sec n times as big. This helps make sure
    that overall workload doesn't depend on number of workers.
  * Memoryless. Times of future events don't depend on times of past
    events. This helps make sure that workload from many short-living
    workers is the same as from few long-living ones, even when average
    time between events is longer than worker life time.
  * A good model of many real event distributions. Union of many
    arbitrarily distributed processes converges to a poisson process
    (conditions apply). E.g. if each of a billion people occasionally
    likes something, the overall stream of likes will be very close to a
    Poisson process.

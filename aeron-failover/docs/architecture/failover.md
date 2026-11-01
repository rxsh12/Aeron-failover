# Failure detection and recovery

## Two independent mechanisms

**Aeron Cluster owns leadership.** The three members exchange consensus/heartbeat traffic and elect a leader. The proxy client observes cluster events and resumes its pending outbox work against the newly elected leader. A three-voter majority requires two members; one failed voter is tolerated, two failed voters remove quorum.

**The monitor is observational.** It reads the current leader ID from the proxy status endpoint, then sends health PINGs to that leader's member-specific TCP endpoint. Its missed-PING threshold does not cause or accelerate a cluster election.

The member heartbeat, election, and consensus timeouts are left at the selected Aeron release defaults; they are not the monitor's `monitor.ping-interval-ms` or `monitor.misses-before-down`.

## Timing

The monitor defaults to a one-second ping interval and reports a down member after three consecutive misses. Depending on when failure occurs relative to the ping schedule, that report generally takes about two to three seconds, plus connection/read timeout delays.

This is **not the failover duration**. Promotion time is the interval from the failure to Aeron reporting the new leader. It depends on Aeron Cluster's configured/default timeout behavior, process scheduling, network latency, and failure mode. Do not infer leader promotion time from the monitor's threshold.

## Measuring an election

1. Start the proxy and wait until it reports an elected leader.
2. Record a monotonic timestamp when the leader is stopped or isolated.
3. Record a monotonic timestamp when the proxy logs the next-leader callback or the new member logs `IS LEADER`.
4. Subtract the timestamps. Repeat several times with the same failure mode and report median and tail values.
5. Record whether failure was a process kill, graceful stop, host power loss, or network partition. These are not equivalent.

For code-level instrumentation, timestamp `onNewLeader` in the proxy and `onRoleChange(LEADER)` in the service using `System.nanoTime()`, and emit elapsed time from a recorded failure marker in a controlled test harness. A monitor PING can help detect reachability but should not be used as the election start signal.

## What recovery protects

An outbox record already synchronized to proxy storage is retained and retried after leader/client-session changes. A message in an unacknowledged UDP path has no producer retry. Packets buffered in the proxy's in-memory queue have not yet reached the durable outbox and can be lost if the proxy exits at that moment.

# Target obsess experiment

Obsess mode temporarily collects more pings when a target starts losing packets
or its latency rises. It is opt-in for each target and configured in YAML:

```yaml
targets:
  - id: gateway
    address: 1.1.1.1
    ping:
      interval: 5s
      timeout: 1s
    obsess:
      interval: 100ms
      latency_threshold: 50%
      baseline_window: 1m
      min_samples: 3
      recover_after: 1m
```

`obsess: {}` enables those defaults. Omit the object or use `enabled: false`
inside it to disable the feature. Restart Flameping after editing configuration.
The default fast interval uses the target's effective `ping.min_interval`,
inherited from global settings unless overridden on that target; it must be
shorter than the target's normal interval. Per-target `ping.interval`,
`ping.timeout`, and `ping.min_interval` override independently. The ping timeout
stays the same in both modes. See the [configuration example](../README.md#per-target-ping-settings).

## Brainstorming and decisions

Two latency rules are useful for different targets:

| Setting | Trigger | When it helps |
| --- | --- | --- |
| `latency_threshold: 100ms` | An on-time RTT strictly above 100 ms | A fixed latency budget; works immediately after startup |
| `latency_threshold: 50%` | An on-time RTT strictly above 1.5 times the preceding average | Targets with different typical RTTs |

A duration is an absolute RTT limit, not an amount added to the baseline. Relative
thresholds can react to very small changes on low-latency targets; use an absolute
threshold when that sensitivity is undesirable. This experiment chooses one rule
per target to keep configuration and recovery behavior easy to explain. Combined
rules, percentile estimators, and automatic baseline adaptation during an incident
are possible later extensions.

The relative baseline is the arithmetic mean of previous on-time probe RTTs
settled in send order whose send times fall in `baseline_window`. Completed
replies behind an unresolved older probe wait for that probe to settle. A probe captures its
pre-event baseline when its send begins. It cannot contribute to its own trigger
threshold. Relative detection waits for `min_samples` usable observations; loss
detection and absolute latency detection work immediately. Configuration rejects
a relative minimum sample count that cannot fit the normal cadence and window.

On entering obsess mode, Flameping freezes that baseline and its threshold for
the whole incident. For a 10 ms average and a 50% threshold, RTTs above 15 ms
trigger obsessing, and recovery requires RTTs at or below 15 ms. Continued slow
pings cannot teach the controller to accept the degraded latency as normal.

A successfully sent probe that reaches its timeout without a valid reply triggers
loss. A late reply is also a deadline loss, including when it arrives before the
timeout worker runs. It is still retained in history. Duplicate, foreign, and
wrong-source replies cannot count as extra healthy observations.

Recovery requires `recover_after` of consecutive healthy probe evidence. Loss,
latency above the saved threshold, local send errors, and scheduler gaps restart
the healthy run. A gap between actual successful probe sends longer than twice
the fast interval also restarts it, so stalled collection cannot count silence as
health. Results advance in send order: an unresolved older probe prevents later
successes from prematurely proving recovery. Newer probes may remain in flight.

If loss triggers before a relative baseline is available, latency judgment is
unavailable for that incident. A full healthy run of on-time replies permits
recovery. In either case, the final healthy observations seed the next baseline;
degraded incident measurements never move the frozen recovery threshold. A
persistent degradation keeps fast probing active. There is no forced maximum
duration that would silently restore normal probing during an ongoing problem.

## Implementation plan

1. Add strict per-target configuration, defaults, and validation. Budget the
   aggregate probe rate assuming every enabled target is obsessing simultaneously.
2. Add a live controller with one deadline timer, authenticated probe correlation,
   ordered recovery evidence, and bounded baseline and pending-probe storage.
3. Add immediate echo/scheduler observations alongside the existing lossless
   storage pipeline. Preserve local-error, late-reply, and network-loss semantics.
4. Allow scheduler interval changes to wake it promptly. Flush gaps at their old
   cadence and avoid catch-up bursts when changing rates.
5. Expose live status in the target API/dashboard and preserve subsecond raw
   observations when zooming into history.
6. Verify transitions, overlap/race cases, DNS resets, resource limits, dashboard
   refresh behavior, and existing measurement workflows. Use independent
   ultra-effort reviews of both the design and implementation.

## Runtime and history

The dashboard shows when obsess mode is learning a baseline, ready, actively
probing faster, or paused. An active target shows its interval, trigger cause,
saved threshold, and confirmed healthy progress. Status refreshes with the normal
five-second dashboard poll. `/api/v1/targets` adds an `obsess` object for enabled
targets; `interval_ms` on the target remains the configured normal cadence, while
`obsess.interval_ms` is the live requested cadence.

Transition logs identify the target using its configured string `target_id`,
display name (`target_name`), configured hostname/address (`target_address`), and
the actual resolved destination (`target_ip`). Entering logs distinguish a
`timeout`, a `late_reply`, and a `latency_spike`, with the probe sequence, send
time, deadline, and timeout. Received replies also include their RTT. Latency
entries show the frozen threshold and available pre-event average, sample count,
configured rule, and percentage rise when available. Relative rules without enough baseline
samples explain that condition; unknown RTTs and thresholds are omitted.

For example, a timeout entry includes these fields:

```json
{
  "msg": "target started obsessing",
  "target_id": "wan-check",
  "target_name": "WAN uplink",
  "target_address": "edge.example.net",
  "target_ip": "192.0.2.1",
  "reason": "loss",
  "trigger": "timeout",
  "timeout_ms": 1000,
  "detail": "No ping reply was observed before the 1000 ms deadline."
}
```

`event_at` records the triggering evidence time, which can precede log emission.
Probe details and the destination IP are captured together, so a DNS update
cannot relabel an earlier trigger. Exit logs distinguish `recovered` from
`endpoint_changed`; repeated bad probes within an incident do not produce new
transition entries.

State is in memory. Restarting or changing a target's resolved address clears
the incident and baseline and restores normal cadence. Queued work is refreshed
to the current same-family address; queued work for an obsolete IP family is
recorded as a local scheduler gap. Old replies stay in history but cannot affect
the replacement endpoint's state.

All probes use the existing raw samples and rollups. Deep zoom can show buckets
as small as 100 ms, including after a restart or disabling obsess mode. Ping
series expose the precise `bucket_ms` width alongside the existing whole-second
`resolution_s` field. Graphs and loss percentages remain weighted by probe
counts, so obsess periods contribute more observations than normal periods.

Worst-case rates must fit `ping.max_probes_per_second`. A combined 100,000-slot
budget covers pending outcomes, baseline samples, and queued-send bursts; extreme
timeouts or baseline windows can therefore be rejected at configuration time.
The runtime guard reports when live observations have been limited. That notice
remains until a full healthy recovery or an endpoint reset; it does not mean
the limit is still being reached. Unknown outcomes interrupt recovery without
being reported as network loss.
Storage backpressure and disk-pressure shutdown continue to apply to fast probes.

## Review and validation

Independent ultra-effort agents brainstormed semantics, planned the architecture,
and reviewed the plan. Corrections included immediate gap notifications instead
of delayed persisted gaps, source validation, ordered recovery with probes still
in flight, silence detection, bounded tracking, and DNS changes across IP
families. Implementation review additionally caught buffered replies that needed
rechecking against a newly frozen threshold and fractional bucket widths that
could exceed a query's point cap. The final review had no blocking findings.

Validation passed: the full Go suite, the full race suite, the static binary
build, TypeScript checking, frontend build, and browser smoke tests. The browser
checks include live trigger/recovery refreshes, target switching, and desktop
and mobile layouts under the production content security policy. A temporary
loopback instance using a deliberately tiny absolute threshold switched from
1-second probing to obsess mode; its stored send spacing had a 100.26 ms median,
and it shut down cleanly. Fake-clock tests separately verify loss, recovery,
overlapping replies, frozen thresholds, and missing-evidence behavior.

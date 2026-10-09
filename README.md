# Signal — Roblox event utilities

A small collection of Luau `ModuleScript` utilities: **Signal**, **EventBus**, and **Promise**.
The public method names in the existing modules are preserved in this corrective release.

## What was fixed

- `EventBus.lua` is no longer an accidental copy of `Signal.lua` with a self-require; it now implements independent event subscriptions.
- `Signal:Wait()` suspends the calling coroutine until the next event, and `Destroy()` wakes pending waiters with `nil`.
- `Signal:Replay(n, callback)` replays the most recent `n` values, oldest to newest.
- `Signal:Profile()` no longer republishes its own event recursively. It measures **dispatch scheduling time**, not handler wall time.
- `Signal:DisconnectAll()` leaves the Signal usable for new subscriptions.
- Derived signals release their parent subscriptions when destroyed; debounce cancels pending timers.
- Promise continuation passthrough is fixed for pending resolutions and rejections; `Promise:await()` now uses a real coroutine resume.
- EventBus/Signal contain `--!strict` directives and typed method signatures. Promise's legacy API remains not fully statically typed; do **not** treat this as a complete type-check pass.

## Install

Place all three ModuleScripts together in the same parent:

```text
ReplicatedStorage
└── SignalPackage
    ├── Signal
    ├── EventBus
    └── Promise
```

```luau
local Signal = require(game.ReplicatedStorage.SignalPackage.Signal)
local changed = Signal.new()

local connection = changed:Connect(function(value)
    print("changed:", value)
end, 10) -- optional higher priority schedules first

changed:Fire("ready")
connection:Disconnect()

task.spawn(function()
    print("next value:", changed:Wait())
end)
changed:Fire("again")
changed:Destroy()
```

**Scheduling:** Callback invocations use `task.spawn` so a yielding subscriber cannot hold up a publisher. Higher priorities are enqueued first, but asynchronous task completion order is not guaranteed. `Fire` returns immediately after enqueueing. `FireAsync` returns a Promise which settles after dispatched subscribers complete. Listener exceptions are warned and do not prevent other subscribers from running.

## API overview

| Utility | Methods |
| --- | --- |
| Signal | `new`, `Connect`, `Once`, `Fire`, `FireAsync`, `Wait`, `Replay`, `DisconnectAll`, `Destroy` |
| Derived Signal | `Map`, `Filter`, `Pipe`, `Throttle`, `Debounce`, `Trace`, `Profile` |
| EventBus | `new`, `Subscribe(event, callback, priority?)`, `SubscribeOnce`, `Publish`, `PublishAsync`, `Clear`, `Destroy` |
| Promise | `new`, `andThen`, `catch`, `finally`, `await`, `all`, `race`, `delay`, `retry`, `timeOut` and existing helpers |

`Signal` records a **bounded** history of the last 128 non-nil values for `Replay`; this avoids unbounded event-history memory growth. If previous projects depended on more than 128 historical events, migrate that application-specific history to dedicated storage. The utilities exchange dynamic callback payloads, so consumer code should annotate payload shapes where needed.

## Validation

Use `tests/SignalRegression.server.lua` as a Roblox Studio Script with all three ModuleScripts as siblings. This exercises EventBus loading, Replay, Wait, Destroy, and Promise continuation/await cases.

Do not assume native compilation makes event dispatch faster: user callbacks and Roblox engine scheduling typically dominate. Benchmark with the Studio Script Profiler using the same workload before and after. Luau and Studio type-analysis tools should be run on the full project before merging.

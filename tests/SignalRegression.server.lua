--!strict
-- Run inside Roblox Studio as a Script alongside Signal, EventBus and Promise.
local Signal = require(script.Parent:WaitForChild("Signal"))
local EventBus = require(script.Parent:WaitForChild("EventBus"))
local Promise = require(script.Parent:WaitForChild("Promise"))

local bus = EventBus.new()
local count = 0
local one = bus:SubscribeOnce("tick", function() count += 1 end)
assert(one.Connected)
bus:Publish("tick", 1)
bus:Publish("tick", 2)
task.wait(0.03)
assert(count == 1, "Once must be invoked exactly once")
assert(not one.Connected, "Once connection remains active")
bus:Destroy()

local signal = Signal.new()
signal:Fire(11)
signal:Fire(22)
local history = {}
signal:Replay(2, function(v) table.insert(history, v) end)
assert(history[1] == 11 and history[2] == 22, "Replay order is incorrect")

local awaited = false
task.spawn(function()
    assert(signal:Wait() == 33, "Wait returned incorrect value")
    awaited = true
end)
task.wait()
signal:Fire(33)
task.wait(0.03)
assert(awaited, "Signal:Wait did not resume")

local mapped = signal:Map(function(v) return v * 2 end)
local received = 0
mapped:Once(function(v) received = v end)
signal:Fire(7)
task.wait(0.03)
assert(received == 14, "Map did not forward events")
mapped:Destroy()

-- A cancelled Wait coroutine must not abort future Signal:Fire dispatch.
local cancelledSignal = Signal.new()
local cancelledWaiter = task.spawn(function() cancelledSignal:Wait() end)
task.wait()
task.cancel(cancelledWaiter)
local stillDispatched = false
cancelledSignal:Connect(function() stillDispatched = true end)
cancelledSignal:Fire(1)
task.wait(0.03)
assert(stillDispatched, "Cancelled Wait coroutine interrupted event dispatch")
cancelledSignal:Destroy()

-- Debounce must emit only the latest event and not retain completed timers.
local source = Signal.new()
local debounced = source:Debounce(0.02)
local debouncedValues = {}
debounced:Connect(function(value) table.insert(debouncedValues, value) end)
source:Fire("old")
source:Fire("new")
task.wait(0.07)
assert(#debouncedValues == 1 and debouncedValues[1] == "new", "Debounce emitted stale or duplicate values")
assert(next(debounced._timers) == nil, "Completed debounce timer was retained")
debounced:Destroy()
source:Destroy()

local passedThrough = false
Promise.new(function(resolve)
    task.defer(resolve, 41)
end):andThen(nil):andThen(function(v)
    assert(v == 41, "Pending Promise passthrough failed")
    passedThrough = true
end)
task.wait(0.03)
assert(passedThrough)

local done = false
task.spawn(function()
    local value = Promise.delay(0.01):andThen(function() return 99 end):await()
    assert(value == 99, "Promise:await failed")
    done = true
end)
task.wait(0.08)
assert(done, "await never resumed")
signal:Destroy()
print("Signal / EventBus / Promise regression PASS")

--!strict
-- Server Script regression; put beside the Signal/EventBus/Promise ModuleScripts.
local Signal = require(script.Parent:WaitForChild("Signal"))
local Promise = require(script.Parent:WaitForChild("Promise"))

local passed = 0
local signal = Signal.new()
local fired = 0
local connection = signal:Connect(function(value)
    fired += value
end)
signal:Fire(2)
signal:Fire(3)
assert(fired == 5, "Fire delivery failed")
passed += 1

local history = {}
signal:Replay(1, function(value)
    history[#history + 1] = value
end)
assert(#history == 1 and history[1] == 3, "Replay returned wrong history")
passed += 1

local once = 0
signal:Once(function(value)
    once += value
end)
signal:Fire(4)
signal:Fire(4)
assert(once == 4, "Once callback called more than once")
passed += 1

local waited = false
task.spawn(function()
    waited = signal:Wait() == 7
end)
task.wait()
signal:Fire(7)
task.wait()
assert(waited, "Wait did not resume")
passed += 1

signal:DisconnectAll()
signal:Fire(8)
assert(fired == 2 + 3 + 4 + 4 + 7, "DisconnectAll retained listeners")
signal:Connect(function(value)
    fired += value
end)
signal:Fire(1)
assert(fired == 21, "Cannot reconnect after DisconnectAll")
passed += 1
connection:Disconnect()

local mapped = signal:Map(function(value) return value * 2 end)
local mappedValue = 0
mapped:Connect(function(value) mappedValue = value end)
signal:Fire(6)
assert(mappedValue == 12, "Map failed")
mapped:Destroy()
passed += 1

local chained = Promise.resolve(10):andThen(function(value) return value + 4 end)
assert(chained:await() == 14, "Promise chaining failed")
passed += 1
local forwarded = Promise.resolve(20):andThen(nil)
assert(forwarded:await() == 20, "Promise missing-handler propagation failed")
passed += 1
local recovered = Promise.reject("bad"):catch(function(reason) return reason .. " fixed" end)
assert(recovered:await() == "bad fixed", "Promise catch failed")
passed += 1

signal:Profile()
signal:Fire(1) -- Prior implementation recursively fired inside Profile listener.
passed += 1
signal:Destroy()
print(("Signal + Promise regression PASS: %d cases"):format(passed))

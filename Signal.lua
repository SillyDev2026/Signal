--!strict
--!optimize 2

local EventBus = require(script.Parent:WaitForChild("EventBus"))

local Signal = {}
Signal.__index = Signal
local MAX_HISTORY = 128

type Callback = (any) -> ()
type Connection = EventBus.Connection

function Signal.new()
    return setmetatable({
        _bus = EventBus.new(),
        _eventName = "SignalEvent",
        _history = {} :: {any},
        _waiters = {} :: {thread},
        _upstreams = {} :: {Connection},
        _timers = {} :: {thread},
        _destroyed = false,
        _profile = false,
    }, Signal)
end

function Signal:Connect(callback: Callback, priority: number?): Connection
    assert(not self._destroyed, "Cannot connect to a destroyed signal")
    return self._bus:Subscribe(self._eventName, callback, priority)
end

function Signal:Once(callback: Callback, priority: number?): Connection
    assert(not self._destroyed, "Cannot connect to a destroyed signal")
    return self._bus:SubscribeOnce(self._eventName, callback, priority)
end

function Signal:_remember(data: any)
    local history = self._history
    history[#history + 1] = data
    if #history > MAX_HISTORY then table.remove(history, 1) end
end

function Signal:_wake(data: any)
    local waiters = self._waiters
    self._waiters = {}
    for _, waiting in ipairs(waiters) do task.spawn(waiting, data) end
end

function Signal:Fire(data: any)
    if self._destroyed then return end
    self:_remember(data)
    self:_wake(data)
    local started = if self._profile then os.clock() else 0
    self._bus:Publish(self._eventName, data)
    if self._profile then print("[Signal] Dispatch scheduling seconds:", os.clock() - started) end
end

function Signal:FireAsync(data: any)
    if self._destroyed then return nil end
    self:_remember(data)
    self:_wake(data)
    return self._bus:PublishAsync(self._eventName, data)
end

function Signal:Wait(): any
    assert(not self._destroyed, "Cannot wait on a destroyed signal")
    local waiting = coroutine.running()
    table.insert(self._waiters, waiting)
    return coroutine.yield()
end

function Signal:DisconnectAll()
    self._bus:Clear(self._eventName)
end

function Signal:Destroy()
    if self._destroyed then return end
    self._destroyed = true
    self._bus:Destroy()
    for _, connection in ipairs(self._upstreams) do connection:Disconnect() end
    for _, timer in ipairs(self._timers) do pcall(task.cancel, timer) end
    self:_wake(nil)
    table.clear(self._history)
    table.clear(self._upstreams)
    table.clear(self._timers)
end

function Signal:Replay(n: number, callback: Callback)
    assert(type(n) == "number" and n >= 0 and n < math.huge, "n must be finite and nonnegative")
    local count = math.min(#self._history, math.floor(n))
    for index = #self._history - count + 1, #self._history do callback(self._history[index]) end
end

function Signal:Map(mapper: (any) -> any)
    local mapped = Signal.new()
    local connection = self:Connect(function(data) mapped:Fire(mapper(data)) end)
    table.insert(mapped._upstreams, connection)
    return mapped
end

function Signal:Filter(predicate: (any) -> boolean)
    local filtered = Signal.new()
    local connection = self:Connect(function(data)
        if predicate(data) then filtered:Fire(data) end
    end)
    table.insert(filtered._upstreams, connection)
    return filtered
end

function Signal:Pipe(targetSignal)
    return self:Connect(function(data) targetSignal:Fire(data) end)
end

function Signal:Throttle(seconds: number)
    assert(seconds >= 0 and seconds < math.huge, "seconds must be finite and nonnegative")
    local throttled = Signal.new()
    local last = -math.huge
    local connection = self:Connect(function(data)
        local now = os.clock()
        if now - last >= seconds then
            last = now
            throttled:Fire(data)
        end
    end)
    table.insert(throttled._upstreams, connection)
    return throttled
end

function Signal:Debounce(seconds: number)
    assert(seconds >= 0 and seconds < math.huge, "seconds must be finite and nonnegative")
    local debounced = Signal.new()
    local pending: thread? = nil
    local connection = self:Connect(function(data)
        if pending then pcall(task.cancel, pending) end
        pending = task.delay(seconds, function()
            pending = nil
            if not debounced._destroyed then debounced:Fire(data) end
        end)
        table.insert(debounced._timers, pending)
    end)
    table.insert(debounced._upstreams, connection)
    return debounced
end

function Signal:Trace(tag: string)
    return self:Connect(function(data) print("[Signal Trace]", tag, data) end)
end

function Signal:Profile()
    self._profile = true
    return self
end

return Signal

--!native
--!optimize 2

local Signal = {}
Signal.__index = Signal
local EventBus = require(script.Parent.EventBus)
local MAX_HISTORY = 256

function Signal.new()
	local self = {}
	self._bus = EventBus.new()
	self._eventName = 'SignalEvent'
	self._history = {}
	self._waiters = {}
	self._destroyed = false
	self._profileEnabled = false
	return setmetatable(self, Signal)
end

function Signal:Connect(callback, priority)
	return self._bus:Subscribe(self._eventName, callback, priority)
end

function Signal:Once(callback, priority)
	return self._bus:SubscribeOnce(self._eventName, callback, priority)
end

local function recordAndWake(self, data)
    if data ~= nil then
        local history = self._history
        history[#history + 1] = data
        if #history > MAX_HISTORY then table.remove(history, 1) end
    end
    local waiters = self._waiters
    self._waiters = {}
    for _, thread in ipairs(waiters) do task.spawn(thread, data) end
end

function Signal:Fire(data)
    if self._destroyed then return end
    recordAndWake(self, data)
    local startTime = if self._profileEnabled then os.clock() else 0
    self._bus:Publish(self._eventName, data)
    if self._profileEnabled then print("Signal dispatch seconds:", os.clock() - startTime) end
end

function Signal:FireAsync(data)
    if self._destroyed then return nil end
    recordAndWake(self, data)
    local startTime = if self._profileEnabled then os.clock() else 0
    local promise = self._bus:PublishAsync(self._eventName, data)
    if self._profileEnabled then
        promise:finally(function()
            print("Signal async dispatch seconds:", os.clock() - startTime)
        end)
    end
    return promise
end

function Signal:Wait()
    assert(not self._destroyed, "SIGNAL_DESTROYED")
    local thread = coroutine.running()
    table.insert(self._waiters, thread)
    return coroutine.yield()
end

function Signal:DisconnectAll()
    self._bus:Clear(self._eventName)
end

function Signal:Destroy()
    if self._destroyed then return end
    self._destroyed = true
    if self._parentConnection then self._parentConnection:Disconnect() end
    self._bus:Destroy()
    self._history = {}
    local waiters = self._waiters
    self._waiters = {}
    for _, thread in ipairs(waiters) do task.spawn(thread, nil) end
end

function Signal:Replay(n, callback)
    assert(type(callback) == "function", "Callback required")
    assert(type(n) == "number" and n >= 0 and n % 1 == 0, "Invalid replay count")
    local history = self._history
    local count = math.min(#history, n)
    for index = #history - count + 1, #history do
        callback(history[index])
    end
end

function Signal:Map(mapper)
	local mapped = Signal.new()
	mapped._parentConnection = self:Connect(function(data)
		mapped:Fire(mapper(data))
	end)
	return mapped
end

function Signal:Filter(predicate)
	local filtered = Signal.new()
	filtered._parentConnection = self:Connect(function(data)
		if predicate(data) then
			filtered:Fire(data)
		end
	end)
	return filtered
end

function Signal:Pipe(targetSignal)
	self:Connect(function(data)
		targetSignal:Fire(data)
	end)
end

function Signal:Throttle(seconds)
	local throttled = Signal.new()
	local last = 0
	self:Connect(function(data)
		local now = os.clock()
		if now - last >= seconds then
			last = now
			throttled:Fire(data)
		end
	end)
	return throttled
end

function Signal:Debounce(seconds)
	local debounced = Signal.new()
	local timer = nil
	self:Connect(function(data)
		if timer then task.cancel(timer) end
		timer = task.delay(seconds, function()
			debounced:Fire(data)
		end)
	end)
	return debounced
end

function Signal:Trace(tag)
	self:Connect(function(data)
		print(`[Trace]: [{tag}]: {data}`)
	end)
end

function Signal:Profile()
    -- Instrument real dispatch, never republish inside a listener.
    self._profileEnabled = true
end

return Signal

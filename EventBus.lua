--!native
--!optimize 2

local Signal = {}
Signal.__index = Signal
local EventBus = require(script.Parent.EventBus)

function Signal.new()
	local self = {}
	self._bus = EventBus.new()
	self._eventName = 'SignalEvent'
	self._history = {}
	self._waiters = {}
	self._destroyed = false
	return setmetatable(self, Signal)
end

function Signal:Connect(callback, priority)
	return self._bus:Subscribe(self._eventName, callback, priority)
end

function Signal:Once(callback, priority)
	return self._bus:SubscribeOnce(self._eventName, callback, priority)
end

function Signal:Fire(data)
	if self._destroyed then return end
	table.insert(self._history, data)
	self._bus:Publish(self._eventName, data)
	for _, waiter in ipairs(self._waiters) do
		task.spawn(waiter, data)
	end
	table.clear(self._waiters)
end

function Signal:FireAsync(data)
	if self._destroyed then return end
	table.insert(self._history, data)
	return self._bus:PublishAsync(self._eventName, data)
end

function Signal:Wait()
	return coroutine.yield(function(resolve)
		table.insert(self._waiters, resolve)
	end)
end

function Signal:DisconnectAll()
	self._bus:Destroy()
end

function Signal:Destroy()
	self._destroyed = true
	self._bus:Destroy()
	self._history = {}
	self._waiters = {}
end

function Signal:Replay(n, callback)
	local count = math.min(#self._history, n)
	for i = 1, #self._history - count + 1, #self.history do
		callback(self._history[i])
	end
end

function Signal:Map(mapper)
	local mapped = Signal.new()
	self:Connect(function(data)
		mapped:Fire(mapper(data))
	end)
	return mapped
end

function Signal:Filter(predicate)
	local filtered = Signal.new()
	self:Connect(function(data)
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
	self:Connect(function(data)
		local start = os.clock()
		self._bus:PublishAsync(self._eventName, data)
			:andThen(function()
				print("Profile:", os.clock() - start)
			end)
	end)
end

return Signal

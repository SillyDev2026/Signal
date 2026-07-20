--!native
--!optimize 2

local HttpService = game:GetService("HttpService")

local Promise = {}
Promise.__index = Promise

local PENDING = "PENDING"
local FULFILLED = "FULFILLED"
local REJECTED = "REJECTED"

local microtaskQueue = {}
local microtaskRunning = false

function flushMicrotasks()
	for _, promise in ipairs(microtaskQueue) do
		promise:_flushCallbacks()
	end
	table.clear(microtaskQueue)
	microtaskRunning = false
end

function scheduleMicrotask(promise)
	table.insert(microtaskQueue, promise)
	if not microtaskRunning then
		microtaskRunning = true
		task.defer(flushMicrotasks)
	end
end

local CancellationToken = {}
CancellationToken.__index = CancellationToken

function CancellationToken.new()
	return setmetatable({ cancelled = false }, CancellationToken)
end

function CancellationToken:Cancel()
	self.cancelled = true
end

Promise.CancellationToken = CancellationToken

function Promise.new(executor, token)
	assert(type(executor) == "function", "Executor must be a function")

	local self = setmetatable({
		_state = PENDING,
		_value = nil,
		_callbacks = {},
		_token = token,
	}, Promise)

	local function resolve(value)
		if self._state ~= PENDING then return end
		if token and token.cancelled then return end

		self._state = FULFILLED
		self._value = value
		scheduleMicrotask(self)
	end

	local function reject(reason)
		if self._state ~= PENDING then return end
		if token and token.cancelled then return end

		self._state = REJECTED
		self._value = reason
		scheduleMicrotask(self)
	end

	task.spawn(function()
		local ok, err = pcall(executor, resolve, reject, token)
		if not ok then reject(err) end
	end)

	return self
end

function Promise:_flushCallbacks()
	local callbacks = self._callbacks
	self._callbacks = {}

	for _, cb in ipairs(callbacks) do
		local fn = (self._state == FULFILLED) and cb.fulfilled or cb.rejected
		if fn then
			local ok, result = pcall(fn, self._value)
			if ok then
				if cb.resolve then cb.resolve(result) end
			else
				if cb.reject then cb.reject(result) end
			end
		end
	end
end

function Promise:andThen(onFulfilled, onRejected)
	local parent = self

	return Promise.new(function(resolve, reject)
		local function handleFulfilled(value)
			if onFulfilled then
				local ok, result = pcall(onFulfilled, value)
				if ok then resolve(result) else reject(result) end
			else
				resolve(value)
			end
		end

		local function handleRejected(reason)
			if onRejected then
				local ok, result = pcall(onRejected, reason)
				if ok then resolve(result) else reject(result) end
			else
				reject(reason)
			end
		end

		if parent._state == PENDING then
			table.insert(parent._callbacks, {
				fulfilled = onFulfilled,
				rejected = onRejected,
				resolve = resolve,
				reject = reject,
			})
		elseif parent._state == FULFILLED then
			task.defer(function()
				handleFulfilled(parent._value)
			end)
		else
			task.defer(function()
				handleRejected(parent._value)
			end)
		end
	end)
end

function Promise:catch(onRejected)
	return self:andThen(nil, onRejected)
end

function Promise:finally(onFinally)
	return self:andThen(
		function(value)
			if onFinally then onFinally() end
			return value
		end,
		function(reason)
			if onFinally then onFinally() end
			error(reason)
		end
	)
end

function Promise:Error(onError)
	return self:andThen(nil, function(reason)
		local ok, res = pcall(onError, reason)
		if not ok then error(res) end
		return res
	end)
end

function Promise:await()
	if self._state == FULFILLED then
		return self._value
	elseif self._state == REJECTED then
		error(self._value)
	end

	return coroutine.yield(function(resolve)
		self:andThen(resolve, function(err)
			error(err)
		end)
	end)
end

function Promise.resolve(value)
	return Promise.new(function(resolve)
		resolve(value)
	end)
end

function Promise.reject(reason)
	return Promise.new(function(_, reject)
		reject(reason)
	end)
end

function Promise.delay(seconds)
	return Promise.new(function(resolve)
		task.delay(seconds, resolve)
	end)
end

function Promise.all(promises)
	return Promise.new(function(resolve, reject)
		local results = {}
		local remaining = #promises

		if remaining == 0 then
			resolve(results)
			return
		end

		for i, p in ipairs(promises) do
			p:andThen(function(value)
				results[i] = value
				remaining -= 1
				if remaining == 0 then resolve(results) end
			end):catch(reject)
		end
	end)
end

function Promise.race(promises)
	return Promise.new(function(resolve, reject)
		for _, p in ipairs(promises) do
			p:andThen(resolve):catch(reject)
		end
	end)
end

function Promise.timeOut(promise, seconds)
	return Promise.new(function(resolve, reject)
		local timedOut = false

		task.delay(seconds, function()
			timedOut = true
			reject("Promise timed out")
		end)

		promise:andThen(function(value)
			if not timedOut then resolve(value) end
		end):catch(function(err)
			if not timedOut then reject(err) end
		end)
	end)
end

function retryHelper(executor, remaining, delaySeconds, resolve, reject)
	Promise.new(executor)
		:andThen(resolve)
		:catch(function(err)
			if remaining > 0 then
				if delaySeconds then
					task.delay(delaySeconds, function()
						retryHelper(executor, remaining - 1, delaySeconds, resolve, reject)
					end)
				else
					retryHelper(executor, remaining - 1, nil, resolve, reject)
				end
			else
				reject(err)
			end
		end)
end

function Promise.retry(executor, retries)
	return Promise.new(function(resolve, reject)
		retryHelper(executor, retries, nil, resolve, reject)
	end)
end

function Promise.retryDelay(executor, retries, delaySeconds)
	return Promise.new(function(resolve, reject)
		retryHelper(executor, retries, delaySeconds, resolve, reject)
	end)
end

function Promise.retryAsync(executor, retries, delaySeconds)
	retries = retries or 30
	delaySeconds = delaySeconds or 0.3

	return Promise.new(function(resolve, reject)
		retryHelper(executor, retries, delaySeconds, resolve, reject)
	end)
end

function Promise.fromEvent(signal)
	return Promise.new(function(resolve)
		local conn
		conn = signal:Connect(function(...)
			resolve(...)
			conn:Disconnect()
		end)
	end)
end

function Promise.fromEvents(signals)
	return Promise.new(function(resolve)
		local results = {}
		local completed = 0
		local total = #signals

		for i, s in ipairs(signals) do
			s:Connect(function(...)
				results[i] = {...}
				completed += 1
				if completed == total then resolve(results) end
			end)
		end
	end)
end

local LOG_LEVELS = { Debug = true, Info = true, Warn = true, Error = true }

function Promise.LogMessage(level, message)
	return Promise.new(function(resolve, reject)
		if not LOG_LEVELS[level] then
			reject("Invalid log level")
			return
		end

		local msg = string.format("[Promise] [%s] %s", level, message)

		if level == "Error" then
			reject(msg)
			error(msg)
		elseif level == "Warn" then
			warn(msg)
		else
			print(msg)
		end

		resolve(msg)
	end)
end

function Promise.CreateEmbed(embed)
	return Promise.new(function(resolve, reject)
		if type(embed) ~= "table" then reject("Embed must be a table") return end
		if embed.title and #embed.title > 256 then reject("Title too long") return end
		if embed.description and #embed.description > 4086 then reject("Description too long") return end
		resolve(embed)
	end)
end

function Promise.sendToDiscord(url, data, contentType)
	return Promise.new(function(resolve, reject)
		local ok, res = pcall(function()
			local json = HttpService:JSONEncode(data)
			return HttpService:PostAsync(url, json, contentType or Enum.HttpContentType.ApplicationJson)
		end)

		if ok and res then resolve(res) else reject("Failed to send to Discord") end
	end)
end

return Promise

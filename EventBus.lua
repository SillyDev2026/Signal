--!strict
--!optimize 2

-- Lightweight per-event listener registry. Callbacks are scheduled independently
-- so yielding user handlers cannot stall a publisher.
local EventBus = {}
EventBus.__index = EventBus

export type Connection = {
    Connected: boolean,
    Disconnect: (self: Connection) -> (),
}

type Callback = (...any) -> ()
type Listener = {callback: Callback, priority: number, order: number, once: boolean, connection: Connection}
type Registry = {[string]: {Listener}}

function EventBus.new()
    return setmetatable({
        _listeners = {} :: Registry,
        _order = 0,
        _destroyed = false,
    }, EventBus)
end

function EventBus:_subscribe(eventName: string, callback: Callback, priority: number?, once: boolean): Connection
    assert(not self._destroyed, "EventBus is destroyed")
    assert(type(eventName) == "string" and eventName ~= "", "Event name must be a nonempty string")
    assert(type(callback) == "function", "Listener must be a function")
    local rank = priority or 0
    assert(type(rank) == "number" and rank == rank and math.abs(rank) < math.huge, "Invalid listener priority")
    self._order += 1
    local bucket = self._listeners[eventName]
    if bucket == nil then
        bucket = {}
        self._listeners[eventName] = bucket
    end

    local connection: Connection = {Connected = true, Disconnect = function() end}
    local listener: Listener = {callback = callback, priority = rank, order = self._order, once = once, connection = connection}
    function connection:Disconnect()
        if not self.Connected then return end
        self.Connected = false
        local index = table.find(bucket, listener)
        if index ~= nil then table.remove(bucket, index) end
    end
    table.insert(bucket, listener)
    table.sort(bucket, function(a, b)
        if a.priority == b.priority then return a.order < b.order end
        return a.priority > b.priority
    end)
    return connection
end

function EventBus:Subscribe(eventName: string, callback: Callback, priority: number?): Connection
    return self:_subscribe(eventName, callback, priority, false)
end

function EventBus:SubscribeOnce(eventName: string, callback: Callback, priority: number?): Connection
    return self:_subscribe(eventName, callback, priority, true)
end

function EventBus:_snapshot(eventName: string): {Listener}
    if self._destroyed then return {} end
    local bucket = self._listeners[eventName]
    if bucket == nil then return {} end
    local copy = table.clone(bucket)
    for _, listener in ipairs(copy) do
        if listener.once then listener.connection:Disconnect() end
    end
    return copy
end

local function invoke(listener: Listener, args: {any}, argc: number)
    local ok, err = pcall(listener.callback, table.unpack(args, 1, argc))
    if not ok then warn("[EventBus] subscriber failed:", err) end
end

function EventBus:Publish(eventName: string, ...): number
    local listeners = self:_snapshot(eventName)
    local args = table.pack(...)
    for _, listener in ipairs(listeners) do
        if listener.once or listener.connection.Connected then
            task.spawn(invoke, listener, args, args.n)
        end
    end
    return #listeners
end

function EventBus:PublishAsync(eventName: string, ...)
    local Promise = require(script.Parent:WaitForChild("Promise"))
    local listeners = self:_snapshot(eventName)
    local args = table.pack(...)
    return Promise.new(function(resolve: (number) -> ())
        local remaining = #listeners
        if remaining == 0 then resolve(0) return end
        for _, listener in ipairs(listeners) do
            task.spawn(function()
                if listener.once or listener.connection.Connected then
                    invoke(listener, args, args.n)
                end
                remaining -= 1
                if remaining == 0 then resolve(#listeners) end
            end)
        end
    end)
end

function EventBus:Clear(eventName: string)
    local bucket = self._listeners[eventName]
    if bucket == nil then return end
    for _, listener in ipairs(bucket) do listener.connection.Connected = false end
    self._listeners[eventName] = nil
end

function EventBus:Destroy()
    if self._destroyed then return end
    self._destroyed = true
    for name in pairs(self._listeners) do self:Clear(name) end
end

return EventBus

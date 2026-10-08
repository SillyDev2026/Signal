--!strict
--!optimize 2

-- Local synchronous/async event bus used by Signal.
local Promise = require(script.Parent.Promise)

local EventBus = {}
EventBus.__index = EventBus

function EventBus.new()
    return setmetatable({_listeners = {}, _destroyed = false}, EventBus)
end

function EventBus:Subscribe(eventName: string, callback: (...any) -> (), priority: number?)
    assert(not self._destroyed, "EVENT_BUS_DESTROYED")
    assert(type(eventName) == "string" and #eventName > 0, "Event name required")
    assert(type(callback) == "function", "Callback required")
    assert(priority == nil or (type(priority) == "number" and priority == priority), "Invalid priority")
    local list = self._listeners[eventName]
    if not list then
        list = {}
        self._listeners[eventName] = list
    end
    local item = {Callback = callback, Priority = priority or 0, Connected = true}
    local index = #list + 1
    while index > 1 and list[index - 1].Priority < item.Priority do
        index -= 1
    end
    table.insert(list, index, item)
    local connection = {Connected = true}
    function connection:Disconnect()
        if not self.Connected then return end
        self.Connected = false
        item.Connected = false
        local indexFound = table.find(list, item)
        if indexFound then table.remove(list, indexFound) end
    end
    return connection
end

function EventBus:SubscribeOnce(eventName: string, callback: (...any) -> (), priority: number?)
    local connection
    connection = self:Subscribe(eventName, function(...: any)
        connection:Disconnect()
        callback(...)
    end, priority)
    return connection
end

function EventBus:Publish(eventName: string, ...: any)
    if self._destroyed then return end
    local list = self._listeners[eventName]
    if not list or #list == 0 then return end
    local snapshot = table.clone(list)
    for _, item in ipairs(snapshot) do
        if item.Connected then item.Callback(...) end
    end
end

function EventBus:PublishAsync(eventName: string, ...: any)
    if self._destroyed then return Promise.reject("EVENT_BUS_DESTROYED") end
    local list = self._listeners[eventName]
    if not list or #list == 0 then return Promise.resolve(nil) end
    local snapshot = table.clone(list)
    local args = table.pack(...)
    return Promise.new(function(resolve, reject)
        local remaining = 0
        local settled = false
        for _, item in ipairs(snapshot) do
            if item.Connected then remaining += 1 end
        end
        if remaining == 0 then resolve(nil) return end
        for _, item in ipairs(snapshot) do
            if item.Connected then
                task.spawn(function()
                    local ok, err = pcall(item.Callback, table.unpack(args, 1, args.n))
                    if not ok and not settled then
                        settled = true
                        reject(err)
                    end
                    remaining -= 1
                    if remaining == 0 and not settled then
                        settled = true
                        resolve(nil)
                    end
                end)
            end
        end
    end)
end

function EventBus:Clear(eventName: string?)
    if self._destroyed then return end
    if eventName ~= nil then
        local list = self._listeners[eventName]
        if list then
            for _, item in ipairs(list) do item.Connected = false end
        end
        self._listeners[eventName] = nil
    else
        for _, list in pairs(self._listeners) do
            for _, item in ipairs(list) do item.Connected = false end
        end
        table.clear(self._listeners)
    end
end

function EventBus:Destroy()
    if self._destroyed then return end
    self:Clear()
    self._destroyed = true
end

return EventBus

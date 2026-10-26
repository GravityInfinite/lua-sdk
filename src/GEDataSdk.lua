---sudo apt install luarocks
---luarocks install luasocket
---luarocks install lua-cjson
local socket = require("socket")
local https = require("ssl.https")
local ltn12 = require("ltn12")
local cjson = require("cjson")

local Util = {}
local GELog = {}

local function class(base, _ctor)
    local c = {}
    if not _ctor and type(base) == 'function' then
        _ctor = base
        base = nil
    elseif type(base) == 'table' then
        for i, v in pairs(base) do
            c[i] = v
        end
        c._base = base
    end
    c.__index = c
    local mt = {}
    mt.__call = function(_, ...)
        local obj = {}
        setmetatable(obj, c)
        if _ctor then
            _ctor(obj, ...)
        end
        return obj
    end
    c._ctor = _ctor
    c.is_a = function(self, klass)
        local m = getmetatable(self)
        while m do
            if m == klass then
                return true
            end
            m = m._base
        end
        return false
    end
    setmetatable(c, mt)
    return c
end

---
---@param consumer any
---@param clientId string
---@param eventType string
---@param eventName string
---@param properties table
local function upload(consumer, clientId, eventType, eventName, properties)
    GELog.info("X upload clientId" .. clientId)

    local eventJson = {}
    eventJson["client_id"] = tostring(clientId)
    eventJson["event_list"] = {}

    event_list_item = {}
    event_list_item["type"] = eventType
    event_list_item["event"] = eventName
    event_list_item["time"] = tonumber(string.format("%d", socket.gettime() * 1000))

    event_list_item["properties"] = properties

    if eventType == 'track' then
        properties["$lib"] = GEAnalytics.platForm
        properties["$lib_version"] = GEAnalytics.version
    end

    GELog.info("X upload clientId" .. clientId)
    eventJson["event_list"][#eventJson["event_list"] + 1] = event_list_item
    local ret = consumer:add(eventJson)
    eventJson = nil
    return ret
end

---
--- Init analytics instance
---@param self any
---@param consumer any batchConsumer/debugConsumer
GEAnalytics = class(function(self, consumer)
    if consumer == nil or type(consumer) ~= "table" then
        GELog.error("consumer params is invalidate.")
        return
    end
    self.consumer = consumer
    self.superProperties = {}
    GELog.info("SDK init success")
end)

--- Enable log or not
---@param enable boolean
function GEAnalytics.enableLog(enable)
    GELog.enable = enable
end

--- Construct debug consumer
---@param self any
---@param url string Project url
---@param callback function Global error callback, invoked when report failed; receives (err, client_id)
GEAnalytics.GEDebugConsumer = class(function(self, url, callback)
    if url == nil or type(url) ~= "string" or string.len(url) == 0 then
        GELog.error("server url can't be empty.")
    end
    self.url = url
    self.callback = callback
    GEAnalytics.strictMode = true
    GELog.info("Mode: debug consumer.  ReceiverUrl: " .. url)
end)

function GEAnalytics.GEDebugConsumer:add(msg)
    local returnCode, code = Util.post(self.url, msg)
    if (returnCode == 0) then
        return true
    end
    self:reportError("report failed, returnCode: " .. tostring(returnCode) .. ", httpCode: " .. tostring(code),
        msg and msg["client_id"] or nil)
    return false
end

--- Invoke the global error callback when report failed
---@param err string|table error
---@param clientId string|nil
function GEAnalytics.GEDebugConsumer:reportError(err, clientId)
    if self.callback ~= nil then
        local ok, callbackErr = pcall(self.callback, err, clientId)
        if not ok then
            GELog.error("error callback failed: " .. tostring(callbackErr))
        end
    end
end

function GEAnalytics.GEDebugConsumer:flush()
end

function GEAnalytics.GEDebugConsumer:close()
    GELog.info("Close debug consumer")
end

--- Construct batch consumer
---@param self any
---@param url string
---@param batchNum number
---@param cacheCapacity number
---@param callback function Global error callback, invoked when report failed; receives (err, client_id)
GEAnalytics.GEBatchConsumer = class(function(self, url, batchNum, cacheCapacity, callback)
    if url == nil or type(url) ~= "string" or string.len(url) == 0 then
        GELog.error("url can't be empty")
    end
    if batchNum ~= nil and type(batchNum) ~= "number" then
        GELog.error("batchNum must be nummber type")
    end
    self.url = url
    self.batchNum = batchNum or GEAnalytics.batchNumber
    self.eventArrayJson = {}
    self.cacheCapacity = cacheCapacity or GEAnalytics.cacheCapacity
    self.cacheTable = {}
    self.callback = callback
    GELog.info("Mode: batch consumer.  ReceiverUrl: " .. url)
end)

function GEAnalytics.GEBatchConsumer:add(msg)
    local num = #self.eventArrayJson + 1
    self.eventArrayJson[num] = msg
    GELog.info("Enqueue data to buffer")
    if (num >= self.batchNum or #self.cacheTable > 0) then
        return self:flush()
    end
    return true
end

function GEAnalytics.GEBatchConsumer:flush(flag)
    if #self.eventArrayJson == 0 and #self.cacheTable == 0 then
        return true
    end
    if (flag or #self.eventArrayJson >= self.batchNum or #self.cacheTable == 0) then
        local events = self.eventArrayJson
        self.eventArrayJson = {}
        table.insert(self.cacheTable, events)
    end
    while (#self.cacheTable > 0)
    do
        local events = self.cacheTable[1]
        -- retry 3 times
        local number = 3
        local success = false
        local returnCode
        local code

        local client_id_map = {}
        for i, v in ipairs(events) do
            local client_id = v['client_id']
            local event_list = v["event_list"]
            if client_id_map[client_id] == nil then
                client_id_map[client_id] = {}
            end
            for j, e in ipairs(event_list) do
                client_id_map[client_id][#client_id_map[client_id] + 1] = e
            end
        end

        local all_ok = true
        for client_id, event_list in pairs(client_id_map) do
            while number > 0 and success == false do
                local i = {}
                i['client_id'] = client_id
                i['event_list'] = event_list
                returnCode, code = Util.post(self.url, i)
                if (code == 200 and returnCode == 0) then
                    success = true
                else
                    success = false
                    all_ok = false
                    number = number - 1
                    self:reportError("report failed, returnCode: " .. tostring(returnCode) .. ", httpCode: " .. tostring(code), client_id)
                end
            end
        end
        if (all_ok) then
            table.remove(self.cacheTable, 1)
        else
            if (#self.cacheTable > self.cacheCapacity) then
                table.remove(self.cacheTable, 1)
            end
            return false
        end
        if (not flag) then
            if (returnCode ~= 0) then
                return false
            else
                return true
            end
        end
    end

    return true
end

function GEAnalytics.GEBatchConsumer:close()
    self:flush(true)
    GELog.info("Close batch consumer")
end

--- Invoke the global error callback when report failed
---@param err string|table error
---@param clientId string|nil
function GEAnalytics.GEBatchConsumer:reportError(err, clientId)
    if self.callback ~= nil then
        local ok, callbackErr = pcall(self.callback, err, clientId)
        if not ok then
            GELog.error("error callback failed: " .. tostring(callbackErr))
        end
    end
end

--- Set user properties. Would overwrite existing names
---@param clientId string
---@param properties table
function GEAnalytics:userSet(clientId, properties)
    local ok, ret = pcall(upload, self.consumer, clientId, "profile", "profile_set", properties)
    if ok then
        return ret
    end
end

--- Set user properties, if such property had been set before, this message would be neglected.
---@param clientId string
---@param properties table
function GEAnalytics:userSetOnce(clientId, properties)
    local ok, ret = pcall(upload, self.consumer, clientId, "profile", "profile_set_once", properties)
    if ok then
        return ret
    end
end

--- To accumulate operations against the property
---@param clientId string
---@param properties table
function GEAnalytics:userIncrement(clientId, properties)
    local ok, ret = pcall(upload, self.consumer, clientId, "profile", "profile_increment", properties)
    if ok then
        return ret
    end
end

--- To add user properties of array type
---@param clientId string
---@param properties table
function GEAnalytics:userAppend(clientId, properties)
    local ok, ret = pcall(upload, self.consumer, clientId, "profile", "profile_append", properties)
    if ok then
        return ret
    end
end

--- Append user properties to array type by unique.
---@param clientId string
---@param properties table
function GEAnalytics:userUniqAppend(clientId, properties)
    local ok, ret = pcall(upload, self.consumer, clientId, "profile", "profile_uniq_append", properties)
    if ok then
        return ret
    end
end

--- Set user properties to max val.
---@param clientId string
---@param properties table
function GEAnalytics:userMax(clientId, properties)
    local ok, ret = pcall(upload, self.consumer, clientId, "profile", "profile_number_max", properties)
    if ok then
        return ret
    end
end

--- Set user properties to min val.
---@param clientId string
---@param properties table
function GEAnalytics:userMin(clientId, properties)
    local ok, ret = pcall(upload, self.consumer, clientId, "profile", "profile_number_min", properties)
    if ok then
        return ret
    end
end
--- Clear the user properties of users
---@param clientId string
---@param properties table
function GEAnalytics:userUnset(clientId, properties)
    local ok, ret = pcall(upload, self.consumer, clientId, "profile", "profile_unset", properties)
    if ok then
        return ret
    end
end

--- Delete a user, This operation cannot be undone
---@param clientId string
function GEAnalytics:userDel(clientId)
    local ok, ret = pcall(upload, self.consumer, clientId, "profile", "profile_delete", {})
    if ok then
        return ret
    end
end

--- Report ordinary event
---@param clientId string
---@param eventName string
---@param properties table
function GEAnalytics:track(clientId, eventName, properties)
    GELog.info("Mode: debug consumer.eventName: " .. eventName)

    local ok, ret = pcall(upload, self.consumer, clientId, "track", eventName, properties)
    if ok then
        return ret
    end
end

--- Flush data
function GEAnalytics:flush()
    self.consumer:flush()
end

--- Close SDK
function GEAnalytics:close()
    self.consumer:close()
    GELog.info("SDK closed.")
end

function GEAnalytics:toString()
    return self.consumer:toString()
end

GEAnalytics.platForm = "lua"
GEAnalytics.version = "1.0.0"
GEAnalytics.batchNumber = 20
GEAnalytics.cacheCapacity = 50

function Util.post(url, eventArrayJson)
    local request_body = Util.toJson(eventArrayJson)
    local contentType = "application/json"
    GELog.info("Send data, request = " .. request_body)

    local response_body = {}
    local count = 0
    local res, code
    while (count < 3)
    do
        local params = {
            url = url,
            method = "POST",
            headers = {
                ["GE-Integration-Type"] = GEAnalytics.platForm;
                ["GE-Integration-Version"] = GEAnalytics.version;
                ["GE-Integration-Count"] = #eventArrayJson;
                ["Content-Type"] = contentType;
                ["Content-Length"] = #request_body;
            },
            ssl_params = {
                verify = "none"
            },
            source = ltn12.source.string(request_body),
            sink = ltn12.sink.table(response_body),
        }
        res, code = https.request(params)

        res = table.concat(response_body)
        GELog.info("Send data, response = " .. (res or ""))
        if code ~= nil and type(code) == "number" and tonumber(code) == 200 then
            break
        end
        count = count + 1
    end
    if count >= 3 then
        return -1, code
    end
    local resultCode
    local resultJson = cjson.decode(res)
    resultCode = tonumber(resultJson["code"])
    if resultCode ~= 0 then
        local msg = resultJson["msg"]
        GELog.error("Up failed:" .. resultCode .. ", msg:" .. msg)
    end
    return resultCode, code
end

function Util.toJson(eventArrayJson)
    return cjson.encode(eventArrayJson)
end

function Util.mergeTables(...)
    local tabs = { ... }
    if not tabs then
        return {}
    end
    local origin = tabs[1]
    for i = 2, #tabs do
        if origin then
            if tabs[i] then
                for k, v in pairs(tabs[i]) do
                    if (v ~= nil) then
                        origin[k] = v
                    end
                end
            end
        else
            origin = tabs[i]
        end
    end
    return origin
end

function Util.tablecopy(src, dest)
    for k, v in pairs(src) do
        if type(v) == "table" then
            dest[k] = {}
            Util.tablecopy(v, dest[k])
        else
            dest[k] = v
        end
    end
end

Util.enableLog = false

GELog.enable = false
function GELog.info(...)
    if GELog.enable then
        io.write("[GEData][" .. os.date("%Y-%m-%d %H:%M:%S") .. "][Info] ")
        print(...)
    end
end

function GELog.error(...)
    if GELog.enable then
        io.write("[GEData][" .. os.date("%Y-%m-%d %H:%M:%S") .. "][Error] ")
        print(...)
    end
end

return GEAnalytics

local GEAnalytics = require("GEDataSdk")

SERVER_URL = 'https://backend.gravity-engine.com/event_center/api/v1/event/collect/?access_token=___XXX___'

local function getDebugConsumer()
    return GEAnalytics.GEDebugConsumer(SERVER_URL)
end

local function getBatchConsumer()
    return GEAnalytics.GEBatchConsumer(SERVER_URL, 20, 50)
end

GEAnalytics.enableLog(true)

-- local consumer = getDebugConsumer()
local consumer = getBatchConsumer()

--- init SDK with consumer
local sdk = GEAnalytics(consumer, false, true)

local clientId = "_test_client_id_0"
local properties = {}

for i = 0, 100, 1 do
    properties["idx"] = i
    properties["productNames"] = { "Lua", "hello" }
    properties["productType"] = "Lua book"
    properties["producePrice"] = 80
    properties["shop"] = "xx-shop"
    properties["#os"] = "1.1.1.1"
    properties["date"] = os.date()
    properties["date1"] = os.date("%Y-%m-%d %H:%M:%S")
    sdk:track(clientId, "$AdClick", properties)
end

properties = {}
properties['prop_set_once'] = "once";
sdk:userSetOnce(clientId, properties)

properties = {}
properties['user_name'] = "xxx";
sdk:userSet(clientId, properties)

properties = {}
properties['user_name'] = "";
sdk:userUnset(clientId, properties)

properties = {}
properties['prop_list_type'] = { 'a', 'a', 'b' }
sdk:userAppend(clientId, properties)

properties = {}
properties['prop_list_type'] = { 'a', 'a', 'b', 'c' }
sdk:userUniqAppend(clientId, properties)

properties = {}
properties['TotalRevenue'] = 100
sdk:userIncrement(clientId, properties)

properties = {}
properties['TotalRevenue'] = 1000
sdk:userMax(clientId, properties)

properties = {}
properties['TotalRevenue'] = 1
sdk:userMin(clientId, properties)

sdk:userDel(clientId)

properties = nil

sdk:flush()
sdk:close()


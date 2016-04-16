local function debug(wat)
    redis.call("SET", "debug", cjson.encode(wat))
end

local function eventSetKey(es)
    local k = "eventset:{"
    k = k .. es.Base .. "}"
    for i = 1,#es.Subs do
        k = k .. ":" .. es.Subs[i]
    end
    return k
end

local function expandEvent(e)
    if e.ID and e.packed then
        return e
    elseif e.ID then
        e.packed = cmsgpack.pack(e)
        return e
    else
        local ee = cmsgpack.unpack(e)
        ee.packed = e
        return ee
    end
end

local function formatQEMinMax(m, excl, inf)
    local mm = string.format("%.0f", m)
    if mm == "0" then
        mm = inf
    elseif excl then
        mm = "(" .. mm
    end
    return mm
end

local function query_select(qs)
    local esKey = eventSetKey(qs.EventSet)
    if qs.QueryEventRangeSelect then
        local qe = qs.QueryEventRangeSelect

        if qe.MinQuerySelector then
            local minEE = query_select(qe.MinQuerySelector)
            if #minEE > 0 then
                qe.Min = minEE[#minEE].ID
            end
        end
        local min = formatQEMinMax(qe.Min, qe.MinExcl, "-inf")

        if qe.MaxQuerySelector then
            local maxEE = query_select(qe.MaxQuerySelector)
            if #maxEE > 0 then
                qe.Max = maxEE[1].ID
            end
        end
        local max = formatQEMinMax(qe.Max, qe.MaxExcl, "+inf")

        local ee
        if qe.Limit ~= 0 then
            ee = redis.call("ZRANGEBYSCORE", esKey, min, max, "LIMIT", qe.Offset, qe.Limit)
        else
            ee = redis.call("ZRANGEBYSCORE", esKey, min, max)
        end
        for i = 1,#ee do
            ee[i] = expandEvent(ee[i])
        end
        return ee

    elseif qs.Events then
        for i = 1,#qs.Events do
            qs.Events[i] = expandEvent(qs.Events[i])
        end
        return qs.Events
    end
end

local qa = cmsgpack.unpack(ARGV[1])
local result_set = query_select(qa.QuerySelector)

for i = 1, #qa.AddTo do
    local esKey = eventSetKey(qa.AddTo[i])
    for i = 1, #result_set do
        local e = result_set[i]
        redis.call("ZADD", esKey, e.ID, e.packed)
    end
end

for i = 1, #qa.RemoveFrom do
    local esKey = eventSetKey(qa.RemoveFrom[i])
    for i = 1, #result_set do
        local e = result_set[i]
        redis.call("ZREM", esKey, e.packed)
    end
end

for i = 1,#result_set do
    result_set[i].packed = nil
end

return cmsgpack.pack({Events = result_set})

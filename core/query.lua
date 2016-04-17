local nowTS = cmsgpack.unpack(ARGV[1])

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

-- predefined this because it and query_select_inner call each other recursively
local query_select

-- Does the actual work of doing the query selection. Returns a result set, and
-- a boolean indicating if the result set needs expanding. Should only be called
-- by query_select
local function query_select_inner(qs)
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

        if qe.Limit ~= 0 then
            return redis.call("ZRANGEBYSCORE", esKey, min, max, "LIMIT", qe.Offset, qe.Limit), true
        else
            return redis.call("ZRANGEBYSCORE", esKey, min, max), true
        end

    elseif #qs.PosRangeSelect > 0 then
        local pr = qs.PosRangeSelect
        return redis.call("ZRANGE", esKey, pr[1], pr[2]), true

    elseif #qs.Newest > 0 then
        local n = qs.Newest
        local newest_set = {}
        for i = 1,#n do
            local rs = query_select(n[i])
            if #rs > 0 then
                if #newest_set == 0 then
                    newest_set = rs
                elseif #rs > 0 and newest_set[#newest_set].ID < rs[#rs].ID then
                    newest_set = rs
                end
            end
        end
        return newest_set, false

    elseif #qs.FirstNotEmpty > 0 then
        local fn = qs.FirstNotEmpty
        for i = 1,#fn do
            local rs = query_select(fn[i])
            if #rs > 0 then return rs, false end
        end
        return {}

    elseif qs.Events then
        return qs.Events, true
    end
end

-- Does a selection, always returns and expanded result set
query_select = function(qs)
    local rs, expand = query_select_inner(qs)
    local rs_filtered = {}
    for i = 1,#rs do
        local rsi = rs[i]
        if expand then rsi = expandEvent(rsi) end
        -- ~= is not equals, which is synonomous with xor
        if (rsi.Expire > nowTS ~= qs.FilterNotExpired) then
            table.insert(rs_filtered, rsi)
        end
    end
    return rs_filtered
end

local qa = cmsgpack.unpack(ARGV[2])
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

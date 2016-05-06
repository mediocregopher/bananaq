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
    local ee
    if e.ID and e.packed then
        ee = e
    elseif e.ID then
        e.packed = cmsgpack.pack(e)
        ee = e
    else
        ee = cmsgpack.unpack(e)
        ee.packed = e
    end

    ee.Contents = nil
    return ee
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
local function query_select_inner(input, qs)
    local esKey = eventSetKey(qs.EventSet)
    if qs.QueryEventRangeSelect then
        local qe = qs.QueryEventRangeSelect

        if qe.MinFromInput then
            if #input > 0 then qe.Min = input[#input].ID else qe.Min = 0 end
        end
        local min = formatQEMinMax(qe.Min, qe.MinExcl, "-inf")

        if qe.MaxFromInput then
            if #input > 0 then qe.Max = input[1].ID else qe.Max = 0 end
        end
        local max = formatQEMinMax(qe.Max, qe.MaxExcl, "+inf")

        local zrangebyscore = "ZRANGEBYSCORE"
        if qe.Reverse then
            zrangebyscore = "ZREVRANGEBYSCORE"
            min, max = max, min
        end

        if qe.Limit ~= 0 then
            return redis.call(zrangebyscore, esKey, min, max, "LIMIT", qe.Offset, qe.Limit)
        else
            return redis.call(zrangebyscore, esKey, min, max)
        end

    elseif #qs.PosRangeSelect > 0 then
        local pr = qs.PosRangeSelect
        return redis.call("ZRANGE", esKey, pr[1], pr[2])

    elseif qs.Events then
        return qs.Events
    end
end

-- Does a selection, always returns an expanded output. Also handles Union
-- and other Selector modifiers
query_select = function(input, qs)
    local output = query_select_inner(input, qs)
    for i = 1,#output do
        output[i] = expandEvent(output[i])
    end

    if qs.Union then
        local set = {}
        for i = 1,#output do
            set[output[i].ID] = output[i]
        end
        for i = 1,#input do
            set[input[i].ID] = input[i]
        end

        output = {}
        for _, e in pairs(set) do
            table.insert(output, e)
        end
    end

    -- We always sort the output by ID. It kind of sucks, but there's no way of
    -- knowing that the events were stored ordered by ID versus something else,
    -- and for Union we have to do it anyway
    local ids = {}
    local byid = {}
    for i = 1, #output do
        table.insert(ids, output[i].ID)
        byid[output[i].ID] = output[i]
    end
    table.sort(ids)
    local sorted_output = {}
    for i = 1, #ids do
        table.insert(sorted_output, byid[ids[i]])
    end
    return sorted_output
end

local function query_filter(input, qf)
    local output = {}
    for i = 1, #input do
        local e = input[i]
        local filter
        if qf.Expired then
            filter = e.ID <= nowTS
        end
        -- ~= is not equals, which is synonomous with xor
        filter = filter ~= qf.Invert
        if not filter then table.insert(output, e) end
    end
    return output
end


-- Returns true if the conditional succeeds, i.e. the QueryAction should be
-- performed
local function query_conditional(input, qc)
    if qc.And then
        for i = 1,#qc.And do
            if not query_conditional(input, qc.And[i]) then return false end
        end
    end
    if qc.IfNoInput and #input > 0 then return false end
    if qc.IfInput and #input == 0 then return false end
    if qc.IfEmpty then
        local key = eventSetKey(qc.IfEmpty)
        if redis.call("ZCARD", key) > 0 then return false end
    end
    if qc.IfNotEmpty then
        local key = eventSetKey(qc.IfNotEmpty)
        if redis.call("ZCARD", key) == 0 then return false end
    end
    return true
end

-- performs the QueryAction with the given input set. Returns the new input set,
-- and whether or not the action was skipped (which it might be if a conditional
-- stopped it from happening)
local function query_action(input, qa)
    if not query_conditional(input, qa.QueryConditional) then return input, true end

    if qa.QuerySelector then return query_select(input, qa.QuerySelector), false end

    if qa.QueryAddTo then
        for i = 1, #qa.QueryAddTo.EventSets do
            local esKey = eventSetKey(qa.QueryAddTo.EventSets[i])
            for i = 1, #input do
                local score = input[i].ID
                if qa.QueryAddTo.Score > 0 then score = qa.QueryAddTo.Score end
                redis.call("ZADD", esKey, score, input[i].packed)
            end
        end
        return input, false
    end

    if #qa.RemoveFrom > 0 then
        for i = 1, #qa.RemoveFrom do
            local esKey = eventSetKey(qa.RemoveFrom[i])
            for i = 1, #input do
                redis.call("ZREM", esKey, input[i].packed)
            end
        end
        return input, false
    end

    if qa.QueryFilter then return query_filter(input, qa.QueryFilter), false end

    -- Shouldn't really get here but whatever
    return input, false
end

local qas = cmsgpack.unpack(ARGV[2])
local ee = {}
local skipped
for i = 1,#qas.QueryActions do
    ee, skipped = query_action(ee, qas.QueryActions[i])
    if not skipped and qas.QueryActions[i].Break then break end
end

for i = 1,#ee do
    ee[i].packed = nil
end

return cmsgpack.pack({Events = ee})

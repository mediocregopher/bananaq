local nowTS = cmsgpack.unpack(ARGV[1])
local prefix = ARGV[3]

local function debug(wat)
    redis.call("SET", "debug", cjson.encode(wat))
end

local function keyString(k)
    local str = prefix .. ":k:{"
    str = str .. k.Base .. "}"
    for i = 1,#k.Subs do
        str = str .. ":" .. k.Subs[i]
    end
    return str
end

local function expandID(id)
    local idout
    if type(id) == "string" then
        idout = cmsgpack.unpack(id)
        idout.packed = id
    elseif id.T and id.packed then
        idout = id
    elseif id.T then
        id.packed = cmsgpack.pack(id)
        idout = id
    else
        error("can't expand broke ass id value")
    end

    idout.Contents = nil
    return idout
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

-- handles determining the min/max parameters for a command following the score
-- range syntax in redis (e.g. zrangebyscore). Returns the string to use as min
-- and the string to use as max
local function query_score_range(input, qsr)
    if qsr.MinFromInput then
        if #input > 0 then qsr.Min = input[#input].T else qsr.Min = 0 end
    end
    local min = formatQEMinMax(qsr.Min, qsr.MinExcl, "-inf")

    if qsr.MaxFromInput then
        if #input > 0 then qsr.Max = input[1].T else qsr.Max = 0 end
    end
    local max = formatQEMinMax(qsr.Max, qsr.MaxExcl, "+inf")
    return min, max
end

-- predefined this because it and query_select_inner call each other recursively
local query_select

-- Does the actual work of doing the query selection. Returns a result set, and
-- a boolean indicating if the result set needs expanding. Should only be called
-- by query_select
local function query_select_inner(input, qs)
    local key = keyString(qs.Key)
    if qs.QueryRangeSelect then
        local qsr = qs.QueryRangeSelect
        local min, max = query_score_range(input, qsr.QueryScoreRange)

        local zrangebyscore = "ZRANGEBYSCORE"
        if qsr.Reverse then
            zrangebyscore = "ZREVRANGEBYSCORE"
            min, max = max, min
        end

        local ret
        if qsr.Limit ~= 0 then
            ret = redis.call(zrangebyscore, key, min, max, "LIMIT", qsr.Offset, qsr.Limit)
        else
            ret = redis.call(zrangebyscore, key, min, max)
        end
        --debug({ret=ret, esKey=esKey, cmd=zrangebyscore, min=min, max=max})
        return ret

    elseif qs.QueryIDScoreSelect then
        local qiss = qs.QueryIDScoreSelect
        local id = expandID(qiss.ID)
        local scoreRaw = redis.call("ZSCORE", key, id.packed)
        if not scoreRaw then return {} end
        local score = tonumber(scoreRaw)

        if score < qiss.Min then return {} end
        if qiss.Max > 0 and score > qiss.Max then return {} end
        if qiss.Equal > 0 and score ~= qiss.Equal then return {} end
        return {id}

    elseif #qs.PosRangeSelect > 0 then
        local pr = qs.PosRangeSelect
        return redis.call("ZRANGE", key, pr[1], pr[2])

    elseif qs.IDs then
        return qs.IDs
    end
end

-- Does a selection, always returns an expanded output. Also handles Union
-- and other Selector modifiers
query_select = function(input, qs)
    local output = query_select_inner(input, qs)
    for i = 1,#output do
        output[i] = expandID(output[i])
    end
    return output
end

local function query_filter(input, qf)
    local output = {}
    for i = 1, #input do
        local id = input[i]
        local filter
        if qf.Expired then
            filter = id.Expire <= nowTS
        end
        -- ~= is not equals, which is synonomous with xor
        filter = filter ~= qf.Invert
        if not filter then table.insert(output, id) end
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
        local key = keyString(qc.IfEmpty)
        if redis.call("EXISTS", key) > 0 then return false end
    end
    if qc.IfNotEmpty then
        local key = keyString(qc.IfNotEmpty)
        if redis.call("EXISTS", key) == 0 then return false end
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
        for i = 1, #qa.QueryAddTo.Keys do
            local key = keyString(qa.QueryAddTo.Keys[i])
            for i = 1, #input do
                local score = input[i].T
                if qa.QueryAddTo.ExpireAsScore then score = input[i].Expire end
                if qa.QueryAddTo.Score > 0 then score = qa.QueryAddTo.Score end
                redis.call("ZADD", key, score, input[i].packed)
            end
        end
        return input, false
    end

    if #qa.RemoveFrom > 0 then
        for i = 1, #qa.RemoveFrom do
            local key = keyString(qa.RemoveFrom[i])
            for i = 1, #input do
                redis.call("ZREM", key, input[i].packed)
            end
        end
        return input, false
    end

    if qa.QueryRemoveByScore then
        local qrems = qa.QueryRemoveByScore
        local min, max = query_score_range(input, qrems.QueryScoreRange)
        for i = 1, #qrems.Keys do
            local key = keyString(qrems.Keys[i])
            redis.call("ZREMRANGEBYSCORE", key, min, max)
        end
        return input, false
    end

    if qa.QuerySingleSet then
        local qss = qa.QuerySingleSet
        local key = keyString(qss.Key)
        if #input > 0 then
            if qss.IfNewer then
                local oldi = redis.call("GET", key)
                if oldi then
                    oldi = expandID(oldi)
                    if oldi.T > input[1].T then
                        return input, false
                    end
                end
            end

            local exp = math.floor(input[1].Expire / 1000) -- to milliseconds
            exp = exp + 1 -- add one for funsies
            redis.call("SET", key, input[1].packed)
            redis.call("PEXPIREAT", key, exp)
        end
        return input, false
    end

    if qa.SingleGet then
        local key = keyString(qa.SingleGet)
        local id = redis.call("GET", key)
        if not id then return {}, false end
        id = expandID(id)
        if id.Expire < nowTS then return {}, false end
        return {id}, false
    end

    if qa.QueryFilter then return query_filter(input, qa.QueryFilter), false end

    -- Shouldn't really get here but whatever
    return input, false
end

local function sort_ids(ii)
    local ids = {}
    local byid = {}
    for i = 1, #ii do
        table.insert(ids, ii[i].T)
        byid[ii[i].T] = ii[i]
    end
    table.sort(ids)
    local sorted_ii = {}
    for i = 1, #ids do
        table.insert(sorted_ii, byid[ids[i]])
    end
    return sorted_ii
end

local qas = cmsgpack.unpack(ARGV[2])
local ii = {}
for i = 1,#qas.QueryActions do
    local qa = qas.QueryActions[i]
    local newii, skipped = query_action(ii, qa)
    if not skipped and qas.QueryActions[i].Break then break end

    if qa.Union then
        local set = {}
        for i = 1,#ii do
            set[ii[i].T] = ii[i]
        end
        for i = 1,#newii do
            set[newii[i].T] = newii[i]
        end

        newii = {}
        for _, e in pairs(set) do
            table.insert(newii, e)
        end
    end

    -- We always sort the output by T. It kind of sucks, but there's no way of
    -- knowing that the ids were stored ordered by T versus something else, and
    -- for Union we have to do it anyway
    ii = sort_ids(newii)
end

for i = 1,#ii do
    ii[i].packed = nil
end

return cmsgpack.pack({IDs = ii})

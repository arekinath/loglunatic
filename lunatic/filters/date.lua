--[[
loglunatic -- logstash for lunatics
Copyright (c) 2013, Alex Wilson, the University of Queensland
Distributed under a BSD license -- see the LICENSE file in the root of the distribution.
]]

local exports = {}

local f = require("lunatic/filters/common")
local lpeg = require("lpeg")
V = lpeg.V; R = lpeg.R; S = lpeg.S; Ct = lpeg.Ct; Cg = lpeg.Cg; P = lpeg.P; C = lpeg.C

local date_parsers = {}
date_parsers["http"] = f.new_parser("date", {
	tz = (C(S("+-") * P("1") * R("09")) * R("09")^2) +
		 (C(S("+-") * R("09")) * R("09")^2),
	datepart = Cg(V("number"), "day") * S("/-") *
		Cg(V("word"), "month") * S("/-") *
		Cg(V("number"), "year"),
	timepart = Cg(V("number"), "hour") * P(":") *
		Cg(V("number"), "min") * P(":") *
		Cg(V("number"), "sec"),
	date = Ct(
		V("datepart") * P(":") * V("timepart") *
		(S(" ")^1 * Cg(V("tz"), "timezone"))^-1 )
})
date_parsers["euro"] = f.new_parser("date", {
	tz = (C(S("+-") * P("1") * R("09")) * R("09")^2) +
		 (C(S("+-") * R("09")) * R("09")^2),
	date = Ct(
		Cg(V("number"), "year") * S("/-") *
		Cg(V("number"), "month") * S("/-") *
		Cg(V("number"), "day") * S(" -:")^1 *
		Cg(V("number"), "hour") * S(":-.") *
		Cg(V("number"), "min") * S(":-. ") *
		Cg(V("number"), "sec") *
		(S(" ")^1 * Cg(V("tz"), "timezone"))^-1
	)
})
date_parsers["apache"] = f.new_parser("date", {
	tz = (C(S("+-") * P("1") * R("09")) * R("09")^2) +
		 (C(S("+-") * R("09")) * R("09")^2),
	timepart = Cg(V("number"), "hour") * P(":") *
		Cg(V("number"), "min") * P(":") *
		Cg(V("number"), "sec"),
	datepart = Cg(V("word"), "dayofweek") * V("spc") *
		Cg(V("word"), "month") * V("spc") *
		Cg(V("number"), "day"),
	date = Ct(
		V("datepart") * V("spc") *
		V("timepart") * V("spc") *
		Cg(V("number"), "year")
	)
})

local isotime = "%Y-%m-%dT%H:%M:%S.000Z"
local short_months = {
	["Jan"]=1, ["Feb"]=2, ["Mar"]=3, ["Apr"]=4, ["May"]=5, ["Jun"]=6,
	["Jul"]=7, ["Aug"]=8, ["Sep"]=9, ["Oct"]=10, ["Nov"]=11, ["Dec"]=12
}
local long_months = {
	["January"]=1, ["February"]=2, ["March"]=3, ["April"]=4, ["May"]=5,
	["June"]=6, ["July"]=7, ["August"]=8, ["September"]=9, ["October"]=10,
	["November"]=11, ["December"]=12
}

local date = f.new_filter("date")
function date:run(input)
	local now = os.time()
	local tzoffset = os.difftime(now, os.time(os.date("!*t", now)))
	input.timestamp = os.date(isotime, os.time() - tzoffset)

	if input.fields[self.key] == nil then return input end
	local m = lpeg.match(self.parser, input.fields[self.key])
	if m ~= nil then
		m.day = tonumber(m.day)
		if short_months[m.month] ~= nil then
			m.month = short_months[m.month]
		elseif long_months[m.month] ~= nil then
			m.month = long_months[m.month]
		else
			m.month = tonumber(m.month)
		end
		m.year = tonumber(m.year)
		m.hour = tonumber(m.hour)
		m.min = tonumber(m.min)
		m.sec = tonumber(m.sec)

		local time = nil
		pcall(function() time = os.time(m) end)
		if time == nil then
			io.write("date: nil time? m = { ")
			for k,v in pairs(m) do
				io.write(k .. "=" .. v .. " ")
			end
			io.write("}\n")
			io.write("from message: '" .. input.message .. "'\n")
			return input
		end
		if m.timezone ~= nil then
			m.timezone = tonumber(m.timezone)
			time = time - (m.timezone * 3600)
		else
			-- assume it's in local time
			time = time - tzoffset
		end
		input.timestamp = os.date(isotime, time)
	end
	return input
end
function exports.date(tbl)
	local typ = tbl.type or "euro"
	local key = tbl.key or tbl.field or "timestamp"
	local parser = date_parsers[typ]
	if parser == nil or tbl.pattern then
		if tbl.pattern and (type(tbl.pattern) == "table" or type(tbl.pattern) == "string") then
			parser = f.grok_compile(tbl.pattern)
		else
			error("date: must specify a 'type' or 'pattern'")
		end
	end
	local t = { ["type"] = typ, ["key"] = key, ["parser"] = parser }
	setmetatable(t, date)
	return t
end

-- just return our toplevel function
return exports.date

--[[
loglunatic -- logstash for lunatics
Copyright (c) 2013, Alex Wilson, the University of Queensland
Distributed under a BSD license -- see the LICENSE file in the root of the distribution.
]]

local exports = {}

local lpeg = require("lpeg")
V = lpeg.V; R = lpeg.R; S = lpeg.S; Ct = lpeg.Ct; Cg = lpeg.Cg; P = lpeg.P; C = lpeg.C

local pattern_lib = {
	spc = S(" \t\r\n")^1,

	anything = P(1)^1,
	["end"] = S("\r\n") + P(-1),
	qs = P("\"") * C( ((P(1) - S("\\\"")) + (P("\\") * P(1)) )^1) * P("\""),

	v4part = R("09") * R("09")^-2,
	ipv4 = V("v4part") * P(".") * V("v4part") * (P(".") * V("v4part"))^-2,
	hexdigi = R("09") + R("af") + R("AF"),
	v6part = V("hexdigi") * V("hexdigi")^-3,
	ipv6tail = (P("::") * V("v6part")) + (P(":") * V("v6part") * V("ipv6tail")) + (P(":") * V("v6part")) + P("::"),
	ipv6 = (P("::") * V("v6part")) + (V("v6part") * V("ipv6tail")) + P("::"),
	ip = V("ipv4") + V("ipv6"),

	hostname = V("ip") + (R("AZ") + R("az") + R("09") + S(".-_"))^1,
	notspace = (P(1) - S(" \t\r\n"))^1,
	restofline = (P(1) - S("\r\n"))^0,
	word = (R("az") + R("AZ"))^1,

	decimal = P("-")^-1 * R("09")^1 * (P(".") * R("09")^1)^-1,
	expnum = V("decimal") * S("eE") * S("+-") * R("09")^1,
	number = V("expnum") + V("decimal"),
}

local qp_parser = {
	expr = Ct( P("%{") * Cg((1 - S(":}"))^1, "pattern") *
		(P("}") + (
			P(":") * Cg((1 - S(":}"))^1, "name") *
				(P("}") + (
					P(":") * Cg((1 - S("}"))^1, "type") * P("}")
				))
		)) ),
	qpp = C((1 - P("%{"))^1) + V("expr"),
	qp = Ct(V("qpp")^1),
	[1] = "qp"
}

local function qp_compile(qp)
	local out = lpeg.match(qp_parser, qp)

	local np = nil
	for i,v in ipairs(out) do
		local this = P("")
		if type(v) == "string" then
			this = P(v)
		elseif type(v) == "table" and v.pattern ~= nil then
			this = V(v.pattern)
			if v.type == "int" or v.type == "integer" or v.type == "float" or v.type == "number" then
				this = this / (function(i) return tonumber(i) end)
			end
			if v.name ~= nil then
				this = Cg(this, v.name)
			end
		end
		if np == nil then
			np = this
		else
			np = np * this
		end
	end

	np = Ct(np)
	return np
end
exports.qp = qp_compile

local override_patterns = {}
function exports.common_patterns(tbl)
	tbl.reactor = nil
	for k,v in pairs(tbl) do
		if override_patterns[k] or pattern_lib[k] == nil then
			if type(v) == "string" then
				v = qp_compile(v)
			end
			pattern_lib[k] = v
			override_patterns[k] = true
		end
	end
end

local function new_parser(top, ...)
	local new = {}
	for k,v in pairs(pattern_lib) do
		new[k] = v
	end
	local args = {...}
	for i,rest in ipairs(args) do
		for k,v in pairs(rest) do
			new[k] = v
		end
	end
	new[1] = top
	return new
end
exports.new_parser = new_parser

local function new_filter(name, protect)
	local f = { ["name"] = name }
	if protect then
		function f:__call(...)
			local res, err = pcall(function(...) return f.run(self, ...) end, ...)
			if res and err ~= nil and self.sink ~= nil then
				local dsres, dserr = pcall(function() return self.sink(err) end)
				if dsres then
					return dserr
				else
					print("error downstream of filter: " .. f.name .. ": " .. dserr)
					return nil
				end
			elseif res then
				return err
			else
				print("error in filter: " .. f.name .. ": " .. err)
				return nil
			end
		end
	else
		function f:__call(...)
			local res = f.run(self, ...)
			if res ~= nil and self.sink ~= nil then
				return self.sink(res)
			else
				return res
			end
		end
	end
	return f
end
exports.new_filter = new_filter

local function match_fields_filter(parser, field, extras)
	local ff = new_filter("match_fields for " .. (field or 'message'))
	function ff:run(input)
		local f = input.message
		if field ~= nil then f = input.fields[field] end
		if f == nil then return input end
		local t = lpeg.match(parser, f)
		if t ~= nil then
			if extras.type ~= nil then input.type = extras.type end
			if extras.tags ~= nil then
				if input.tags == nil then input.tags = {} end
				for i,v in ipairs(extras.tags) do table.insert(input.tags, v) end
			end
			for k,v in pairs(t) do
				input.fields[k] = v
			end
		end
		return input
	end
	function ff.new()
		local tt = {}
		setmetatable(tt, ff)
		return tt
	end
	return ff
end
exports.new_mfilter = match_fields_filter

local function cleanparser(tbl)
	tbl.reactor = nil
	tbl.field = nil
	tbl.anchor = nil
	tbl.anywhere = nil
end

local grok_base = {
	grokanywhere = V("compiled") + (P(1) * V("grokanywhere")),
	grokstart = V("compiled") + ((1 - S("\r\n"))^0 * S("\r\n")^1 * V("grokstart"))
}
function exports.grok_compile(tbl)
	local pattern = tbl.pattern or tbl
	local field = tbl.field
	if type(pattern) == "string" then
		local ps = { compiled = qp_compile(pattern) }
		if tbl.anchor or tbl.anywhere == false then
			pattern = new_parser("grokstart", grok_base, ps)
		else
			pattern = new_parser("grokanywhere", grok_base, ps)
		end
	elseif type(pattern) == "table" and type(pattern[1]) == "string" then
		pattern.compiled = qp_compile(pattern[1])
		for k,v in pairs(pattern) do
			if type(v) == "string" then
				pattern[k] = qp_compile(v)
			end
		end
		if tbl.anchor or tbl.anywhere == false then
			cleanparser(pattern)
			pattern = new_parser("grokstart", grok_base, pattern)
		else
			cleanparser(pattern)
			pattern = new_parser("grokanywhere", grok_base, pattern)
		end
	elseif type(pattern) == "table" and type(pattern[1]) == "userdata" then
		pattern.compiled = pattern[1]
		if tbl.anchor or tbl.anywhere == false then
			cleanparser(pattern)
			pattern = new_parser("grokstart", grok_base, pattern)
		else
			cleanparser(pattern)
			pattern = new_parser("grokanywhere", grok_base, pattern)
		end
	else
		error("unknown pattern passed to grok")
	end
	return pattern
end
function exports.grok(tbl)
	local pattern = exports.grok_compile(tbl)
	local field = tbl.field
	return match_fields_filter(pattern, field, { type = tbl.set_type, tags = tbl.add_tags }).new()
end

function exports.link(...)
	local args = {...}
	if table.getn(args) == 1 and type(args[1]) == "table" then
		args = args[1]
	end
	local last = nil
	for i,arg in ipairs(args) do
		if last ~= nil then
			last.sink = arg
		end
		last = arg
	end
	return last
end

local ffi = require("ffi")
ffi.cdef[[
int gethostname(char *name, int namelen);
]]

local stamper = new_filter("stamper")
function stamper:run(input)
	input.source = self.source .. "://" .. self.hostname .. "/" .. self.uri
	input.type = self.type
	return input
end
function exports.stamper(tbl)
	local typ = tbl.type
	local source = tbl.scheme
	local uri = tbl.path

	local buf = ffi.new("char[?]", 1024)
	ffi.C.gethostname(buf, 1024)

	local t = { ["hostname"] = ffi.string(buf),
				["type"] = typ, ["source"] = source, ["uri"] = uri }
	setmetatable(t, stamper)
	return t
end

local multitail_head_parser = new_parser("head", {
	head = Ct( S("=-")^1 * P(">") * V("spc") * Cg(V("notspace"), "path") * V("spc") * P("<") * S("=-")^1 )
})

local multitail = new_filter("multitail")
function multitail:run(input)
	local t = lpeg.match(multitail_head_parser, input.message)
	if t ~= nil then
		self.curpath = t.path
		return nil
	else
		if self.curpath ~= nil then
			input.source = "tail://" .. self.hostname .. self.curpath
		end
		return input
	end
end
function exports.multitail()
	local buf = ffi.new("char[?]", 1024)
	ffi.C.gethostname(buf, 1024)

	local t = { ["hostname"] = ffi.string(buf), ["curpath"] = nil }
	setmetatable(t, multitail)
	return t
end

local dygrok_base = {
	dygrokrep = Ct(Cg(V("grokanywhere"),"this") * (Cg(V("dygrokrep"), "next") + V("end")))
}
local dygrok = new_filter("dygrok")
function dygrok:run(input)
	local f = input.message
	if field ~= nil then f = input.fields[field] end
	if f == nil then return input end
	local t = lpeg.match(self.parser, input.message)
	if t ~= nil then
		local function recurse(inp, res)
			local field = res.this.field
			local value = res.this.value
			inp[field] = value
			if res.next then
				return recurse(inp, res.next)
			else
				return
			end
		end
		recurse(input.fields, t)
	end
	return input
end
function exports.dygrok(tbl)
	tbl.anywhere = true
	local gr = exports.grok_compile(tbl)
	local t = { parser = new_parser("dygrokrep", dygrok_base, gr) }
	setmetatable(t, dygrok)
	return t
end

function unfold(toptbl, tbl, prefix)
	if prefix == nil then prefix = "" end
	for k,v in pairs(tbl) do
		if type(v) == "table" then
			unfold(toptbl, v, prefix .. k .. "_")
		else
			toptbl[prefix .. k] = v
		end
	end
end
local unfold_fields = new_filter("unfold_fields")
function unfold_fields:run(input)
	local f = {}
	unfold(f, input.fields)
	input.fields = f
	return input
end
function exports.unfold_fields()
	local t = {}
	setmetatable(t, unfold_fields)
	return t
end

function table_to_json(tbl)
	local json = "{"
	local first = true
	local a = function(s) json = json .. s end
	for k,v in pairs(tbl) do
		if type(k) == "string" then
			if not first then a(",") end
			first = false
			if type(v) == "string" then
				v = v:gsub("\\","\\\\"):gsub("\"", "\\\""):gsub("\n","\\n")
				a(string.format("\"%s\":\"%s\"", k, v))
			elseif type(v) == "table" then
				a(string.format("\"%s\":%s", k, table_to_json(v)))
			else
				a(string.format("\"%s\":%s", k, v))
			end
		end
	end
	return json .. "}"
end

local function quote_tags(tags)
	local tbl = {}
	local tgs = tags or {}
	for i,v in ipairs(tgs) do
		table.insert(tbl, "\"" .. v:gsub("[^a-zA-Z0-9_]","") .. "\"")
	end
	return table.concat(tbl, ",")
end

local jsonify = new_filter("jsonify")
function jsonify:run(input)
	local json = ""
	local a = function(s) json = json .. s end
	a("{")
	a(string.format("\"@timestamp\":\"%s\",", input.timestamp))
	a(string.format("\"@source\":\"%s\",", input.source))
	a(string.format("\"@type\":\"%s\",", input.type))
	a(string.format("\"@tags\":[%s],", quote_tags(input.tags)))
	a("\"@fields\":")
	a(table_to_json(input.fields))
	a(",")
	a(string.format("\"@message\":\"%s\"", input.message:gsub("\\","\\\\"):gsub("\"", "\\\""):gsub("\n","")))
	a("}")
	return json
end
function exports.jsonify()
	local t = {}
	setmetatable(t, jsonify)
	return t
end

local map = new_filter("map")
function map:run(input)
	if input.fields[self.field] then
		input.fields[self.field] = self.f(input.fields[self.field])
	elseif input[self.field] then
		input[self.field] = self.f(input[self.field])
	end
	return input
end
function exports.map(tbl)
	local f = tbl.func or (function(v) return tbl.lookup[v] or tbl.lookup[1] end)
	local t = {["f"] = f, ["field"] = tbl.field}
	setmetatable(t, map)
	return t
end

local input = new_filter("input", true)
function input:run(input)
	return { ["message"] = input, ["fields"] = {} }
end
function exports.input(tbl)
	local pipe = tbl.channel or tbl
	local t = {}
	setmetatable(t, input)
	t.handle_line = function(pipe, rtor, line)
		line = line:gsub("\n", ""):gsub("\r", "")
		if line ~= "" then t(line) end
	end
	if pipe ~= nil then pipe.on_line = t.handle_line end
	return t
end

return exports

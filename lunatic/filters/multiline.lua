--[[
loglunatic -- logstash for lunatics
Copyright (c) 2013, Alex Wilson, the University of Queensland
Distributed under a BSD license -- see the LICENSE file in the root of the distribution.
]]

local exports = {}

local f = require("lunatic/filters/common")

local multiline = f.new_filter("multiline")
function multiline:run(input)
	local ts = nil
	if self.startp ~= nil then
		ts = lpeg.match(self.startp, input.message)
	end
	local te = nil
	if self.endp ~= nil then
		te = lpeg.match(self.endp, input.message)
	end

	if (not self.started and self.startp == nil) or ts ~= nil then
		local ret = nil
		if self.started then
			if self.keep_end then
				table.insert(self.sofar, input.message)
			end
			input.message = table.concat(self.sofar, "\n")
			ret = input
		end
		self.started = true
		self.n = 0
		if self.keep_start then
			self.sofar = { input.message }
		else
			self.sofar = {}
		end
		return ret
	elseif te ~= nil or (self.maxn ~= nil and self.n >= self.maxn) then
		local ret = nil
		if self.started then
			if self.keep_end then
				table.insert(self.sofar, input.message)
			end
			input.message = table.concat(self.sofar, "\n")
			ret = input
		end
		self.started = false
		return ret
	elseif self.started then
		table.insert(self.sofar, input.message)
		self.n = self.n + 1
		return nil
	end
end
function exports.multiline(tbl)
	local start_pattern = nil
	local end_pattern = nil
	if tbl.start_pattern ~= nil then
		start_pattern = f.grok_compile(tbl.start_pattern)
	end
	if tbl.end_pattern ~= nil then
		end_pattern = f.grok_compile(tbl.end_pattern)
	end
	local t = { startp = start_pattern, endp = end_pattern, sofar = {}, started = false, keep_start = tbl.keep_start or tbl.keep, keep_end = tbl.keep_end or tbl.keep, maxn = tbl.max_lines, n = 0 }
	setmetatable(t, multiline)
	return t
end

return exports.multiline

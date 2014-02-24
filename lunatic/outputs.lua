--[[
loglunatic -- logstash for lunatics
Copyright (c) 2013, Alex Wilson, the University of Queensland
Distributed under a BSD license -- see the LICENSE file in the root of the distribution.
]]

local exports = {}

exports.elasticsearch = require('lunatic/outputs/elasticsearch')
exports.carbon = require('lunatic/outputs/carbon')

local inspect = require('lunatic/outputs/inspect')
local f = require('lunatic/filters/common')
local stdout = f.new_filter("stdout")
function stdout:run(input)
	io.write(inspect(input) .. "\n")
	return input
end
function exports.stdout(tbl)
	local t = {}
	setmetatable(t, stdout)
	return t
end

return exports

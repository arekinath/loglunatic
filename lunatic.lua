--[[
loglunatic -- logstash for lunatics
Copyright (c) 2013, Alex Wilson, the University of Queensland
Distributed under a BSD license -- see the LICENSE file in the root of the distribution.
]]

local exports = {}

local function merge(tbout, tbin) for k,v in pairs(tbin) do tbout[k] = v end end

exports.reactor = require('lunatic/reactor')
exports.r = exports.reactor
merge(exports, exports.r)

exports.inputs = require('lunatic/inputs')
exports.i = exports.inputs

exports.filters = require('lunatic/filters')
exports.f = exports.filters
merge(exports, exports.f)

exports.outputs = require('lunatic/outputs')
exports.o = exports.outputs

return exports

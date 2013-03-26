#!/usr/bin/env luajit

--[[
loglunatic -- logstash for lunatics
Copyright (c) 2013, Alex Wilson, the University of Queensland
Distributed under a BSD license -- see the LICENSE file in the root of the distribution.
]]

local fname = arg[1]
if fname == nil then
	io.write("Usage: ./loglunatic.lua <conffile>\n")
	return false
end
local f, err = loadfile(fname)
if f == nil then
	io.write("Could not process config file " .. fname .. "\n")
	io.write(err .. "\n")
	return false
end

local l = require('lunatic')
local lpeg = require('lpeg')

local rtor = l.Reactor.new()

local env = {}
env.string = string
env.table = table
env.math = math

env.os = {}
env.os.time = os.time
env.os.date = os.date

for k,v in pairs(lpeg) do
	env[k] = v
end

env.inputs = {}
for k,v in pairs(l.inputs) do
	env.inputs[k] = function(tbl)
		tbl.reactor = rtor
		local chan = v(tbl)
		rtor:add(chan)
		return l.filters.input{ channel = chan, reactor = rtor }
	end
end

local reactorwrap = function(orig)
	return function(tbl)
		tbl.reactor = rtor
		return orig(tbl)
	end
end

local function wraptbl(dest, src)
	for k,v in pairs(src) do
		if type(v) == "table" then
			dest[k] = {}
			wraptbl(dest[k], v)
		else
			dest[k] = reactorwrap(v)
		end
	end
end

wraptbl(env, l.filters)

env.outputs = {}
wraptbl(env.outputs, l.outputs)

env.link = l.link

f = setfenv(f, env)
f()

rtor:run()

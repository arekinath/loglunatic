#!/usr/bin/env luajit

--[[
loglunatic -- logstash for lunatics
Copyright (c) 2013, Alex Wilson, the University of Queensland
Distributed under a BSD license -- see the LICENSE file in the root of the distribution.
]]

local function usage()
	io.write("Usage: ./loglunatic.lua [-d|--daemon] [-p|--pidfile <file>] [-l|--logfile <file>] <config>\n")
	os.exit(1)
end

local fname = nil
local lastarg = nil
local logfile = "lunatic.log"
local pidfile = "lunatic.pid"
local foreground = true

for i,v in ipairs(arg) do
	if v == "--daemon" or v == "-d" then
		foreground = false
	elseif lastarg == "--logfile" or lastarg == "-l" then
		logfile = v
	elseif lastarg == "--pidfile" or lastarg == "-p" then
		pidfile = v
	else
		fname = v
	end
	lastarg = v
end
if fname == nil then
	usage()
end
local f, err = loadfile(fname)
if f == nil then
	io.write("Could not process config file " .. fname .. "\n")
	io.write(err .. "\n")
	os.exit(1)
end

local lpeg = assert(require('lpeg'))
local ffi = assert(require('ffi'))
local bit = assert(require('bit'))
local l = assert(require('lunatic'))

ffi.cdef[[
int fork(void);
int setsid(void);
int close(int);
typedef void (*sig_t)(int);
sig_t signal(int sig, sig_t func);
int dup2(int from, int to);
int open(const char *path, int oflag, ...);
int fsync(int);
]]

local O_WRONLY = 0x01
local O_CREAT = 0x0200
if ffi.os == "Linux" then
	O_CREAT = 0x40
end
if ffi.os == "POSIX" then
	O_CREAT = 0x100
end

local function daemonize()
	assert(io.open(logfile, "w"))
	local fd = ffi.C.open(logfile, O_WRONLY)
	assert(fd >= 0)

	local r = ffi.C.fork()
	if r < 0 then
		print("fork failed: " .. ffi.errno)
		os.exit(1)
	elseif r > 0 then
		local pidfile = io.open(pidfile, "w")
		pidfile:write(r .. "\n")
		pidfile:close()
		os.exit(0)
	end

	ffi.C.setsid()

	ffi.C.close(0)
	ffi.C.close(1)
	ffi.C.close(2)

	assert(ffi.C.dup2(fd, 1) >= 0)
	assert(ffi.C.dup2(fd, 2) >= 0)

	ffi.C.fsync(fd)
	ffi.C.close(fd)

	print("daemonized ok, ready to go")
	ffi.C.fsync(1)
end

if not foreground then
	daemonize()
end

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
		if tbl.restart then
			chan.old_close = chan.on_close
			chan.on_close = function(ch, rt)
				ch.old_close()
				print("loglunatic: restarting closed input '" .. k .. "'")
				local newchan = v(tbl)
				rt:add(newchan)
				newchan.old_close = newchan.on_close
				newchan.on_close = ch.on_close
				local newinp = l.filters.input{ channel = newchan, reactor = rt }
				newchan.inp = newinp
				newinp.sink = chan.inp.sink
			end
		end
		chan.inp = l.filters.input{ channel = chan, reactor = rtor }
		return chan.inp
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

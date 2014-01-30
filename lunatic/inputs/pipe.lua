--[[
loglunatic -- logstash for lunatics
Copyright (c) 2013, Alex Wilson, the University of Queensland
Distributed under a BSD license -- see the LICENSE file in the root of the distribution.
]]

local ffi = require("ffi")
local bit = require("bit")
local r = require("lunatic/reactor")

ffi.cdef[[
int pipe(int fildes[2]);
int write(int fd, const void *buf, int bytes);
int fork(void);
int execvp(const char *file, char *const argv[]);
int dup2(int oldfd, int newfd);
int close(int fd);
int waitpid(int pid, int *stat_loc, int options);
int kill(int pid, int sig);
int usleep(uint32_t usecs);
]]

local function write(str)
	return ffi.C.write(1, str, #str)
end

local function pipe(tbl)
	local command = tbl.command or tbl
	local fds = ffi.new("int[?]", 2)

	local ret = ffi.C.pipe(fds)
	if ret < 0 then error("pipe failed") end

	local pid = ffi.C.fork()
	if pid == -1 then
		error("fork failed")
	elseif pid == 0 then
		ffi.C.dup2(fds[1], 1)
		ffi.C.close(fds[0])
		ffi.C.close(fds[1])

		local args = ffi.new("char *[?]", 4)
		args[0] = ffi.new("char[?]", 5, "sh")
		args[1] = ffi.new("char[?]", 3, "-c")
		args[2] = ffi.new("char[?]", #command + 1, command)
		args[3] = nil
		ffi.C.execvp("sh", args)
	else
		local fd = fds[0]
		ffi.C.close(fds[1])
		local chan = r.Channel.new(fd)
		chan.pid = pid
		chan.command = command
		chan.on_close = function(ch, rtor)
			io.write("input pipe: " .. ch.command .. "/"..ch.fd.."/"..ch.pid..": closing...\n")
			ffi.C.close(ch.fd)
                       ffi.C.kill(ch.pid, 15)
                       ffi.C.usleep(10000)
                       ffi.C.kill(ch.pid, 9)
			local sl = ffi.new("int[?]", 1)
			ffi.C.waitpid(ch.pid, sl, 0)
		end
		return chan
	end
end

return pipe

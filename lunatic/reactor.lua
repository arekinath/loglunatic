--[[
loglunatic -- logstash for lunatics
Copyright (c) 2013, Alex Wilson, the University of Queensland
Distributed under a BSD license -- see the LICENSE file in the root of the distribution.
]]

local exports = {}

local ffi = require("ffi")
local bit = require("bit")

local POLLIN = 0x01
local POLLOUT = 0x04

ffi.cdef[[
struct pollfd {
	int fd;
	short events;
	short revents;
};
int read(int fd, void *buf, int size);
int write(int fd, const void *buf, int bytes);
int poll(struct pollfd *fds, unsigned long nfds, int timeout);
int write(int fd, const void *buf, int bytes);
int close(int fd);

void *malloc(int size);
void free(void *ptr);

struct sockaddr {
	uint8_t sa_len;
	uint8_t sa_family;
	char sa_data[14];
};

struct sockaddr_in {
	uint8_t sin_len;
	uint8_t sin_family;
	uint16_t sin_port;
	char sin_pad[32];
};

struct timeval {
	long tv_sec;
	long tv_usec;
};

struct timezone {
	int tz_minuteswest;
	int tz_dsttime;
};

int getaddrinfo(const char *hostname, const char *servname, const struct addrinfo *hints, struct addrinfo **res);
void freeaddrinfo(struct addrinfo *ai);
int socket(int family, int type, int protocol);
int connect(int sock, struct sockaddr *name, int namelen);

int gettimeofday(struct timeval *tp, struct timezone *tzp);

char *strerror(int errno);
]]

if ffi.os == "OSX" then
	ffi.cdef[[
	struct addrinfo {
		int ai_flags;
		int ai_family;
		int ai_socktype;
		int ai_protocol;
		int ai_addrlen;
		char *ai_canonname;
		struct sockaddr *ai_addr;
		struct addrinfo *ai_next;
	};
	]]
else
	ffi.cdef[[
	struct addrinfo {
		int ai_flags;
		int ai_family;
		int ai_socktype;
		int ai_protocol;
		int ai_addrlen;
		struct sockaddr *ai_addr;
		char *ai_canonname;
		struct addrinfo *ai_next;
	};
	]]
end

local function write(str)
	return ffi.C.write(1, str, #str)
end

local Reactor = {}
Reactor.__index = Reactor

function Reactor.new()
	local reac = {}
	setmetatable(reac, Reactor)
	reac.channels = {}
	reac.timers = {}
	reac.timer_id = 0
	return reac
end

function Reactor:add(chan)
	table.insert(self.channels, chan)
end

function Reactor:remove(rchan)
	local ri = {}
	for i,chan in ipairs(self.channels) do
		if chan.fd == rchan.fd then
			table.insert(ri, i)
		end
	end
	for x,i in ipairs(ri) do
		table.remove(self.channels, i)
	end
	rchan:cleanup()
end

function Reactor:add_timer(timeout, handler)
	local id = self.timer_id
	self.timer_id = self.timer_id + 1

	local tv = ffi.new("struct timeval")
	assert(ffi.C.gettimeofday(tv, nil) == 0)
	local t = {
		["time"] = timeout + tonumber(tv.tv_sec) + tonumber(tv.tv_usec) / 1.0e6,
		["handler"] = handler
	}
	self.timers[id] = t

	return id
end

function Reactor:remove_timer(id)
	self.timers[id] = nil
end

function Reactor:run()
	local nfds = table.getn(self.channels)
	local nowtv = ffi.new("struct timeval")
	local pollfds = ffi.new("struct pollfd[?]", nfds)
	while nfds > 0 do
		for i,chan in ipairs(self.channels) do
			chan:fill_pollfd(pollfds[i-1])
		end

		local ret = ffi.C.poll(pollfds, nfds, 500)
		if ret < 0 then
			error("poll: " .. ffi.string(ffi.C.strerror(ffi.errno())))
		end

		if ret > 0 then
			for i,chan in ipairs(self.channels) do
				if bit.band(pollfds[i-1].revents, POLLIN) == POLLIN then
					chan:read_data(self)
				end
				if bit.band(pollfds[i-1].revents, POLLOUT) == POLLOUT then
					chan:write_data(self)
				end
			end
		end

		assert(ffi.C.gettimeofday(nowtv, nil) == 0)
		now = tonumber(nowtv.tv_sec) + tonumber(nowtv.tv_usec) / 1.0e6
		for id,timer in pairs(self.timers) do
			if timer.time < now then
				local h = timer.handler
				self.timers[id] = nil
				h:on_timeout(self)
			end
		end

		local new_nfds = table.getn(self.channels)
		if new_nfds ~= nfds then
			nfds = new_nfds
			if nfds > 0 then
				pollfds = ffi.new("struct pollfd[?]", nfds)
			end
		end
	end
end

exports.Reactor = Reactor

ffi.cdef[[
int uname(char *);
int fcntl(int, int, ...);
]]

local sysnamebuf = ffi.new("char[?]", 16384)
assert(ffi.C.uname(sysnamebuf) == 0)
local sysname = ffi.string(sysnamebuf)

local F_GETFL = 0x03
local F_SETFL = 0x04
local O_NONBLOCK = 0x04
local EAGAIN = 35
if ffi.os == "Linux" or string.match(sysname, "Linux") then
	O_NONBLOCK = 0x800
	EAGAIN = 11
end
if ffi.os == "Solaris" or string.match(sysname, "SunOS") then
	O_NONBLOCK = 0x80
	EAGAIN = 11
end

local Channel = {}
Channel.__index = Channel
Channel.read_buf = 32768

function Channel.new(fd)
	local chan = {
		["fd"] = fd,
		buffer = ffi.new("char[?]", Channel.read_buf),
		bufused = 0,
		wrdata = {},
	}

	local flags = ffi.C.fcntl(fd, F_GETFL, 0)
	if flags == -1 then
		return nil
	end
	flags = bit.bor(flags, O_NONBLOCK)
	if ffi.C.fcntl(fd, F_SETFL, ffi.new("int", flags)) == -1 then
		return nil
	end

	setmetatable(chan, Channel)
	chan.on_line = function (chan, rtor, line)
		io.write(string.format("reactor: fd %d got line: '%s'\n", chan.fd, line))
	end
	chan.on_close = function (chan, rtor)
		io.write("reactor: fd " .. chan.fd .. " reached eof or error\n")
		ffi.C.close(chan.fd)
	end
	return chan
end

function Channel:cleanup()
	return
end

function Channel:fill_pollfd(s)
	s.fd = self.fd
	if self.wrdata.first ~= nil or self.on_writeable ~= nil then
		s.events = bit.bor(POLLOUT, POLLIN)
	else
		s.events = POLLIN
	end
	s.revents = 0
	return s
end

function Channel:write(str)
	local len = #str

	local buf = {["data"] = ffi.new("char[?]", len)}
	buf.size = len
	buf.pos = 0
	ffi.copy(buf.data, str, len)

	local lastbuf = self.wrdata.last
	if lastbuf ~= nil then
		lastbuf.next = buf
		self.wrdata.last = buf
	else
		self.wrdata.last = buf
		self.wrdata.first = buf
	end
end

function Channel:write_data(rtor)
	local buf = self.wrdata.first
	if buf == nil then
		if self.on_writeable ~= nil then
			self:on_writeable(rtor)
		end
	else
		local ret = ffi.C.write(self.fd, buf.data + buf.pos, buf.size - buf.pos)
		if ret < 0 then
			if ffi.errno() == EAGAIN then
				return
			end
			io.write("reactor: write error on fd " .. self.fd .. "\n")
			self:on_close(rtor)
			rtor:remove(self)
		else
			buf.pos = buf.pos + ret
			if buf.pos >= buf.size then
				self.wrdata.first = buf.next
				if buf.next == nil then
					self.wrdata.last = nil
				end
			end
		end
	end
end

function Channel:read_data(rtor)
	local start = self.bufused
	local limit = Channel.read_buf - self.bufused

	local ret = ffi.C.read(self.fd, self.buffer + start, limit)
	if ret == 0 then
		io.write("reactor: eof on fd " .. self.fd .. "\n")
		self:on_close(rtor)
		rtor:remove(self)
		return
	elseif ret < 0 then
		if ffi.errno() == EAGAIN then
			return
		else
			io.write("reactor: read error on fd " .. self.fd .. ": " .. ffi.string(ffi.C.strerror(ffi.errno())) .. "\n")
			self:on_close(rtor)
			rtor:remove(self)
		end
	end
	self.bufused = self.bufused + ret
	local i = start
	while i < self.bufused do
		if self.buffer[i] == 10 then
			local line = ffi.string(self.buffer, i)
			self:on_line(rtor, line)

			for j = i+1, self.bufused, 1 do
				self.buffer[j - (i+1)] = self.buffer[j]
			end
			self.bufused = self.bufused - (i + 1)
			i = 0
			start = 0
		end
		i = i + 1
	end
end

exports.Channel = Channel

local SOCK_STREAM = 1
local SOCK_DGRAM = 2

local AI_NUMERICHOST = 4
local AI_NUMERICSERV = 16
if ffi.os == "Linux" then
	AI_NUMERICSERV = 0x0400
end
if ffi.os == "OSX" then
	AI_NUMERICSERV = 0x1000
end

local TcpChannel = {}
TcpChannel.__index = Channel
function TcpChannel.new(host, port)
	local hints = ffi.new("struct addrinfo[?]", 1)
	hints[0].ai_flags = AI_NUMERICSERV
	hints[0].ai_socktype = SOCK_STREAM

	local ai = ffi.new("struct addrinfo*[?]", 1)

	local ret = ffi.C.getaddrinfo(host, tostring(port), hints, ai)
	if ret ~= 0 then error("getaddrinfo: " .. ffi.string(ffi.C.strerror(ffi.errno()))) end

	local firstai = ai[0]
	local lasterrno = 0
	if firstai == nil then error("failed looking up " .. host) end
	ai = ai[0]

	while ai ~= nil do
		local s = ffi.C.socket(ai.ai_family, ai.ai_socktype, ai.ai_protocol)
		if s <= 0 then error("socket: " .. ffi.string(ffi.C.strerror(ffi.errno()))) end

		local ret = ffi.C.connect(s, ai.ai_addr, ai.ai_addrlen)
		if ret == 0 then
			ffi.C.freeaddrinfo(firstai)
			local chan = Channel.new(s)
			return chan
		end
		lasterrno = ffi.errno()

		ffi.C.close(s)
		ai = ai.ai_next
	end

	ffi.C.freeaddrinfo(firstai)
	error("connect: " .. ffi.string(ffi.C.strerror(lasterrno)))
end

exports.TcpChannel = TcpChannel

return exports

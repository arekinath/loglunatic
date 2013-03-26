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
int poll(struct pollfd *fds, unsigned long nfds, int timeout);
int write(int fd, const void *buf, int bytes);
int close(int fd);

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

int getaddrinfo(const char *hostname, const char *servname, const struct addrinfo *hints, struct addrinfo **res);
void freeaddrinfo(struct addrinfo *ai);
int socket(int family, int type, int protocol);
int connect(int sock, struct sockaddr *name, int namelen);
]]

local function write(str)
	return ffi.C.write(1, str, #str)
end

local Reactor = {}
Reactor.__index = Reactor

function Reactor.new()
	local reac = {}
	setmetatable(reac, Reactor)
	reac.channels = {}
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
end

function Reactor:run()
	local nfds = table.getn(self.channels)
	local pollfds = ffi.new("struct pollfd[?]", nfds)
	while nfds > 0 do
		for i,chan in ipairs(self.channels) do
			chan:fill_pollfd(pollfds[i-1])
		end

		local ret = ffi.C.poll(pollfds, nfds, -1)
		if ret < 0 then error("poll failed") end
		for i,chan in ipairs(self.channels) do
			if chan.on_writeable ~= nil then
				if bit.band(pollfds[i-1].revents, POLLOUT) == POLLOUT then
					chan:on_writeable(self)
				end
			else
				if bit.band(pollfds[i-1].revents, POLLIN) == POLLIN then
					chan:read_data(self)
				end
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

local Channel = {}
Channel.__index = Channel

function Channel.new(fd)
	local chan = { ["fd"] = fd, buffer = ffi.new("char[?]", 8192), bufused = 0 }
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

function Channel:fill_pollfd(s)
	s.fd = self.fd
	if self.on_writeable ~= nil then
		s.events = POLLOUT
	else
		s.events = POLLIN
	end
	s.revents = 0
	return s
end

function Channel:read_data(rtor)
	local start = self.bufused
	local limit = 8192 - self.bufused

	local ret = ffi.C.read(self.fd, self.buffer + start, limit)
	if ret == 0 then
		io.write("reactor: eof on fd " .. self.fd .. "\n")
		self:on_close(rtor)
		rtor:remove(self)
		return
	elseif ret < 0 then
		io.write("reactor: read error on fd " .. self.fd .. "\n")
		self:on_close(rtor)
		rtor:remove(self)
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

local TcpChannel = {}
TcpChannel.__index = Channel
function TcpChannel.new(host, port)
	local hints = ffi.new("struct addrinfo[?]", 1)
	hints[0].ai_flags = AI_NUMERICSERV
	hints[0].ai_socktype = SOCK_STREAM

	local ai = ffi.new("struct addrinfo*[?]", 1)

	local ret = ffi.C.getaddrinfo(host, tostring(port), hints, ai)
	if ret ~= 0 then error("getaddrinfo") end

	local firstai = ai[0]
	ai = ai[0]

	while ai ~= nil do
		local s = ffi.C.socket(ai.ai_family, ai.ai_socktype, ai.ai_protocol)
		if s <= 0 then error("socket") end

		local ret = ffi.C.connect(s, ai.ai_addr, ai.ai_addrlen)
		if ret == 0 then
			ffi.C.freeaddrinfo(firstai)
			local chan = Channel.new(s)
			return chan
		end

		ffi.C.close(s)
		ai = ai.ai_next
	end

	ffi.C.freeaddrinfo(firstai)
	error("tcp_connect failed")
end

exports.TcpChannel = TcpChannel

return exports

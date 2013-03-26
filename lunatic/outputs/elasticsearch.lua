--[[
loglunatic -- logstash for lunatics
Copyright (c) 2013, Alex Wilson, the University of Queensland
Distributed under a BSD license -- see the LICENSE file in the root of the distribution.
]]

local exports = {}

local f = require("lunatic/filters/common")
local r = require("lunatic/reactor")

local ffi = require("ffi")
local bit = require("bit")

ffi.cdef[[
int write(int fd, const void *buf, int bytes);
int close(int fd);
]]

local function write(fd, string)
	ffi.C.write(fd, string, #string)
end

local http = f.new_filter("elasticsearch.http")
function http:run(tbl)
	local input = tbl
	if type(input) == "table" then input = self.jsonify(input) end

	local now = os.time()
	local since = os.difftime(now, self.last_time)

	table.insert(self.buffer, input)
	local n = table.getn(self.buffer)
	if since > self.threshold.time or n > self.threshold.reqs then
		self.last_time = now

		local bucket = os.date(self.bucket, os.time(os.date("!*t", now)))
		local req = ""
		local a = function(s) req = req .. s end

		for i,v in ipairs(self.buffer) do
			a("{\"index\":{")
			a(string.format("\"_index\":\"%s\",", bucket))
			a(string.format("\"_type\":\"log\""))
			a("}}\n")

			a(v)
			a("\n")
		end
		local reqlen = #req

		local chan = r.TcpChannel.new(self.host, self.port)

		chan.on_writeable = function(_chan, rtor)
			if not chan.sent_header then
				write(chan.fd, "POST /_bulk HTTP/1.1\r\n")
				write(chan.fd, string.format("Host: %s\r\n", self.host))
				write(chan.fd, "Connection: close\r\n")
				write(chan.fd, string.format("Content-Length: %d\r\n", reqlen))
				write(chan.fd, "\r\n")
				chan.sent_header = true
			elseif not chan.sent_req then
				write(chan.fd, req)
				chan.sent_req = true
				chan.on_writeable = nil
			end
		end
		chan.on_close = function(_chan, rtor)
			if not chan.got_first_line then
				io.write("elasticsearch: WARNING: closed before first line\n")
			end
			ffi.C.close(chan.fd)
			rtor:remove(chan)
		end
		chan.on_line = function(_chan, rtor, line)
			line = line:gsub("\r",""):gsub("\n","")
			if chan.got_first_line then
				return
			end
			if line ~= "HTTP/1.1 200 OK" then
				io.write("elasticsearch: WARNING: instead of OK I got '" .. line .. "'\n")
			end
			chan.got_first_line = true
			chan:on_close(rtor)
		end

		self.rtor:add(chan)

		self.buffer = {}
	end

	return tbl
end
function exports.http(tbl)
	local rtor = tbl.reactor
	local host = tbl.host or "localhost"
	local port = tbl.port or 9200
	local bucket = tbl.bucket or "logstash-%Y.%m.%d"
	local threshold = tbl.threshold or { time = 7, reqs = 100 }

	local t = { ["rtor"] = rtor, ["host"] = host, ["port"] = port, ["bucket"] = bucket, ["threshold"] = threshold, buffer = {}, last_time = os.time(), jsonify = f.jsonify() }
	setmetatable(t, http)
	return t
end

return exports

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
int close(int fd);
]]

local http = f.new_filter("elasticsearch.http")
function http:run(tbl)
	local input = tbl
	if type(input) == "table" then input = self.jsonify(input) end

	table.insert(self.buffer, input)
	if not self:check_send() then
		self.timer_ref = rtor:add_timer(self.threshold.time+1, self)
	end

	return tbl
end
function http:on_timeout(rtor)
	self.timer_ref = nil
	self:check_send()
end
function http:check_send()
	local now = os.time()
	local since = os.difftime(now, self.last_time)
	local n = table.getn(self.buffer)
	if (n > 0 and since > self.threshold.time) or n > self.threshold.reqs then
		self.last_time = now

		-- if this is the first after a long gap we don't want to send
		-- right away, return false so a timer is set up
		if n == 1 and n < self.threshold.reqs then
			return false
		end

		if self.timer_ref ~= nil then
			self.rtor:remove_timer(self.timer_ref)
			self.timer_ref = nil
		end

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
				chan:write("POST /_bulk HTTP/1.1\r\n")
				chan:write(string.format("Host: %s\r\n", self.host))
				chan:write("Connection: close\r\n")
				chan:write(string.format("Content-Length: %d\r\n", reqlen))
				chan:write("\r\n")
				chan.sent_header = true
			elseif not chan.sent_req then
				chan:write(req)
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
		return true
	end
	return false
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

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

local plain = f.new_filter("carbon.plain")
function plain:run(tbl)
	local input = tbl

	local now = input._timestamp or os.time()
	local metrics = self.mapping(input)

	for k,v in pairs(metrics) do
		local line = string.format("%s %s %d", k, v, now)
		local chan = r.TcpChannel.new(self.host, self.port)
		chan.on_writeable = function(_chan, rtor)
			if not chan.sent_header then
				chan:write(line)
				chan.sent_header = true
			else
				chan:on_close(rtor)
			end
		end
		chan.on_close = function(_chan, rtor)
			if not chan.got_first_line then
				io.write("carbon: WARNING: closed before data was sent\n")
			end
			ffi.C.close(chan.fd)
			rtor:remove(chan)
		end
		chan.on_line = function(_chan, rtor, line)
			-- do nothing
		end

		self.rtor:add(chan)
	end

	return tbl
end
function exports.plain(tbl)
	local rtor = tbl.reactor
	local host = tbl.host or "localhost"
	local port = tbl.port or 2003
	local mapping = nil
	if type(tbl.mapping) == "table" then
		mapping = function(input)
			local out = {}
			for k,v in pairs(tbl.mapping) do
				if input.fields[k] then
					out[v] = input.fields[k]
				elseif input[k] then
					out[v] = input[k]
				end
			end
			return out
		end
	else
		mapping = tbl.mapping
	end

	local t = { ["rtor"] = rtor, ["host"] = host, ["port"] = port, ["mapping"] = mapping }
	setmetatable(t, http)
	return t
end

return exports

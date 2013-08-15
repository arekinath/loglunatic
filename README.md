Loglunatic is a tool for parsing useful information out of log files and shipping them off to some sort of storage/analysis suite (such as ElasticSearch). It's intended to work a lot like [Logstash](http://www.logstash.net/), but with a much lighter footprint, and thus a more focussed (also read: limited) feature set. I wanted a tool that would do the very basics of what logstash does, but in 1/10th the CPU and memory footprint, and without Java.

Loglunatic is written in Lua, and depends on LuaJIT (for the FFI and bitops library) and LPEG. So far it's tested working on Linux and OpenBSD, but should work on other POSIX operating systems too.

## Dependencies
 * [LuaJIT](http://www.luajit.org/) (v2 or later)
 * [LPEG](http://www.inf.puc-rio.br/~roberto/lpeg/)
 * A [POSIX operating system](http://www.openbsd.org/)

## License
BSD.

## Documentation

A brief overview/tutorial appears below. The [Wiki](https://github.com/arekinath/loglunatic/wiki) also contains more detailed references for the supported functions / config elements.

## Using Loglunatic

Loglunatic, much like logstash, is based around config files that describe what it should do. In the config file, you build a pipeline using the link{} function. Loglunatic's config files are actually Lua code, which gives you tremendous flexibility.

A typical config file consists of a call to the `link{}` function to create a pipeline. First, we list the input to the pipeline, then any filters to apply, and finally an output.

In this example (which parses a basic nginx access log) we take a command pipe as input, apply some filters, and produce output to stdout:

    link {
        inputs.pipe { command = "tail -f /var/log/nginx/access.log" },

        stamper { type = "nginx_access", scheme = "tail", path = "var/log/nginx/access.log" },
        grok {
            pattern = {
                "%{hostname:client} %{notspace} %{notspace:user} [%{notsq:timestamp}] %{qs:request} %{number:response_code:int} %{number:bytes:int} %{qs:referer} %{qs:user_agent}",
                notsq = (1 - P("]"))^1
            },
            anywhere = false
        },
        grok {
            pattern = "%{word:request_method} %{notspace:request_path} HTTP/%{number:http_version:float}",
            field = "request", anywhere = false
        },
        date { type = "http" },
        unfold_fields {},

        outputs.stdout {}
    }

Really the core of Loglunatic is the `grok{}` filter, which is used for parsing the log message text and splitting it up into semantic fields. `grok` accepts a pattern syntax very similar to the Logstash `grok`, but notably, it can also be used to build any abritrary LPEG parser. In the example above you can see both a `grok` based only on a logstash-style pattern, and one that combines this with an LPEG pattern.

The `(1 - P("]"))^1` pattern indicates "match at least one character that is not `]`". You can find out more about LPEG patterns in [the LPEG documentation](http://www.inf.puc-rio.br/~roberto/lpeg/). They are more general than regular expressions, and typically can be executed faster.

To run the config file above, we could save it in "example.conf" and apply:

    $ ./loglunatic.lua example.conf
    {
      fields = {
        bytes = 3650,
        client = "127.0.0.1",
        referer = "-",
        request = "GET /favicon.ico HTTP/1.1",
        response_code = 404,
        timestamp = "26/Mar/2013:13:13:32 +1000",
        user_agent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.95 Safari/537.11"
      },
      message = '...',
      source = "tail://hostname/var/log/nginx/access.log",
      timestamp = "2013-03-26T03:13:32.000Z",
      type = "nginx_access"
    }

As you can see, the `stdout{}` output "pretty-prints" the internal representation of the log event as a Lua table. We could also replace `stdout` with the `elasticsearch.http` output module, to submit this as JSON to ElasticSearch. The JSON it generates is compatible with logstash, and even stored in the same day-by-day indexes, so you can keep using the same tools (such as Kibana) that you would use with logstash.

## As a daemon

Loglunatic already comes equipped to become a daemon without any other tools necessary. This simplifies your init scripts and monitoring. Just use the `-d` switch (run `loglunatic.lua` without any arguments to see the usage message).

## Performance/footprint

Running Logstash on one particular server I take care of at work was using ~10-20% CPU and 100+MB RSS. To do the same job with Loglunatic, I see it using ~1% CPU and less than 10MB RSS. I consider this to be good enough for what I need, but if someone else wants to actually benchmark and compare things, that would be cool.

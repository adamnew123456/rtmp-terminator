# What Is this?

This is a basic streaming server which you can use to terminate RTMP streams
from OBS. It accepts input from one stream and then rebroadcasts it to any
number of connected clients over HTTP.

# How Do I Run It?

All you need is .NET Core 3.1 and you can run it using `dotnet run`. It accepts
both an HTTP port as well as an optional RTMP port:

```sh
$ dotnet run -- 2020      # Streams RTMP 1935 => HTTP 2020
$ dotnet run -- 2020 1940 # Streams RTMP 1940 => HTTP 2020 
```

In OBS all you need to do is use a Custom service and provide the server URL
`rmtp://localhost` (if you're using 1935 as the RTMP port). The stream key
can be any value you want, since this server only supports input from one
stream at a time.

You can connect to the stream using VLC or another media player that knows how
to play FLV:

```sh
$ vlc http://localhost:2020
```

Disconnecting within OBS will stop the server automatically.

# Limitations

Currently this has only been tested with OBS. It takes several shortcuts on the
RTMP spec which means that other clients like FFMPEG may not work.

Also, since this only supports FLV on output you won't be able to play it in
web browsers. At some point I would like to support another container format
like MKV which can be played in both FF and Chrome.

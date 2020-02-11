open System
open System.Collections.Concurrent
open System.Net
open System.Net.Sockets
open System.Threading

let usage = (
    "rtmp-terminator - Serves RTMP streams as FLV over HTTP\n" +
    "\n" +
    "Usage: rtmp-terminator HTTP-PORT [RTMP-PORT]\n" +
    "\n" +
    "Arguments:\n" +
    "  HTTP-PORT The port to listen for incoming HTTP connections on.\n" +
    "\n" +
    "  RTMP-PORT The port to listen for an incoming RTMP connection on.\n" +
    "            Optional, 1935 by default.\n"
)

[<EntryPoint>]
let main argv =
    if Array.contains "-h" argv || Array.contains "--help" argv then
        printf "%s" usage
        0
    else
        let (http_port, rtmp_port) =
            try
                match argv with
                | [| http_port |] ->
                    (int http_port, 1935)

                | [| http_port; rtmp_port |] ->
                    (int http_port, int rtmp_port)

                | _ ->
                    eprintfn "Unexpected arguments"
                    eprintf "%s" usage
                    Environment.Exit(1)
                    failwith "" // Dead, needed for typecheck
            with
            | :? System.FormatException ->
                eprintfn "Could not parse one of the provided port values"
                eprintf "%s" usage
                Environment.Exit(1)
                failwith "" // Dead, needed for typecheck

        eprintfn "Listening for RTMP on port %d, HTTP on port %d" rtmp_port http_port
        let rtmp_endpoint = IPEndPoint(IPAddress.Any, rtmp_port)
        let http_endpoint = IPEndPoint(IPAddress.Any, http_port)

        use acceptor_mbox = new BlockingCollection<HTTPAcceptor.Events>()
        use repeater_mbox = new BlockingCollection<HTTPRepeater.Events>()

        use rtmp_server = new Socket(SocketType.Stream, ProtocolType.Tcp)
        rtmp_server.Bind(rtmp_endpoint)
        rtmp_server.Listen(1)

        use http_server = new Socket(SocketType.Stream, ProtocolType.Tcp)
        http_server.Bind(http_endpoint)
        http_server.Listen(10)

        let rtmp_thread = Thread(fun () ->
            RtmpReceiver.runner rtmp_server acceptor_mbox repeater_mbox)

        let acceptor_thread = Thread(fun () ->
            HTTPAcceptor.runner http_server acceptor_mbox repeater_mbox)

        let repeater_thread = Thread(fun () ->
            HTTPRepeater.runner repeater_mbox)

        rtmp_thread.Start()
        acceptor_thread.Start()
        repeater_thread.Start()

        rtmp_thread.Join()
        acceptor_thread.Join()
        repeater_thread.Join()
        0

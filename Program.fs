open System
open System.Collections.Concurrent
open System.Net
open System.Net.Sockets
open System.Threading

[<EntryPoint>]
let main _ =
    let rtmp_endpoint = IPEndPoint(IPAddress.Any, 1935)
    let http_endpoint = IPEndPoint(IPAddress.Any, 2020)

    use acceptor_mbox = new BlockingCollection<HTTPAcceptor.Events>()
    use repeater_mbox = new BlockingCollection<HTTPRepeater.Events>()

    use rtmp_server = new Socket(SocketType.Stream, ProtocolType.Tcp)
    rtmp_server.Bind(rtmp_endpoint)
    rtmp_server.Listen(1)

    use http_server = new Socket(SocketType.Stream, ProtocolType.Tcp)
    http_server.Bind(http_endpoint)
    http_server.Listen(10)

    let rtmp_thread = Thread(fun () ->
        ChunkProtocol.runner rtmp_server acceptor_mbox repeater_mbox)

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

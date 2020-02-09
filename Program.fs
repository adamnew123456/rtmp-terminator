open System
open System.Net
open System.Net.Sockets
open System.Threading

open ChunkProtocol

let callbacks = {
    InitialState = fun () -> ()

    OnConnectionClose = fun reason () -> printf "OnConnectionClose(%s)\n" reason

    OnMessageReceived = fun message () ->
        match message with
        | Handshake1 version ->
            printf "<= Handshake 1\n\tversion = %d\n" version

        | Handshake2 {InitTimestamp=ts; RandomData=data} ->
            printf "<= Handshake 2\n\ttimestamp = %d\n\tbytes =\n%s\n"
                   ts
                   (Utils.hexdump data)

        | Handshake3 handshake ->
            printf "<= Handshake 3\n\tack_timestamp = %d\n\tread_timestamp = %d\n\tack_bytes =\n%s\n"
                   handshake.AckTimestamp
                   handshake.ReadTimestamp
                   (Utils.hexdump handshake.AckRandomData)

        | ChunkData (header, data) ->
            printf "<= Chunk:\n\theader_type = %A\n\tstream_id = %d\n\ttimestamp = %d\n\tremaining = %d\n\tmessage_stream_id = %d\n\tmessage_type = %A\n\tbytes =\n%s\n"
                   header.MessageHeader
                   header.ChunkStreamId
                   header.Timestamp
                   header.MessageRemaining
                   header.MessageStreamId
                   header.MessageType
                   (Utils.hexdump data)

    OnMessageSent = fun message () ->
        match message with
        | Handshake1 version ->
            printf "=> Handshake 1\n\tversion = %d\n" version

        | Handshake2 {InitTimestamp=ts; RandomData=data} ->
            printf "=> Handshake 2\n\ttimestamp = %d\n\tbytes =\n%s\n"
                   ts
                   (Utils.hexdump data)

        | Handshake3 handshake ->
            printf "=> Handshake 3\n\tack_timestamp = %d\n\tread_timestamp = %d\n\tack_bytes =\n%s\n"
                   handshake.AckTimestamp
                   handshake.ReadTimestamp
                   (Utils.hexdump handshake.AckRandomData)

        | ChunkData (header, data) ->
            printf "=> Chunk:\n\theader_type = %A\n\tstream_id = %d\n\ttimestamp = %d\n\tremaining = %d\n\tmessage_stream_id = %d\n\tmessage_type = %A\n\tbytes =\n%s\n"
                   header.MessageHeader
                   header.ChunkStreamId
                   header.Timestamp
                   header.MessageRemaining
                   header.MessageStreamId
                   header.MessageType
                   (Utils.hexdump data)

    OnSetChunkSize = fun size () ->
        printf "OnSetChunkSize(%d)\n" size

    OnAbort = fun () ->
        printf "Abort()\n"

    OnAcknowledge = fun size () ->
        printf "Acknowledge(%d)\n" size

    OnUserControl = fun data () ->
        printf "UserControl():\n%s\n" (Utils.hexdump data)

    OnWindowAckSize = fun size () ->
        printf "WindowAckSize(%d)\n" size

    OnSetPeerBandwidth = fun size ltype () ->
        printf "SetPeerBandwidth(%d, %d)\n" size ltype

    OnAMFZeroMessage = fun (command, txn, value) () ->
        printf "AMFZeroMessage()\n\tcommand = %s\n\ttxn = %f\n\tparams = %A\n" command txn value
}

[<EntryPoint>]
let main argv =
    let endpoint = IPEndPoint(IPAddress.Any, 1935)
    let server = Socket(SocketType.Stream, ProtocolType.Tcp)
    server.Bind(endpoint)
    server.Listen(10)

    let peer = server.Accept()
    chunk_protocol peer callbacks

    server.Close()

    0

open System
open System.Net
open System.Net.Sockets
open System.Threading

open ChunkProtocol

let callbacks = {
    InitialState = fun () -> (0u, None)

    OnConnectionClose = fun reason (ts, flv) ->
        printf "OnConnectionClose(%s)\n" reason
        match flv with
        | Some (flv_stream: FLVVideo.FLVStream) -> flv_stream.OutputStream.Close()
        | _ -> ()
        (ts, flv)

    OnMessageReceived = fun message (ts, flv) ->
        match message with
        | Handshake1 version ->
            eprintf "<= Handshake 1\n\tversion = %d\n" version
            (ts, flv)

        | Handshake2 {InitTimestamp=ts; RandomData=data} ->
            eprintf "<= Handshake 2\n\ttimestamp = %d\n\tbytes =\n%s\n"
                    ts
                    (Utils.hexdump data)
            (ts, flv)

        | Handshake3 handshake ->
            eprintf "<= Handshake 3\n\tack_timestamp = %d\n\tread_timestamp = %d\n\tack_bytes =\n%s\n"
                    handshake.AckTimestamp
                    handshake.ReadTimestamp
                    (Utils.hexdump handshake.AckRandomData)
            (ts, flv)

        | ChunkData (header, data) ->
            eprintf "<= Chunk:\n\theader_type = %A\n\tstream_id = %d\n\ttimestamp = %d\n\tremaining = %d\n\tmessage_stream_id = %d\n\tmessage_type = %A\n\tbytes =\n%s\n"
                    header.MessageHeader
                    header.ChunkStreamId
                    header.Timestamp
                    header.MessageRemaining
                    header.MessageStreamId
                    header.MessageType
                    (Utils.hexdump data)
            (header.Timestamp, flv)

    OnMessageSent = fun message state ->
        match message with
        | Handshake1 version ->
            eprintf "=> Handshake 1\n\tversion = %d\n" version

        | Handshake2 {InitTimestamp=ts; RandomData=data} ->
            eprintf "=> Handshake 2\n\ttimestamp = %d\n\tbytes =\n%s\n"
                    ts
                    (Utils.hexdump data)

        | Handshake3 handshake ->
            eprintf "=> Handshake 3\n\tack_timestamp = %d\n\tread_timestamp = %d\n\tack_bytes =\n%s\n"
                    handshake.AckTimestamp
                    handshake.ReadTimestamp
                    (Utils.hexdump handshake.AckRandomData)

        | ChunkData (header, data) ->
            eprintf "=> Chunk:\n\theader_type = %A\n\tstream_id = %d\n\ttimestamp = %d\n\tremaining = %d\n\tmessage_stream_id = %d\n\tmessage_type = %A\n\tbytes =\n%s\n"
                    header.MessageHeader
                    header.ChunkStreamId
                    header.Timestamp
                    header.MessageRemaining
                    header.MessageStreamId
                    header.MessageType
                    (Utils.hexdump data)

        state

    OnSetChunkSize = fun size state ->
        eprintf "OnSetChunkSize(%d)\n" size
        state

    OnAbort = fun state ->
        eprintf "Abort()\n"
        state

    OnAcknowledge = fun size state ->
        eprintf "Acknowledge(%d)\n" size
        state

    OnUserControl = fun data state ->
        eprintf "UserControl():\n%s\n" (Utils.hexdump data)
        state

    OnWindowAckSize = fun size state ->
        eprintf "WindowAckSize(%d)\n" size
        state

    OnSetPeerBandwidth = fun size ltype state ->
        eprintf "SetPeerBandwidth(%d, %d)\n" size ltype
        state

    OnAMFZeroCommand = fun (command, txn, value) state ->
        eprintf "AMFZeroCommand()\n\tcommand = %s\n\ttxn = %f\n\tparams = %A\n" command txn value
        state

    OnAMFZeroData = fun (handler, values) (ts, flv) ->
        eprintf "AMFZeroData()\n\thandler = %s\n\tparams = %A\n" handler values
        match (handler, values) with
        | ("@setDataFrame", _ :: metadata :: _) ->
            let out_file = new System.IO.FileStream("out.flv", System.IO.FileMode.Create)
            let flv_stream = FLVVideo.init_stream out_file ts metadata.Value
            (ts, Some flv_stream)
        | _ ->
            (ts, flv)

    OnVideoData = fun data (ts, flv) ->
        FLVVideo.write_video_frame (Option.get flv) ts (Utils.full_slice data)
        (ts, flv)

    OnAudioData = fun data (ts, flv) ->
        FLVVideo.write_audio_frame (Option.get flv) ts (Utils.full_slice data)
        (ts, flv)
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

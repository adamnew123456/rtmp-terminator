module HTTPRepeater
open Utils

open System.Collections.Concurrent
open System.Net.Sockets

/// <summary>
/// The types of events that the HTTP repeater can process
/// </summary>
type Events =
    | MetadataReceived of AMFZero.ValueType
    | VideoReceived of uint32 * byte array
    | AudioReceived of uint32 * byte array
    | NewConnection of Socket
    | Stopped

type RepeaterState = {
    ExistingConnections: (Socket * FLVVideo.FLVStream) list
    NewConnections: Socket list
    SequenceHeader: byte array option
    Metadata: AMFZero.ValueType
    Timestamp: uint32
}

/// <summary>
/// Sends the initial FLV header and frame of metadata to the client
/// </summary>
let init_client (state: RepeaterState) (client: Socket) =
    eprintf "Initializing FLV on new client from %A" client.RemoteEndPoint
    try
        let (slice, stream) = FLVVideo.init_stream state.Timestamp state.Metadata
        net_send_bytes client slice

        let (Some seq_header) = state.SequenceHeader
        let slice = FLVVideo.write_video_frame stream
                                               state.Timestamp
                                               (full_slice seq_header)


        net_send_bytes client slice
        Result.Ok stream
    with
    | :? SocketException as err ->
        Result.Error (sprintf "Dropped connection: %s" err.Message)

/// <summary>
/// Sends a video frame to all existing clients
/// </summary>
let send_video (state: RepeaterState) (video: System.ArraySegment<uint8>) =
    let connections =
        state.ExistingConnections
        |> List.filter (fun (client, stream) ->
            let slice = FLVVideo.write_video_frame stream state.Timestamp video
            try
                net_send_bytes client slice
                true
            with
            | :? SocketException as err ->
                eprintfn "Dropping %A due to %s" client.RemoteEndPoint err.Message
                false)

    {state with ExistingConnections=connections}

/// <summary>
/// Sends an audio frame to all existing clients
/// </summary>
let send_audio (state: RepeaterState) (audio: System.ArraySegment<uint8>) =
    let connections =
        state.ExistingConnections
        |> List.filter (fun (client, stream) ->
            let slice = FLVVideo.write_audio_frame stream state.Timestamp audio
            try
                net_send_bytes client slice
                true
            with
            | :? SocketException as err ->
                eprintfn "Dropping %A due to %s" client.RemoteEndPoint err.Message
                false)

    {state with ExistingConnections=connections}

/// <summary>
/// Sends a video frame to clients, with extra processing for keyframes and AVC
/// headers
/// </summary>
let process_video (state: RepeaterState) (video: byte array) =
    let tag = video.[0]
    let is_keyframe = (tag >>> 4) &&& 0xFuy = 1uy

    let state =
        match state.SequenceHeader with
        | None ->
            let tag = video.[0]
            let codec = tag &&& 0xFuy
            if codec = 7uy then
                let avc_packet_type = video.[1]
                if avc_packet_type = 0uy then
                    // Buffer the sequence header for later to send to new clients
                    eprintfn "Saving AVC sequence header of %d bytes" video.Length
                    {state with SequenceHeader=Some video}
                else
                    state
            else
                // The sequence header is required before we can stream to
                // clients, so whatever this is can be ignored
                state

        | Some _ ->
            state

    let state =
        match (state.SequenceHeader, is_keyframe) with
        | (Some _, true) ->
            // With both metadata and the AVC sequence header we can initialize
            // new clients and start streaming data to them
            let new_clients = List.length state.NewConnections
            if new_clients > 0 then
                eprintfn "Processing %d new clients on keyframe" (List.length state.NewConnections)

            let added_connections =
                state.NewConnections
                |> List.map (fun client -> (client, init_client state client))
                |> List.filter (fun (_, stream) ->
                    match stream with
                    | Result.Ok _ ->
                        true
                    | Result.Error msg ->
                        eprintfn "%s" msg
                        false)
                |> List.map (fun (client, Result.Ok stream) -> (client, stream))

            {state with NewConnections=[]
                        ExistingConnections=added_connections @ state.ExistingConnections}

        | _ ->
            state

    send_video state (full_slice video)

/// <summary>
/// Dispatches video/audio frames and accepts connections initialized by the
/// HTTPAcceptor
/// </summary>
let runner (mailbox: BlockingCollection<Events>) =
    let rec step state =
        match mailbox.Take() with
        | MetadataReceived metadata ->
            eprintfn "Saving metadata %A" metadata
            step {state with Metadata=metadata}

        | VideoReceived (timestamp, frame) ->
            let state = {state with Timestamp=timestamp}
            step (process_video state frame)

        | AudioReceived (timestamp, frame) ->
            let state = {state with Timestamp=timestamp}
            step (send_audio state (full_slice frame))

        | NewConnection client ->
            step {state with NewConnections=client :: state.NewConnections}

        | Stopped ->
            state.NewConnections
            |> List.iter (fun conn -> conn.Close())

            state.ExistingConnections
            |> List.iter (fun (conn, _) -> conn.Close())

    let state = {
        ExistingConnections=[]
        NewConnections=[]
        SequenceHeader=None
        Metadata=AMFZero.NullVal
        Timestamp=0u
    }

    step state

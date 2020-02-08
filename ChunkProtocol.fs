module ChunkProtocol
open System.Net.Sockets
open Utils

type Timestamp = uint32

/// <summary>
/// The default value used for the window acknowledgment size
/// </summary>
let window_size_def = 4096u

/// <summary>
/// The default value used for chunk size
/// </summary>
let chunk_size_def = 128u

/// <summary>
/// The type of the message header that follows the basic header.
/// The format of each is detailed in section 5.3.1.2
/// </summary>
type MessageHeaderType =
    | FullHeader     // Type 0
    | ShareStream    // Type 1, shares stream with last message
    | DeltaTimestamp // Type 2, shares everything but timestamp
    | ShareAll       // Type 3, shares all properties

/// <summary>
/// Maps byte values to MessageHeaderType
/// </summary>
let message_header_type_map =
    Map.ofList [
        0uy, FullHeader
        1uy, ShareStream
        2uy, DeltaTimestamp
        3uy, ShareAll
    ]

/// <summary>
/// The available protocol message types, including control messages
/// </summary>
type MessageType =
    | SetChunkSize     // Type 1
    | Abort            // Type 2
    | Acknowledge      // Type 3
    | UserControl      // Type 4
    | WindowAckSize    // Type 5
    | SetPeerBandwidth // Type 6
    | AudioData        // Type 8
    | VideoData        // Type 9
    | DataMessageAMF3  // Type 15
    | SharedObjectAMF3 // Type 16
    | CommandAMF3      // Type 17
    | DataAMF0         // Type 18
    | SharedObjectAMF0 // Type 19
    | CommandAMF0      // Type 20
    | Aggregate        // Type 22

/// <summary>
/// Maps byte values to MessageType
/// </summary>
let message_type_map =
    Map.ofList [
        1uy, SetChunkSize
        2uy, Abort
        3uy, Acknowledge
        4uy, UserControl
        5uy, WindowAckSize
        6uy, SetPeerBandwidth
        8uy, AudioData
        9uy, VideoData
        15uy, DataMessageAMF3
        16uy, SharedObjectAMF3
        17uy, CommandAMF3
        18uy, DataAMF0
        19uy, SharedObjectAMF0
        20uy, CommandAMF0
        22uy, Aggregate
    ]

/// <summary>
/// The contents of the timestamp handshake, section 5.2.3
/// </summary>
type Handshake2Data = {
    InitTimestamp: Timestamp
    RandomData: System.ArraySegment<uint8>
}

/// <summary>
/// The contents of the ack handshake, section 5.2.4
/// </summary>
type Handshake3Data = {
    AckTimestamp: Timestamp
    ReadTimestamp: Timestamp
    AckRandomData: System.ArraySegment<uint8>
}

/// <summary>
/// The header data included in a chunk stream packet. Includes both the basic
/// header described in 5.3.1.1 as well as the message header from 5.3.1.2
/// </summary>
type ChunkHeader = {
    ChunkStreamId: uint32
    Timestamp: uint32
    MessageRemaining: uint32
    MessageHeader: MessageHeaderType
    MessageType: MessageType
    MessageStreamId: uint32
}

/// <summary>
/// The different types of chunk stream packages
/// </summary>
type RtmpChunkMessage =
    | Handshake1 of uint8
    | Handshake2 of Handshake2Data
    | Handshake3 of Handshake3Data
    | ChunkData of ChunkHeader * System.ArraySegment<uint8>

/// <summary>
/// The state used by a single stream of the RTMP chunk processor
/// </summary>
type ChunkState = {
    Buffer: uint8 array list
    MessageStreamId: uint32
    MessageType: MessageType
    MessageRemaining: uint32
    Timestamp: uint32
}

/// <summary>
/// Callbacks used when different events occur on the chunk session
/// </summary>
type ChunkEvents<'state> = {
    InitialState: unit -> 'state
    OnConnectionClose: string -> 'state -> 'state
    OnMessageReceived: RtmpChunkMessage -> 'state -> 'state
    OnMessageSent: RtmpChunkMessage -> 'state -> 'state
    OnSetChunkSize: uint32 -> 'state -> 'state
    OnAbort: 'state -> 'state
    OnAcknowledge: uint32 -> 'state -> 'state
    OnUserControl: System.ArraySegment<uint8> -> 'state -> 'state
    OnWindowAckSize: uint32 -> 'state -> 'state
    OnSetPeerBandwidth: uint32 -> uint8 -> 'state -> 'state
    OnAMFZeroMessage: (string * uint32 * AMFZero.ValueType) -> 'state -> 'state
}

/// <summary>
/// The full state used by the RTMP chunk processor
/// </summary>
type ConnectionState<'userdata> = {
    Streams: Map<uint32, ChunkState>
    NextMessageStream: uint32
    Socket: Socket
    Timeout: int32
    Callbacks: ChunkEvents<'userdata>
    UserData: 'userdata
    TimestampEpoch: uint64
    MessageBuffer: uint8 array
    ChunkSize: uint32
    AckWindow: RolloverCounter
    OutputQueue: RtmpChunkMessage list
}

/// <summary>
/// An initial chunk state used immediately after the handshake is
/// received
/// </summary>
let init_stream_state = {
    Buffer=[]
    MessageStreamId=0u
    MessageType=Abort
    MessageRemaining=0u
    Timestamp=0u
}

/// <summary>
/// Retrieves the details for a particular chunk stream in a connection
/// </summary>
let get_stream (id: uint32) (conn: ConnectionState<_>) =
    Map.find id conn.Streams

/// <summary>
/// Updates the details for a particular chunk stream in a connection
/// </summary>
let set_stream (id: uint32) (stream: ChunkState) (conn: ConnectionState<_>) =
    {conn with Streams=Map.add id stream conn.Streams}

/// <summary>
/// Enqueues a new message to be sent to the client
/// </summary>
let enqueue_reply (msg: RtmpChunkMessage) (conn: ConnectionState<_>) =
    {conn with OutputQueue=msg :: conn.OutputQueue}

/// <summary>
/// Adds a new block to the chunk's buffer
/// </summary>
let push_stream_buffer (buffer: uint8 array) (stream: ChunkState) =
    {stream with Buffer=buffer :: stream.Buffer}

/// <summary>
/// Invokes a callback on the connection, returning a connection with an
/// updated user state
/// </summary>
let invoke_callback (cb: 'userdata -> 'userdata) (conn: ConnectionState<'userdata>) =
    {conn with UserData=cb conn.UserData}

/// <summary>
/// Reads a basic chunk header and returns the header type and chunk stream id
/// </summary>
let read_basic_header (conn: ConnectionState<_>) =
    let read_slice = range_slice conn.MessageBuffer 0 1

    // 5.3.1.1

    let read_two_byte_csid header_type =
        read_net_bytes conn.Socket read_slice conn.Timeout
        |> Result.map (fun bytes ->
            (header_type, 64u + uint32 bytes.[0]))

    let read_three_byte_csid header_type =
        let ext_read_slice = range_slice conn.MessageBuffer 0 2
        read_net_bytes conn.Socket ext_read_slice conn.Timeout
        |> Result.map (fun bytes ->
            let stream_id = (256u * uint32 bytes.[1]) + uint32 bytes.[0] + 64u
            (header_type, stream_id))

    read_net_bytes conn.Socket read_slice conn.Timeout
    |> Result.map (fun bytes ->
        let header_type = Map.find (bytes.[0] >>> 6) message_header_type_map
        let stream_id = bytes.[0] &&& 0x3Fuy
        (header_type, stream_id))
    |> Result.bind (fun (header_type, stream_id) ->
        if stream_id = 0uy then
            read_two_byte_csid header_type
        elif stream_id = 1uy then
            read_three_byte_csid header_type
        else
            Result.Ok (header_type, uint32 stream_id))

/// <summary>
/// Reads a message header and updates the state of the chunk
/// header
/// </summary>
let read_message_header (conn: ConnectionState<_>)
                        (ty: MessageHeaderType)
                        (stream_id: uint32) =
    let read_extended_timestamp () =
        let read_slice = range_slice conn.MessageBuffer 0 4
        read_net_bytes conn.Socket read_slice conn.Timeout
        |> Result.map be_bytes_to_uint32

    let read_full_header bytes =
        let timestamp = be_bytes_to_uint24 bytes
        let msg_length = be_bytes_to_uint24 (bytes.Slice(3))
        let msg_type = Map.find bytes.[6] message_type_map
        let msg_stream = be_bytes_to_uint32 (bytes.Slice(7))
        if timestamp = 0xFFFFFFu then
            read_extended_timestamp ()
            |> Result.map (fun ext_timestamp ->
                (Some ext_timestamp,
                 Some msg_length,
                 Some msg_type,
                 Some msg_stream))
        else
            Result.Ok (Some timestamp,
                       Some msg_length,
                       Some msg_type,
                       Some msg_stream)

    let read_sharestream_header bytes =
        let stream_info = get_stream stream_id conn
        let timestamp_delta = be_bytes_to_uint24 bytes
        let msg_length = be_bytes_to_uint24 (bytes.Slice(3))
        let msg_type = Map.find bytes.[6] message_type_map
        if timestamp_delta = 0xFFFFFFu then
            read_extended_timestamp ()
            |> Result.map (fun ext_delta ->
                (Some (stream_info.Timestamp + ext_delta),
                 Some msg_length,
                 Some msg_type,
                 None))
        else
            Result.Ok (Some (stream_info.Timestamp + timestamp_delta),
                       Some msg_length,
                       Some msg_type,
                       None)

    let read_deltatimestamp_header bytes =
        let stream_info = get_stream stream_id conn
        let timestamp_delta = be_bytes_to_uint24 bytes
        if timestamp_delta = 0xFFFFFFu then
            read_extended_timestamp ()
            |> Result.map (fun ext_delta ->
                (Some (stream_info.Timestamp + ext_delta),
                 None,
                 None,
                 None))
        else
            Result.Ok (Some (stream_info.Timestamp + timestamp_delta),
                       None,
                       None,
                       None)

    match ty with
    | FullHeader ->
        let read_slice = range_slice conn.MessageBuffer 0 11
        read_net_bytes conn.Socket read_slice conn.Timeout
        |> Result.bind read_full_header

    | ShareStream ->
        let read_slice = range_slice conn.MessageBuffer 0 7
        read_net_bytes conn.Socket read_slice conn.Timeout
        |> Result.bind read_sharestream_header

    | DeltaTimestamp ->
        let read_slice = range_slice conn.MessageBuffer 0 3
        read_net_bytes conn.Socket read_slice conn.Timeout
        |> Result.bind read_deltatimestamp_header

    | ShareAll ->
        Result.Ok (None, None, None, None)

/// <summary>
/// Sends a single message to the client
/// </summary>
let send_single_message (conn: ConnectionState<_>) (msg: RtmpChunkMessage) =
    let mutable packet_slice = full_slice conn.MessageBuffer
    let conn = invoke_callback (conn.Callbacks.OnMessageSent msg) conn

    match msg with
    | Handshake1 version ->
        packet_slice.[0] <- uint8 version
        net_send_bytes conn.Socket (range_slice conn.MessageBuffer 0 1)
        conn

    | Handshake2 handshake ->
        uint32_to_be_bytes handshake.InitTimestamp packet_slice
        uint32_to_be_bytes 0u (packet_slice.Slice(4))
        net_send_bytes conn.Socket (range_slice conn.MessageBuffer 0 8)
        net_send_bytes conn.Socket handshake.RandomData
        conn

    | Handshake3 handshake ->
        uint32_to_be_bytes handshake.AckTimestamp packet_slice
        uint32_to_be_bytes handshake.ReadTimestamp (packet_slice.Slice(4))
        net_send_bytes conn.Socket (range_slice conn.MessageBuffer 0 8)
        net_send_bytes conn.Socket handshake.AckRandomData
        conn

    | ChunkData (header, contents) ->
        // Write a Type 0 header regardless of the contents of the header
        // value. This avoids the need to track any information on the
        // previous messages we sent.
        if header.ChunkStreamId < 63u then
            packet_slice.[0] <- uint8 header.ChunkStreamId
            packet_slice <- packet_slice.Slice(1)
        elif header.ChunkStreamId < 320u then
            packet_slice.[0] <- 0uy
            packet_slice.[1] <- uint8 (header.ChunkStreamId - 64u)
            packet_slice <- packet_slice.Slice(2)
        else
            packet_slice.[0] <- 1uy
            let msb = header.ChunkStreamId >>> 8
            let lsb = header.ChunkStreamId &&& 0xFFu
            packet_slice.[1] <- uint8 msb
            packet_slice.[2] <- uint8 lsb
            packet_slice <- packet_slice.Slice(3)

        let ext_timestamp =
            if header.Timestamp >= 0xFFFFFFu then
                uint24_to_be_bytes 0xFFFFFFu packet_slice
                true
            else
                uint24_to_be_bytes header.Timestamp packet_slice
                false

        packet_slice <- packet_slice.Slice(3)
        uint24_to_be_bytes header.MessageRemaining packet_slice

        packet_slice <- packet_slice.Slice(3)
        packet_slice.[0] <- inverse_find header.MessageType message_type_map

        packet_slice <- packet_slice.Slice(1)
        uint32_to_be_bytes header.MessageStreamId packet_slice

        packet_slice <- packet_slice.Slice(4)
        if ext_timestamp then
            uint32_to_be_bytes header.Timestamp packet_slice
            packet_slice <- packet_slice.Slice(4)

        net_send_bytes conn.Socket (range_slice conn.MessageBuffer 0 packet_slice.Offset)
        net_send_bytes conn.Socket contents
        conn

/// <summary>
/// Reads and processes the next chunk stream packet
/// </summary>
let rec process_chunk_packet (conn: ConnectionState<_>) =
    let update_stream_info header_type stream_id =
        read_message_header conn header_type stream_id
        |> Result.map (fun header_data ->
            match header_data with
            | (Some ts, Some msg_len, Some msg_type, Some msg_stream) ->
                let stream_state =
                    match Map.tryFind stream_id conn.Streams with
                    | Some state -> state
                    | None -> init_stream_state

                {stream_state with MessageStreamId=msg_stream
                                   MessageType=msg_type
                                   MessageRemaining=msg_len
                                   Timestamp=ts}

            | (Some ts, Some msg_len, Some msg_type, None) ->
                let stream_state = get_stream stream_id conn
                {stream_state with MessageType=msg_type
                                   MessageRemaining=msg_len
                                   Timestamp=ts}

            | (Some ts, None, None, None) ->
                let stream_state = get_stream stream_id conn
                {stream_state with Timestamp=ts}

            | _ ->
                get_stream stream_id conn)

    let materialize_full_message chunks =
        let size =
            chunks
            |> List.sumBy Array.length

        let (chunk_bytes: uint8 array) = Array.zeroCreate size
        chunks
        |> List.rev
        |> List.fold (fun offset chunk ->
            System.Array.Copy(chunk, 0, chunk_bytes, offset, chunk.Length)
            offset + chunk.Length)
                     0
        |> ignore

        chunk_bytes

    let process_full_message stream_id conn =
        let stream_state = get_stream stream_id conn
        let message_bytes = materialize_full_message stream_state.Buffer

        let stream_state = {stream_state with Buffer=[]}
        let conn = set_stream stream_id stream_state conn

        match stream_state.MessageType with
        | SetChunkSize ->
            let chunk_size = min 0xFFFFFFu (be_bytes_to_uint32 (full_slice message_bytes))
            let conn = invoke_callback (conn.Callbacks.OnSetChunkSize chunk_size) conn
            if chunk_size <> 0u then
                {conn with ChunkSize=chunk_size}
            else
                conn

        | Abort ->
            let target_stream = be_bytes_to_uint32 (full_slice message_bytes)
            let conn = invoke_callback conn.Callbacks.OnAbort conn
            let target_state = get_stream stream_id conn
            let flushed_state =
                {target_state with Buffer=[]
                                   MessageRemaining=0u}
            set_stream target_stream flushed_state conn

        | Acknowledge ->
            let sequence_no = be_bytes_to_uint32 (full_slice message_bytes)
            invoke_callback (conn.Callbacks.OnAcknowledge sequence_no) conn

        | UserControl ->
            invoke_callback (conn.Callbacks.OnUserControl (full_slice message_bytes)) conn

        | WindowAckSize ->
            let ack_window = be_bytes_to_uint32 (full_slice message_bytes)
            {conn with AckWindow={conn.AckWindow with Max=ack_window}}
            |> invoke_callback (conn.Callbacks.OnWindowAckSize ack_window)

        | SetPeerBandwidth ->
            // TODO - Needs to implement tracking of client ACKs within the
            // output queue
            let ack_window = be_bytes_to_uint32 (full_slice message_bytes)
            let limit_type = message_bytes.[5]
            invoke_callback (conn.Callbacks.OnSetPeerBandwidth ack_window limit_type) conn

        | CommandAMF0 ->
            let (cmd_end, cmd_context) = AMFZero.parse (full_slice message_bytes)
            let (txn_end, txn_context) = AMFZero.parse cmd_end
            let (_, param_context) = AMFZero.parse txn_end

            let (AMFZero.StringVal command) = cmd_context.Value
            let (AMFZero.NumberVal transaction) = txn_context.Value

            let conn =
                invoke_callback (conn.Callbacks.OnAMFZeroMessage (command, uint32 transaction, param_context.Value))
                                conn

            match command with
            | "connect" ->
                // 2 bytes event type + 4 bvtes new stream id
                // Max 27 bytes for AMF0 response content
                let event_header = {
                    ChunkStreamId=2u
                    Timestamp=uint32 (unix_time_ms () - conn.TimestampEpoch)
                    MessageRemaining=6u
                    MessageHeader=FullHeader
                    MessageType=UserControl
                    MessageStreamId=0u
                }

                let new_stream = conn.NextMessageStream
                let conn = {conn with NextMessageStream=new_stream + 1u}

                let (connect_reply_buffer: uint8 array) = Array.zeroCreate 100
                let mutable reply_slice = full_slice connect_reply_buffer

                uint16_to_be_bytes 0us reply_slice
                reply_slice <- reply_slice.Slice(2)

                uint32_to_be_bytes new_stream reply_slice
                reply_slice <- reply_slice.Slice(4)

                reply_slice <- AMFZero.encode reply_slice (AMFZero.StringVal "_result")
                reply_slice <- AMFZero.encode reply_slice (AMFZero.NumberVal (double 1))
                reply_slice <- AMFZero.encode reply_slice AMFZero.NullVal
                reply_slice <- AMFZero.encode reply_slice AMFZero.NullVal

                let result_header = {
                    ChunkStreamId=2u
                    Timestamp=uint32 (unix_time_ms () - conn.TimestampEpoch)
                    MessageRemaining=uint32 (reply_slice.Offset - 6)
                    MessageHeader=FullHeader
                    MessageType=CommandAMF0
                    MessageStreamId=0u
                }

                let result_length = reply_slice.Offset - 6

                conn
                |> enqueue_reply (ChunkData (event_header, range_slice connect_reply_buffer 0 6))
                |> enqueue_reply (ChunkData (result_header, range_slice connect_reply_buffer 6 result_length))

            | _ ->
                let (rpc_reply_buffer: uint8 array) = Array.zeroCreate 4096
                let mutable reply_slice = full_slice rpc_reply_buffer
                reply_slice <- AMFZero.encode reply_slice (AMFZero.StringVal command)
                reply_slice <- AMFZero.encode reply_slice (AMFZero.NumberVal transaction)
                reply_slice <- AMFZero.encode reply_slice AMFZero.NullVal
                reply_slice <- AMFZero.encode reply_slice AMFZero.NullVal

                let result_header = {
                    ChunkStreamId=2u
                    Timestamp=uint32 (unix_time_ms () - conn.TimestampEpoch)
                    MessageRemaining=uint32 reply_slice.Offset
                    MessageHeader=FullHeader
                    MessageType=CommandAMF0
                    MessageStreamId=0u
                }

                conn
                |> enqueue_reply (ChunkData (result_header, range_slice rpc_reply_buffer 0 reply_slice.Offset))

        | _ ->
            conn

    let process_chunk_bytes (bytes, stream_id, header_info, conn) =
        let message = ChunkData (header_info, bytes)
        let conn = invoke_callback (conn.Callbacks.OnMessageReceived message) conn

        let (buffer_bytes: uint8 array) = Array.zeroCreate bytes.Count
        bytes.CopyTo(buffer_bytes)

        let (send_ack, ack_window) = incr_rcounter conn.AckWindow (uint32 bytes.Count)
        let conn =
            if send_ack then
                let ack_header = {
                    ChunkStreamId=2u
                    Timestamp=uint32 (unix_time_ms () - conn.TimestampEpoch)
                    MessageRemaining=4u
                    MessageHeader=FullHeader
                    MessageType=Acknowledge
                    MessageStreamId=0u
                }

                let ack_reply = full_slice (Array.zeroCreate 4)
                uint32_to_be_bytes (seq_rcounter ack_window) ack_reply
                enqueue_reply (ChunkData (ack_header, ack_reply)) conn
            else
                conn

        let stream_state =
            get_stream stream_id conn
            |> push_stream_buffer buffer_bytes

        let is_full_message =
            not (List.isEmpty stream_state.Buffer)
            && stream_state.MessageRemaining = 0u

        let conn =
            {conn with AckWindow=ack_window}
            |> set_stream stream_id stream_state

        let conn =
            if is_full_message then
                process_full_message stream_id conn
            else
                conn

        let conn =
            List.rev conn.OutputQueue
            |> List.fold send_single_message conn

        {conn with OutputQueue=[]}

    // Prepare the initial chunk and window size assignment if this is the
    // first client message we're processing
    let conn =
        if conn.AckWindow.Current = 0u && conn.AckWindow.TimesFilled = 0u then
            // 4 bytes of chunk size
            // 4 bytes of window ack size
            let (init_send_buffer: uint8 array) = Array.zeroCreate 8
            let chunk_size_slice = range_slice init_send_buffer 0 4
            let window_size_slice = range_slice init_send_buffer 4 4

            let chunk_size_header = {
                ChunkStreamId=2u
                Timestamp=uint32 (unix_time_ms () - conn.TimestampEpoch)
                MessageRemaining=4u
                MessageHeader=FullHeader
                MessageType=SetChunkSize
                MessageStreamId=0u
            }

            let window_size_header = {
                ChunkStreamId=2u
                Timestamp=uint32 (unix_time_ms () - conn.TimestampEpoch)
                MessageRemaining=4u
                MessageHeader=FullHeader
                MessageType=WindowAckSize
                MessageStreamId=0u
            }

            uint32_to_be_bytes (60u * 1024u * 1024u) chunk_size_slice
            uint32_to_be_bytes window_size_def window_size_slice

            conn
            |> enqueue_reply (ChunkData (chunk_size_header, chunk_size_slice))
            |> enqueue_reply (ChunkData (window_size_header, window_size_slice))
        else
            conn

    let read_result =
        read_basic_header conn
        |> Result.bind (fun (header_type, stream_id) ->
            update_stream_info header_type stream_id
            |> Result.map (fun stream_state ->
                let header_info = {
                    ChunkStreamId=stream_id
                    Timestamp=stream_state.Timestamp
                    MessageRemaining=stream_state.MessageRemaining
                    MessageHeader=header_type
                    MessageType=stream_state.MessageType
                    MessageStreamId=stream_state.MessageStreamId
                }
                (stream_id, header_info, stream_state)))
        |> Result.bind (fun (stream_id, header_info, stream_state) ->
          let data_size = min conn.ChunkSize stream_state.MessageRemaining
          let msg_left = stream_state.MessageRemaining - data_size
          let stream_state = {stream_state with MessageRemaining=msg_left}
          let conn = set_stream stream_id stream_state conn

          let read_slice = range_slice conn.MessageBuffer 0 (int data_size)
          read_net_bytes conn.Socket read_slice conn.Timeout
          |> Result.map (fun bytes -> (bytes, stream_id, header_info, conn)))
        |> Result.map process_chunk_bytes

    match read_result with
    | Result.Ok conn ->
        process_chunk_packet conn

    | Result.Error msg ->
        invoke_callback (conn.Callbacks.OnConnectionClose msg) conn |> ignore
        conn.Socket.Close()

/// <summary>
/// Validates the acknowledgment handshake from the client and enters the main
/// RTMP state
/// </summary>
let ack_handshake_state (conn: ConnectionState<_>)
                        (server_time: uint32)
                        (server_random: uint8 array) =
    // Section 5.2.4
    let read_slice = range_slice conn.MessageBuffer 0 1536
    match read_net_bytes conn.Socket read_slice conn.Timeout with
    | Result.Ok bytes ->
        let ack_time = be_bytes_to_uint32 bytes
        let ack_data = bytes.Slice(8)

        let peer_message = Handshake3 {
            AckTimestamp=ack_time
            ReadTimestamp=be_bytes_to_uint32 (bytes.Slice(4))
            AckRandomData=ack_data
        }

        let conn = invoke_callback (conn.Callbacks.OnMessageReceived peer_message) conn

        if ack_time <> server_time then
            invoke_callback (conn.Callbacks.OnConnectionClose "Incorrect timestamp ACK") conn |> ignore
            conn.Socket.Close()
        elif not (slices_equal ack_data (full_slice server_random)) then
            invoke_callback (conn.Callbacks.OnConnectionClose "Incorrect random data ACK") conn |> ignore
            conn.Socket.Close()
        else
            process_chunk_packet conn
    | Result.Error msg ->
        invoke_callback (conn.Callbacks.OnConnectionClose msg) conn |> ignore
        conn.Socket.Close()

/// <summary>
/// Waits for the secondary handshake from the client and passes control onto
/// the acknowledgment handshake state
/// </summary>
let client_handshake2_state (conn: ConnectionState<_>) =
    // Section 5.2.3
    let read_slice = range_slice conn.MessageBuffer 0 1536
    match read_net_bytes conn.Socket read_slice conn.Timeout with
    | Result.Ok bytes ->
        let recv_timestamp = uint32 (unix_time_ms () - conn.TimestampEpoch)
        let peer_timestamp = be_bytes_to_uint32 bytes

        let data_slice = bytes.Slice(8)
        let peer_message = Handshake2 {
            InitTimestamp=peer_timestamp
            RandomData=data_slice
        }

        let conn = invoke_callback (conn.Callbacks.OnMessageReceived peer_message) conn

        // Version handshake 5.2.2
        let conn = send_single_message conn (Handshake1 3uy)

        // Initial handshake 5.2.3
        let server_random = random_bytes 1528
        let handshake2 = {
            InitTimestamp=0u
            RandomData=full_slice server_random
        }
        let conn = send_single_message conn (Handshake2 handshake2)

        // Ack handshake 5.2.4
        let handshake3 = {
            AckTimestamp=peer_timestamp
            ReadTimestamp=recv_timestamp
            AckRandomData=data_slice
        }
        let conn = send_single_message conn (Handshake3 handshake3)

        ack_handshake_state conn 0u server_random
    | Result.Error msg ->
        invoke_callback (conn.Callbacks.OnConnectionClose msg) conn |> ignore
        conn.Socket.Close()

/// <summary>
/// Waits for the initial handshake from the client and passes control onto the
/// secondary handshake state
/// </summary>
let client_handshake1_state (conn: ConnectionState<_>) =
    // Section 5.2.2
    let read_slice = range_slice conn.MessageBuffer 0 1
    match read_net_bytes conn.Socket read_slice conn.Timeout with
    | Result.Ok bytes ->
        let message = Handshake1 bytes.[0]
        let conn = invoke_callback (conn.Callbacks.OnMessageReceived message) conn

        if bytes.[0] <> 3uy then
            invoke_callback (conn.Callbacks.OnConnectionClose "Unsupported RTMP version requested by client") conn |> ignore
            conn.Socket.Close()
        else
            client_handshake2_state conn
    | Result.Error msg ->
        invoke_callback (conn.Callbacks.OnConnectionClose msg) conn |> ignore
        conn.Socket.Close()

/// <summary>
/// Accepts a socket and manages the RTMP chunk session on it. Any messages which are received are
/// passed onto the ChunkEvents handler.
/// </summary>
let chunk_protocol (connection: Socket) (events: ChunkEvents<_>) =
    let user_data = events.InitialState ()
    let connection_state = {
        Streams=Map.empty
        NextMessageStream=3u
        Socket=connection
        Timeout=60 * 1000
        Callbacks=events
        UserData=user_data
        TimestampEpoch=unix_time_ms ()
        // Must be at least 1536 bytes to fit handshakes
        MessageBuffer=Array.zeroCreate (64 * 1024 * 1024)
        ChunkSize=chunk_size_def
        AckWindow={
            Current=0u
            Max=window_size_def
            TimesFilled=0u
        }
        OutputQueue=[]
    }

    client_handshake1_state connection_state

module ChunkProtocol
open System.Net.Sockets
open Utils

type Timestamp = uint32

/// <summary>
/// The type of the message header that follows the basic header.
/// The format of each is detailed in section 5.3.1.2
/// </summary>
type MessageHeaderType =
    | Full        // Type 0
    | ShareStream // Type 1, shares stream with last message
    | Delta       // Type 2, shares everything but timestamp
    | None        // Type 3, shares all properties

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
    MessageHeader: MessageHeaderType
    ChunkStreamId: uint32
    Timestamp: uint32
    Length: uint32
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
    MessageRemaining: uint32
    AckSize: uint32
    SequenceNumber: uint32
}

/// <summary>
/// Callbacks used when different events occur on the chunk session
/// </summary>
type ChunkEvents<'state> = {
    InitialState: unit -> 'state
    OnConnectionClose: 'state -> string -> 'state
    OnMessageReceived: 'state -> RtmpChunkMessage -> 'state
    OnSetChunkSize: 'state -> uint32 -> 'state
    OnAbort: 'state -> 'state
    OnAcknowledge: 'state -> uint32 -> 'state
    OnUserControl: 'state -> System.ArraySegment<uint8> -> 'state
    OnWindowAckSize: 'state -> uint32 -> 'state
    OnSetPeerBandwidth: 'state -> uint32 -> 'state
}

/// <summary>
/// The full state used by the RTMP chunk processor
/// </summary>
type ConnectionState<'userdata> = {
    Streams: Map<uint32, ChunkState>
    Socket: Socket
    Timeout: int32
    Callbacks: ChunkEvents<'userdata>
    UserData: 'userdata
    TimestampEpoch: uint64
}

/// <summary>
/// Validates the acknowledgment handshake from the client and enters the main
/// RTMP state
/// </summary>
let ack_handshake_state (conn: ConnectionState<_>)
                        (server_time: uint32)
                        (server_random: uint8 array) =
    let (messageBuffer: uint8 array) = Array.zeroCreate 1536

    // Section 5.2.4
    match read_net_bytes conn.Socket messageBuffer 1536 conn.Timeout with
    | ReadError err ->
        conn.Callbacks.OnConnectionClose conn.UserData (sprintf "Read error during handshake: %s" err.Message)
        conn.Socket.Close()

    | PartialData _ ->
        conn.Callbacks.OnConnectionClose conn.UserData "Peer closed connection during handshake: %s"
        conn.Socket.Close()

    | FullData bytes ->
        let ack_time = be_bytes_to_uint32 (range_slice bytes 0 4)
        let ack_data = range_slice bytes 8 1528

        let peer_message = Handshake3 {
            AckTimestamp=ack_time
            ReadTimestamp=be_bytes_to_uint32 (range_slice bytes 4 4)
            AckRandomData=ack_data
        }

        let user_data = conn.Callbacks.OnMessageReceived conn.UserData peer_message

        if ack_time <> server_time then
            conn.Callbacks.OnConnectionClose user_data "Incorrect timestamp ACK"
            conn.Socket.Close()
        elif not (slices_equal ack_data (full_slice server_random)) then
            conn.Callbacks.OnConnectionClose user_data "Incorrect random data ACK"
            conn.Socket.Close()
        else
            conn.Callbacks.OnConnectionClose user_data "Closing connection: handshake done"
            conn.Socket.Close()

/// <summary>
/// Waits for the secondary handshake from the client and passes control onto
/// the acknowledgment handshake state
/// </summary>
let client_handshake2_state (conn: ConnectionState<_>) =
    let (message_buffer: uint8 array) = Array.zeroCreate 1536

    // Section 5.2.3
    match read_net_bytes conn.Socket message_buffer 1536 conn.Timeout with
    | ReadError err ->
        conn.Callbacks.OnConnectionClose conn.UserData (sprintf "Read error during handshake: %s" err.Message)
        conn.Socket.Close()

    | PartialData _ ->
        conn.Callbacks.OnConnectionClose conn.UserData "Peer closed connection during handshake: %s"
        conn.Socket.Close()

    | FullData bytes ->
        let recv_timestamp = uint32 (unix_time_ms () - conn.TimestampEpoch)
        let peer_timestamp = be_bytes_to_uint32 (range_slice bytes 0 4)

        let data_slice = range_slice bytes 8 1528
        let peer_message = Handshake2 {
            InitTimestamp=peer_timestamp
            RandomData=data_slice
        }

        let user_data = conn.Callbacks.OnMessageReceived conn.UserData peer_message

        // Version handshake 5.2.2
        conn.Socket.Send([| 3uy |]) |> ignore

        // Initial handshake 5.2.3
        let server_random = random_bytes 1528
        conn.Socket.Send([| 0uy; 0uy; 0uy; 0uy; 0uy; 0uy; 0uy; 0uy |]) |> ignore
        conn.Socket.Send(server_random) |> ignore

        // Ack handshake 5.2.4
        let (ack_buffer: byte array) = Array.zeroCreate 1536
        uint32_to_be_bytes peer_timestamp (range_slice ack_buffer 0 4)
        uint32_to_be_bytes recv_timestamp (range_slice ack_buffer 4 4)
        data_slice.CopyTo(range_slice ack_buffer 8 1528)
        conn.Socket.Send(ack_buffer) |> ignore

        let conn = {conn with UserData=user_data}
        ack_handshake_state conn 0u server_random

/// <summary>
/// Waits for the initial handshake from the client and passes control onto the
/// secondary handshake state
/// </summary>
let client_handshake1_state (conn: ConnectionState<_>) =
    let (message_buffer: uint8 array) = Array.zeroCreate 1

    // Section 5.2.2
    match read_net_bytes conn.Socket message_buffer 1 conn.Timeout with
    | ReadError err ->
        conn.Callbacks.OnConnectionClose conn.UserData (sprintf "Read error during handshake: %s" err.Message)
        conn.Socket.Close()

    | PartialData _ ->
        conn.Callbacks.OnConnectionClose conn.UserData "Peer closed connection during handshake: %s"
        conn.Socket.Close()

    | FullData bytes ->
        let message = Handshake1 bytes.[0]
        let user_data = conn.Callbacks.OnMessageReceived conn.UserData message

        if bytes.[0] <> 3uy then
            conn.Callbacks.OnConnectionClose conn.UserData "Unsupported RTMP version requested by client"
            conn.Socket.Close()
        else
            let conn = {conn with UserData=user_data}
            client_handshake2_state conn

/// <summary>
/// Accepts a socket and manages the RTMP chunk session on it. Any messages which are received are
/// passed onto the ChunkEvents handler.
/// </summary>
let chunk_protocol (connection: Socket) (events: ChunkEvents<_>) =
    let user_data = events.InitialState ()
    let connection_state = {
        Streams=Map.empty
        Socket=connection
        Timeout=60 * 1000
        Callbacks=events
        UserData=user_data
        TimestampEpoch=unix_time_ms ()
    }

    client_handshake1_state connection_state

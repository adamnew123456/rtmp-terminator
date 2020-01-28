module ChunkProtocol

type Timestamp = uint32

/// <summary>
/// The type of the message header that follows the basic header.
/// The format of each is detailed on pp. 14-15.
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
/// The contents of the timestamp handshake
/// </summary>
type Handshake2Data = {
    InitTimestamp: Timestamp
    RandomData: uint8 array
}

/// <summary>
/// The contents of the ack handshake
/// </summary>
type Handshake3Data = {
    AckTimestamp: Timestamp
    ReadTimestamp: Timestamp
    AckRandomData: uint8 array
}

/// <summary>
/// The header data included in a chunk stream
/// packet
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
    | ChunkData of ChunkHeader * uint8 array

/// <summary>
/// The state of the buffer for a single RTMP chunk stream
/// </summary>
type ChunkStreamBuffer = {
    Buffer: uint8 array list
    Remaining: uint32
}

/// <summary>
/// The state used by the RTMP chunk processor
/// </summary>
type ChunkState = {
    Buffers: Map<uint32, ChunkStreamBuffer>
}

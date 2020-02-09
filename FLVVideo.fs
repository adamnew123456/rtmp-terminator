module FLVVideo
open Utils

/// <summary>
/// Basic state used while recording the FLV stream
/// </summary>
type FLVStream = {
    OutputStream: System.IO.Stream
    BaseTimestamp: uint32
    Buffer: System.ArraySegment<uint8>
}

/// <summary>
/// Initializes a new stream with the FLV header and metadata
/// </summary>
let init_stream (output: System.IO.Stream)
                (start_time: uint32)
                (metadata: AMFZero.ValueType) =
    let mutable (buffer: System.ArraySegment<uint8>) = full_slice (Array.zeroCreate (10 * 1024 * 1024))

    // FLV header
    buffer.[0] <- 0x46uy // 'FLV' magic
    buffer.[1] <- 0x4Cuy
    buffer.[2] <- 0x56uy
    buffer.[3] <- 1uy // Version 1
    buffer.[4] <- 5uy // Audio(4) | Video(1)
    uint32_to_be_bytes 9u (buffer.Slice(5))
    stream_send_bytes output (buffer.Slice(0, 9))

    // Initial metadata tag
    uint32_to_be_bytes 0u buffer // Previous tag size
    buffer.[4] <- 18uy // Script type
    // Would put the data size at slice 5 if we knew what it was
    uint24_to_be_bytes 0u (buffer.Slice(8)) // Timestamp base
    buffer.[11] <- 0uy // Timestamp extended
    uint24_to_be_bytes 0u (buffer.Slice(12)) // Stream ID

    let mutable metadata_start = buffer.Slice(15)
    let metadata_end = AMFZero.encode metadata_start (AMFZero.StringVal "onMetaData")
    let metadata_end = AMFZero.encode metadata_end metadata

    let metadata_size = metadata_end.Offset - metadata_start.Offset
    uint24_to_be_bytes (uint32 metadata_size) (buffer.Slice(5))

    uint32_to_be_bytes (uint32 (11 + metadata_size)) metadata_end
    stream_send_bytes output (buffer.Slice(0, metadata_end.Offset + 4))

    {OutputStream=output
     BaseTimestamp=start_time
     Buffer=buffer}

/// <summary>
/// Writes a new frame of FLV video
/// </summary>
let write_video_frame (stream: FLVStream)
                      (timestamp: uint32)
                      (data: System.ArraySegment<byte>) =
    let mutable buffer = stream.Buffer
    buffer.[0] <- 9uy // Video type
    uint24_to_be_bytes (uint32 data.Count) (buffer.Slice(1))

    let timestamp_rel = timestamp - stream.BaseTimestamp
    let timestamp_base = timestamp_rel &&& 0xFFFFFFu
    let timestamp_ext = uint8 ((timestamp_rel >>> 24) &&& 0xFFu)
    uint24_to_be_bytes timestamp_base (buffer.Slice(4))
    buffer.[7] <- timestamp_ext

    uint24_to_be_bytes 0u (buffer.Slice(8)) // Stream ID
    data.CopyTo(buffer.Slice(11))

    uint32_to_be_bytes (uint32 (11 + data.Count)) (buffer.Slice(11 + data.Count))
    stream_send_bytes stream.OutputStream (buffer.Slice(0, 11 + data.Count + 4))

/// <summary>
/// Writes a new frame of FLV audio
/// </summary>
let write_audio_frame (stream: FLVStream)
                      (timestamp: uint32)
                      (data: System.ArraySegment<byte>) =
    let mutable buffer = stream.Buffer
    buffer.[0] <- 8uy // Audio type
    uint24_to_be_bytes (uint32 data.Count) (buffer.Slice(1))

    let timestamp_rel = timestamp - stream.BaseTimestamp
    let timestamp_base = timestamp_rel &&& 0xFFFFFFu
    let timestamp_ext = uint8 ((timestamp_rel >>> 24) &&& 0xFFu)
    uint24_to_be_bytes timestamp_base (buffer.Slice(4))
    buffer.[7] <- timestamp_ext

    uint24_to_be_bytes 0u (buffer.Slice(8)) // Stream ID
    data.CopyTo(buffer.Slice(11))

    uint32_to_be_bytes (uint32 (11 + data.Count)) (buffer.Slice(11 + data.Count))
    stream_send_bytes stream.OutputStream (buffer.Slice(0, 11 + data.Count + 4))

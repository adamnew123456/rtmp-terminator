module FLVVideo
open Utils

/// <summary>
/// Basic state used while recording the FLV stream
/// </summary>
type FLVStream = {
    BaseTimestamp: uint32
    Buffer: System.ArraySegment<uint8>
}

/// <summary>
/// Initializes a new stream with the FLV header and metadata
/// </summary>
let init_stream (start_time: uint32)
                (metadata: AMFZero.ValueType)
                (buffer: System.ArraySegment<uint8>) =
    let mutable bufferm = buffer

    // FLV header
    bufferm.[0] <- 0x46uy // 'FLV' magic
    bufferm.[1] <- 0x4Cuy
    bufferm.[2] <- 0x56uy
    bufferm.[3] <- 1uy // Version 1
    bufferm.[4] <- 5uy // Audio(4) | Video(1)
    uint32_to_be_bytes 9u (bufferm.Slice(5))

    let mutable frame_start = bufferm.Slice(9)

    // Initial metadata tag
    uint32_to_be_bytes 0u frame_start // Previous tag size
    frame_start.[4] <- 18uy // Script type
    // Would put the data size at slice 5 if we knew what it was
    uint24_to_be_bytes 0u (frame_start.Slice(8)) // Timestamp base
    frame_start.[11] <- 0uy // Timestamp extended
    uint24_to_be_bytes 0u (frame_start.Slice(12)) // Stream ID

    let mutable metadata_start = frame_start.Slice(15)
    let metadata_end = AMFZero.encode metadata_start (AMFZero.StringVal "onMetaData")
    let metadata_end = AMFZero.encode metadata_end metadata

    let metadata_size = metadata_end.Offset - metadata_start.Offset
    uint24_to_be_bytes (uint32 metadata_size) (frame_start.Slice(5))

    uint32_to_be_bytes (uint32 (11 + metadata_size)) metadata_end

    (bufferm.Slice(0, metadata_end.Offset + 4),
     {BaseTimestamp=start_time; Buffer=bufferm})

/// <summary>
/// Writes a new frame of FLV video
/// </summary>
let write_video_frame (buffer: System.ArraySegment<uint8>) (data: System.ArraySegment<uint8>) =
    let mutable bufferm = buffer
    bufferm.[0] <- 9uy // Video type
    uint24_to_be_bytes (uint32 data.Count) (bufferm.Slice(1))

    // Should be patched up for each individual client
    (*
      let timestamp_rel = timestamp - stream.BaseTimestamp
      let timestamp_base = timestamp_rel &&& 0xFFFFFFu
      let timestamp_ext = uint8 ((timestamp_rel >>> 24) &&& 0xFFu)
      uint24_to_be_bytes timestamp_base (bufferm.Slice(4))
      bufferm.[7] <- timestamp_ext
    *)

    uint24_to_be_bytes 0u (bufferm.Slice(8)) // Stream ID
    data.CopyTo(bufferm.Slice(11))

    uint32_to_be_bytes (uint32 (11 + data.Count)) (bufferm.Slice(11 + data.Count))
    bufferm.Slice(0, 11 + data.Count + 4)

/// <summary>
/// Writes a new frame of FLV audio
/// </summary>
let write_audio_frame (buffer: System.ArraySegment<uint8>) (data: System.ArraySegment<uint8>) =
    let mutable bufferm = buffer
    bufferm.[0] <- 8uy // Audio type
    uint24_to_be_bytes (uint32 data.Count) (bufferm.Slice(1))

    // Should be patched up for each individual client
    (*
      let timestamp_rel = timestamp - stream.BaseTimestamp
      let timestamp_base = timestamp_rel &&& 0xFFFFFFu
      let timestamp_ext = uint8 ((timestamp_rel >>> 24) &&& 0xFFu)
      uint24_to_be_bytes timestamp_base (bufferm.Slice(4))
      bufferm.[7] <- timestamp_ext
    *)

    uint24_to_be_bytes 0u (bufferm.Slice(8)) // Stream ID
    data.CopyTo(bufferm.Slice(11))

    uint32_to_be_bytes (uint32 (11 + data.Count)) (buffer.Slice(11 + data.Count))
    bufferm.Slice(0, 11 + data.Count + 4)

/// <summary>
/// Updates the timestamp value for the current frame in the stream's shared
/// buffer
/// </summary>
let patch_frame_timestamp (stream: FLVStream) (timestamp: uint32) =
    let mutable buffer = stream.Buffer
    let timestamp_rel = timestamp - stream.BaseTimestamp
    let timestamp_base = timestamp_rel &&& 0xFFFFFFu
    let timestamp_ext = uint8 ((timestamp_rel >>> 24) &&& 0xFFu)
    uint24_to_be_bytes timestamp_base (buffer.Slice(4))
    buffer.[7] <- timestamp_ext

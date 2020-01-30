module Utils

open System.Net.Sockets

/// <summary>
/// The possible results of a socket read
/// </summary>
type NetReadResult =
    | FullData of uint8 array
    | PartialData of uint8 array * int
    | ReadError of SocketException

/// <summary>
/// A convenience wrapper for System.ArraySegment
/// </summary>
let full_slice (array: 'a array) =
    System.ArraySegment(array)

/// <summary>
/// A convenience wrapper for System.ArraySegment
/// </summary>
let range_slice (array: 'a array) (offset: int) (count: int) =
    System.ArraySegment(array, offset, count)

/// <summary>
/// Converts bytes into a big-endian 32-bit unsigned integer
/// </summary>
let be_bytes_to_uint32 (slice: System.ArraySegment<byte>) =
    let msb = uint32 slice.[0]
    let msb_1 = uint32 slice.[1]
    let msb_2 = uint32 slice.[2]
    let lsb = uint32 slice.[3]
    (msb <<< 24) + (msb_1 <<< 16) + (msb_2 <<< 8) + lsb

/// <summary>
/// Converts a 32-bit unsigned integer into big-endian bytes
/// </summary>
let uint32_to_be_bytes (value: uint32) (slice: System.ArraySegment<byte>) =
    slice.Array.[slice.Offset] <- uint8 <| (value <<< 24)
    slice.Array.[slice.Offset + 1] <- uint8 <| ((value <<< 16) &&& 0xFFu)
    slice.Array.[slice.Offset + 2] <- uint8 <| ((value <<< 8) &&& 0xFFu)
    slice.Array.[slice.Offset + 3] <- uint8 <| (value &&& 0xFFu)

/// <summary>
/// Generates a byte array containing random bytes
/// </summary>
let random_bytes (size: int) =
    let (array: uint8 array) = Array.zeroCreate size
    let rng = System.Random()
    rng.NextBytes(array)
    array

/// <summary>
/// Checks that the contents of two array slices are equal
/// </summary>
let slices_equal (a: System.ArraySegment<'t>) (b: System.ArraySegment<'t>) =
    if a.Count <> b.Count then
        false
    else
        a
        |> Seq.zip b
        |> Seq.forall (fun (aelt, belt) -> aelt = belt)

/// <summary>
/// Retrieves the current Unix timestamp
/// </summary>
let unix_time_ms () =
    uint64 (System.DateTimeOffset.Now.ToUnixTimeMilliseconds ())

/// <summary>
/// Generates a hex dump from a byte array
/// </summary>
let hexdump (data: System.ArraySegment<byte>) =
    let rec print_row (buffer: System.Text.StringBuilder) offset =
        let bytes =
            [offset..min (data.Count - 1) (offset + 7)]
            |> Seq.map (fun idx -> sprintf "%d" data.[idx])
            |> String.concat " "

        buffer.AppendLine(bytes) |> ignore
        if data.Count - offset >= 8 then
            print_row buffer (offset + 8)
        else
            buffer.ToString()

    print_row (System.Text.StringBuilder()) 0

/// <summary>
/// Reads exactly N bytes from a socket, possibly over multiple reads if not all
/// the data is available. If the data is not available within the given timeout
/// then this fails.
/// </summary>
let read_net_bytes (connection: Socket) (buffer: uint8 array) (size: int) (timeoutMs: int) =
    let segmentWrapper = System.Collections.Generic.List<System.ArraySegment<byte>>()
    segmentWrapper.Add(full_slice buffer)

    let rec receive_next_chunk read =
        segmentWrapper.[0] <- range_slice buffer read (size - read)
        try
          let chunkSize = connection.Receive(segmentWrapper)
          if chunkSize = 0 then
              PartialData (buffer, read + chunkSize)
          elif chunkSize + read = size then
              FullData buffer
          else
              receive_next_chunk (read + chunkSize)
        with
        | :? SocketException as err -> ReadError err

    connection.ReceiveTimeout <- timeoutMs
    receive_next_chunk 0

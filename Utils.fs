module Utils

open System.Net.Sockets

/// <summary>
/// Track the acknowledgment window, where there's a certain value that has a
/// maximum bound before it resets and some rollover action
/// occurs.
/// </summary>
type RolloverCounter = {
    Current: uint32
    Max: uint32
    TimesFilled: uint32
}

/// <summary>
/// Increments a bounded counter, returning a new counter as well as whether
/// or not the current window was exceeded.
/// </summary>
let incr_rcounter (counter: RolloverCounter) (amount: uint32) =
    let total = counter.Current + amount
    if total >= counter.Max then
        (true,
         {counter with
              Current=total % counter.Max
              TimesFilled=counter.TimesFilled + (total / counter.Max)})
     else
         (false, {counter with Current=total})

/// <summary>
/// Returns the total sequence number for a counter.
/// </summary>
let seq_rcounter (counter: RolloverCounter) =
    uint32 (counter.TimesFilled *  counter.Max) + counter.Current

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
/// Converts bytes into a big-endian 64-bit signed integer
/// </summary>
let be_bytes_to_int64 (slice: System.ArraySegment<byte>) =
    let msb = uint64 slice.[0]
    let msb_1 = uint64 slice.[1]
    let msb_2 = uint64 slice.[2]
    let msb_3 = uint64 slice.[3]
    let lsb_3 = uint64 slice.[4]
    let lsb_2 = uint64 slice.[5]
    let lsb_1 = uint64 slice.[6]
    let lsb = uint64 slice.[7]

    (msb <<< 56)
    + (msb_1 <<< 48)
    + (msb_2 <<< 40)
    + (msb_3 <<< 32)
    + (lsb_3 <<< 24)
    + (lsb_2 <<< 16)
    + (lsb_1 <<< 8)
    + lsb
    |> int64

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
/// Converts bytes into a big-endian 24-bit unsigned integer
/// </summary>
let be_bytes_to_uint24 (slice: System.ArraySegment<byte>) =
    let msb = uint32 slice.[0]
    let mid = uint32 slice.[1]
    let lsb = uint32 slice.[2]
    (msb <<< 16) + (mid <<< 8) + lsb

/// <summary>
/// Converts bytes into a big-endian 16-bit unsigned integer
/// </summary>
let be_bytes_to_uint16 (slice: System.ArraySegment<byte>) =
    let msb = uint16 slice.[0]
    let lsb = uint16 slice.[1]
    (msb <<< 8) + lsb

/// <summary>
/// Converts bytes into a big-endian 64-bit double
/// </summary>
let be_bytes_to_double (slice: System.ArraySegment<byte>) =
    System.BitConverter.Int64BitsToDouble(be_bytes_to_int64 slice)

/// <summary>
/// Converts a 64-bit unsigned integer into big-endian bytes
/// </summary>
let uint64_to_be_bytes (value: uint64) (slice: System.ArraySegment<byte>) =
    let mutable slicem = slice
    slicem.[0] <- uint8 <| ((value >>> 56) &&& 0xFFUL)
    slicem.[1] <- uint8 <| ((value >>> 48) &&& 0xFFUL)
    slicem.[2] <- uint8 <| ((value >>> 40) &&& 0xFFUL)
    slicem.[3] <- uint8 <| ((value >>> 32) &&& 0xFFUL)
    slicem.[4] <- uint8 <| ((value >>> 24) &&& 0xFFUL)
    slicem.[5] <- uint8 <| ((value >>> 16) &&& 0xFFUL)
    slicem.[6] <- uint8 <| ((value >>> 8) &&& 0xFFUL)
    slicem.[7] <- uint8 <| (value &&& 0xFFUL)

/// <summary>
/// Converts a 32-bit unsigned integer into big-endian bytes
/// </summary>
let uint32_to_be_bytes (value: uint32) (slice: System.ArraySegment<byte>) =
    let mutable slicem = slice
    slicem.[0] <- uint8 <| (value >>> 24)
    slicem.[1] <- uint8 <| ((value >>> 16) &&& 0xFFu)
    slicem.[2] <- uint8 <| ((value >>> 8) &&& 0xFFu)
    slicem.[3] <- uint8 <| (value &&& 0xFFu)

/// <summary>
/// Converts a 24-bit unsigned integer into big-endian bytes
/// </summary>
let uint24_to_be_bytes (value: uint32) (slice: System.ArraySegment<byte>) =
    let mutable slicem = slice
    slicem.[0] <- uint8 <| ((value <<< 16) &&& 0xFFu)
    slicem.[1] <- uint8 <| ((value <<< 8) &&& 0xFFu)
    slicem.[2] <- uint8 <| (value &&& 0xFFu)

/// <summary>
/// Converts a 24-bit unsigned integer into big-endian bytes
/// </summary>
let uint16_to_be_bytes (value: uint16) (slice: System.ArraySegment<byte>) =
    let mutable slicem = slice
    slicem.[0] <- uint8 <| ((value <<< 8) &&& 0xFFus)
    slicem.[1] <- uint8 <| (value &&& 0xFFus)

/// <summary>
/// Converts a 64-bit double into big-endian bytes
/// </summary>
let double_to_be_bytes (value: double) (slice: System.ArraySegment<byte>) =
    uint64_to_be_bytes (uint64 (System.BitConverter.DoubleToInt64Bits(value))) slice

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
/// Does an inverse map lookup on a value
/// </summary>
let inverse_find (value: 'b) (map: Map<'a, 'b>) =
    map
    |> Map.toSeq
    |> Seq.filter (fun (_, v) -> v = value)
    |> Seq.map fst
    |> Seq.head

/// <summary>
/// Retrieves the current Unix timestamp
/// </summary>
let unix_time_ms () =
    uint64 (System.DateTimeOffset.Now.ToUnixTimeMilliseconds ())

/// <summary>
/// Generates a hex dump from a byte array
/// </summary>
let hexdump (data: System.ArraySegment<byte>) =
    let width = 24

    let rec print_row (buffer: System.Text.StringBuilder) offset =
        let bytes =
            [offset..min (data.Count - 1) (offset + width - 1)]
            |> Seq.map (fun idx -> sprintf "%02x" data.[idx])
            |> String.concat " "

        buffer.AppendLine(bytes) |> ignore
        if data.Count - offset >= width then
            print_row buffer (offset + width)
        else
            buffer.ToString()

    print_row (System.Text.StringBuilder()) 0

/// <summary>
/// Reads exactly N bytes from a socket, possibly over multiple reads if not all
/// the data is available. If the data is not available within the given timeout
/// then this fails.
/// </summary>
let read_net_bytes (connection: Socket) (slice: System.ArraySegment<byte>) (timeoutMs: int) =
    let size = slice.Count
    let segmentWrapper = System.Collections.Generic.List<System.ArraySegment<byte>>()
    segmentWrapper.Add(slice)

    let rec receive_next_chunk read =
        try
          let chunkSize = connection.Receive(segmentWrapper)
          if chunkSize = 0 then
              Result.Error "Peer closed connection"
          elif chunkSize + read = size then
              Result.Ok slice
          else
              segmentWrapper.[0] <- segmentWrapper.[0].Slice(chunkSize)
              receive_next_chunk (read + chunkSize)
        with
        | :? SocketException as err ->
            Result.Error (sprintf "Read error: %s" err.Message)

    connection.ReceiveTimeout <- timeoutMs
    receive_next_chunk 0

/// <summary>
/// Sends an array slice over a socket
/// </summary>
let net_send_bytes (connection: Socket) (slice: System.ArraySegment<byte>) =
    connection.Send(slice.Array, slice.Offset, slice.Count, SocketFlags.None) |> ignore

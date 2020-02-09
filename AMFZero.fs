module AMFZero
open Utils

/// <summary>
/// All the values supported by AMF0
/// </summary>
type ValueType =
    | NumberVal of double
    | BooleanVal of bool
    | StringVal of string
    | ObjectVal of Map<string, ValueType>
    | NullVal
    | UndefinedVal
    | ReferenceVal of uint16
    | EcmaArrayVal of Map<string, ValueType>
    | StrictArrayVal of ValueType list
    | DateVal of System.DateTimeOffset
    | XmlVal of string
    | TypedObjectVal of string * Map<string, ValueType>

/// <summary>
/// Contains both a value as well as the references necessary to interpret it
/// </summary>
type ValueContext = {
    Value: ValueType
    Context: Map<uint16, ValueType>
}

/// <summary>
/// Simple markers indicate a primitive types without any nesting
/// </summary>
let simple_markers = Set.ofList [
    0uy; 1uy; 2uy; 5uy; 6uy; 7uy; 11uy; 12uy; 15uy
]

/// <summary>
/// Reads a short string from the input slice
/// </summary>
let parse_short_string (slice: System.ArraySegment<uint8>) =
    let length = be_bytes_to_uint16 slice
    let raw_str = System.Text.Encoding.UTF8.GetString(slice.Array, slice.Offset + 2, int32 length)
    (slice.Slice(int32 length + 2), raw_str)

/// <summary>
/// Parses out a simple object (one with a marker type in simple_markers) from a slice.
/// </summary>
let parse_simple (slice: System.ArraySegment<uint8>)  (marker: uint8) =
    match marker with
    | 0uy ->
        let raw_double = be_bytes_to_double slice
        (slice.Slice(8), NumberVal raw_double)

    | 1uy ->
        let flag = slice.[0]
        (slice.Slice(1), BooleanVal (flag > 0uy))

    | 2uy ->
        let (trailing, raw_str) = parse_short_string slice
        (trailing, StringVal raw_str)

    | 5uy ->
        (slice, NullVal)

    | 6uy ->
        (slice, UndefinedVal)

    | 7uy ->
        let ref = be_bytes_to_uint16 slice
        (slice.Slice(2), ReferenceVal ref)

    | 11uy ->
        let unix_ms = be_bytes_to_double slice
        let raw_timestamp = System.DateTimeOffset.FromUnixTimeMilliseconds(int64 unix_ms)
        // This also includes an int16 timezone which the spec says to ignore
        (slice.Slice(10), DateVal raw_timestamp)

    | 12uy ->
        let length = be_bytes_to_uint32 slice
        let raw_str = System.Text.Encoding.UTF8.GetString(slice.Array, slice.Offset + 4, int32 length)
        (slice.Slice(int32 length + 4), StringVal raw_str)

    | 15uy ->
        let length = be_bytes_to_uint32 slice
        let raw_str = System.Text.Encoding.UTF8.GetString(slice.Array, slice.Offset + 4, int32 length)
        (slice.Slice(int32 length + 4), XmlVal raw_str)

    | _ ->
        failwithf "Programming error: cannot decode type %d with parse_simple"
                  marker


/// <summary>
/// Parses an AMF0 format value out of the given byte range
/// </summary>
let rec parse_dispatch (slice: System.ArraySegment<uint8>) (refs: Map<uint16, ValueType>) =
    let marker = slice.[0]
    let remaining = slice.Slice(1)

    let register_ref_value refs value =
        let index = uint16 (Map.count refs)
        Map.add index value refs

    let rec parse_object slice refs fields =
        let (after_name, name) = parse_short_string slice
        if name = "" && after_name.[0] = 9uy then
            (after_name.Slice(1), refs, ObjectVal (Map.ofList fields))
        elif name = "" then
            failwithf "Input error: At byte %d, expected end-of-object marker"
                      (after_name.Offset)
        else
            let (after_value, refs, value) = parse_dispatch after_name refs
            parse_object after_value refs ((name, value) :: fields)

    let rec parse_strict_array slice refs count values =
        if count = 0u then
            (slice, refs, StrictArrayVal (List.rev values))
        else
            let (after_value, refs, value) = parse_dispatch slice refs
            parse_strict_array after_value refs (count - 1u) (value :: values)

    if Set.contains marker simple_markers then
        let (trailing, value) = parse_simple remaining marker
        (trailing, refs, value)
    else
        match marker with
        | 3uy ->
            let (after_obj, refs, value) = parse_object remaining refs []
            let refs = register_ref_value refs value
            (after_obj, refs, value)

        | 8uy ->
            // ECMA arrays are serialized almost the same way as objects, with the exception of a
            // leading uint32 count. They also need an end marker too though, so there's not much
            // point in relying on the count.
            let (after_array, refs, (ObjectVal inner_value)) =
                parse_object (remaining.Slice(4)) refs []

            let value = EcmaArrayVal inner_value
            let refs = register_ref_value refs value
            (after_array, refs, value)

        | 10uy ->
            let count = be_bytes_to_uint32 remaining
            let (after_array, refs, value) =
                parse_strict_array (remaining.Slice(4)) refs count []

            let refs = register_ref_value refs value
            (after_array, refs, value)

        | 16uy ->
            let (after_name, name) = parse_short_string remaining
            let (after_object, refs, (ObjectVal inner_value)) =
                parse_object after_name refs []

            let value = TypedObjectVal (name, inner_value)
            let refs = register_ref_value refs value
            (after_object, refs, value)

        | _ ->
            failwithf "Programming error: unsupported type flag %d" marker

/// <summary>
/// Builds a value context by parsing an AMF0 value out of the slice
/// </summary>
let parse (slice: System.ArraySegment<uint8>) =
    let (slice, refs, value) = parse_dispatch slice Map.empty
    (slice, {Value=value; Context=refs})

/// <summary>
/// Parses AMF0 values until the slice has been exhausted
/// </summary>
let parse_all (slice: System.ArraySegment<uint8>) (length: int) =
    let base_offset = slice.Offset
    let rec parse_one (slice: System.ArraySegment<uint8>) contexts =
        if slice.Offset - base_offset = length then
            List.rev contexts
        else
            let (slice, context) = parse slice
            parse_one slice (context :: contexts)

    parse_one slice []

/// <summary>
/// Encodes a short string without any type prefix
/// </summary>
let encode_short_string (slice: System.ArraySegment<uint8>) (value: string) =
    let mutable slicem = slice
    let bytes = System.Text.Encoding.UTF8.GetBytes(value)
    uint16_to_be_bytes (uint16 bytes.Length) slicem
    bytes.CopyTo(slicem.Array, slicem.Offset + 2)
    slicem.Slice(2 + bytes.Length)

/// <summary>
/// Encodes primitive AMF0 values into the slice
/// </summary>
let encode_simple (slice: System.ArraySegment<uint8>) (value: ValueType) =
    let mutable slicem = slice
    match value with
    | NumberVal value ->
        slicem.[0] <- 0uy
        double_to_be_bytes value (slicem.Slice(1))
        slicem.Slice(9)

    | BooleanVal value ->
        slicem.[0] <- 1uy
        slicem.[1] <- if value then 1uy else 0uy
        slicem.Slice(2)

    | StringVal value ->
        let bytesCount = System.Text.Encoding.UTF8.GetByteCount(value)
        if bytesCount > 65535 then
            let bytes = System.Text.Encoding.UTF8.GetBytes(value)
            slicem.[0] <- 12uy
            uint32_to_be_bytes (uint32 bytes.Length) (slicem.Slice(1))
            bytes.CopyTo(slicem.Array, slicem.Offset + 5)
            slicem.Slice(5 + bytes.Length)
        else
            slicem.[0] <- 2uy
            encode_short_string (slicem.Slice(1)) value

    | NullVal ->
        slicem.[0] <- 5uy
        slicem.Slice(1)

    | UndefinedVal ->
        slicem.[0] <- 6uy
        slicem.Slice(1)

    | ReferenceVal ref ->
        slicem.[0] <- 7uy
        uint16_to_be_bytes ref (slicem.Slice(1))
        slicem.Slice(3)

    | DateVal datetime ->
        let unix_ms = datetime.ToUnixTimeMilliseconds()
        slicem.[0] <- 11uy
        double_to_be_bytes (double unix_ms)  (slicem.Slice(1))
        slicem.[9] <- 0uy
        slicem.[10] <- 0uy
        slicem.Slice(11)

    | XmlVal xml ->
        let bytes = System.Text.Encoding.UTF8.GetBytes(xml)
        slicem.[0] <- 15uy
        uint32_to_be_bytes (uint32 bytes.Length) (slicem.Slice(1))
        bytes.CopyTo(slicem.Array, slicem.Offset + 5)
        slicem.Slice(5 + bytes.Length)

    | _ ->
        failwithf "Programming error: Cannot encode value %A with encode_simple"
                  value

/// <summary>
/// Encodes an AMF0 message into the given buffer
/// </summary>
let rec encode (slice: System.ArraySegment<uint8>) (value: ValueType) =
    let mutable slicem = slice

    let rec encode_object slice fields =
        let mutable slicem = slice
        match fields with
        | [] ->
            uint16_to_be_bytes 0us slice
            slicem.[2] <- 9uy
            slicem.Slice(3)

        | (name, value) :: rest ->
            let after_name = encode_short_string slicem name
            let after_value = encode after_name value
            encode_object after_value rest

    let rec encode_array slice values =
        let mutable slicem = slice
        match values with
        | [] ->
            slicem

        | head :: tail ->
            let after_head = encode slice head
            encode_array after_head tail

    match value with
    | ObjectVal fields ->
        slicem.[0] <- 3uy
        encode_object (slice.Slice(1)) (Map.toList fields)

    | EcmaArrayVal fields ->
        slicem.[0] <- 8uy
        uint32_to_be_bytes (uint32 (Map.count fields)) (slicem.Slice(1))
        encode_object (slicem.Slice(5)) (Map.toList fields)

    | StrictArrayVal values ->
        slicem.[0] <- 10uy
        uint32_to_be_bytes (uint32 (List.length values)) (slicem.Slice(1))
        encode_array (slicem.Slice(5)) values

    | TypedObjectVal (name, fields) ->
        slicem.[0] <- 16uy
        let after_name = encode_short_string (slicem.Slice(1)) name
        encode_object after_name (Map.toList fields)

    | _ ->
        encode_simple slice value

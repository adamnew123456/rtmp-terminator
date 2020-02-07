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

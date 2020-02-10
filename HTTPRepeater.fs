module HTTPRepeater
open System.Net.Sockets

/// <summary>
/// The types of events that the HTTP repeater can process
/// </summary>
type Events =
    | Started
    | MetadataReceived of AMFZero.ValueType
    | VideoReceived of byte array
    | AudioReceived of byte array
    | NewConnection of Socket
    | Stopped

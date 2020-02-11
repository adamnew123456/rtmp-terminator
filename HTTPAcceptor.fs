module HTTPAcceptor
open System.Collections.Concurrent
open System.Net.Sockets

/// <summary>
/// The types of events that the HTTP acceptor can process
/// </summary>
type Events =
    | Started
    | Stopped

/// <summary>
/// Waits for HTTP connections and passes them onto the repeater when they have been
/// initialized
/// </summary>
let runner (server: Socket)
           (mailbox: BlockingCollection<Events>)
           (repeater_mbox: BlockingCollection<HTTPRepeater.Events>) =
    let not_streaming_error =
        System.Text.Encoding.UTF8.GetBytes("HTTP/1.0 400 Error\r\nConnection: close\r\nContent-Type: text/plain\r\n\r\nThe stream is currently not active, please check back later")

    let streaming_response =
        System.Text.Encoding.UTF8.GetBytes("HTTP/1.0 200 OK\r\nConnection: close\r\nContent-Type: video/x-flv\r\n\r\n")

    let accept_client started =
        let client = server.Accept()
        if not started then
            eprintf "New client on %A: Sending not-streaming error" client.RemoteEndPoint
            client.Send(not_streaming_error) |> ignore
            client.Close()
        else
            eprintf "New client on %A: Sending OK" client.RemoteEndPoint
            client.Send(streaming_response) |> ignore
            repeater_mbox.Add(HTTPRepeater.NewConnection client)

    let rec step started (accept_list: System.Collections.IList) =
        accept_list.Clear()
        accept_list.Add(server) |> ignore
        Socket.Select(accept_list, null, null, 50)

        if accept_list.Count = 1 then
            accept_client started

        let mutable message = Started
        if mailbox.TryTake(&message, 50) then
            match message with
            | Started ->
                step true accept_list

            | Stopped ->
                server.Close()
        else
            step started accept_list

    step false (System.Collections.ArrayList())

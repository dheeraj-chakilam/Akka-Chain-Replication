module ChatServer.Network

open System.Text
open System.Net
open Akka.FSharp
open Akka.IO
open World
open System.Diagnostics

type Server =
    | ChatServer
    | MasterServer

let handler world serverType selfID connection (mailbox: Actor<obj>) =  
    let rec loop connection = actor {
        let! msg = mailbox.Receive()

        match msg with
        | :? Tcp.Received as received ->
            //In case we receive multiple messages (delimited by a newline) in the same Tcp.Received message
            let lines = (Encoding.ASCII.GetString (received.Data.ToArray())).Trim().Split([|'\n'|])
            Array.iter (fun (line:string) ->
                let data = line.Split([|' '|], 2)

                match data with
                | [| "heartbeat"; message |] ->
                    world <! Heartbeat (int64 (message.Trim()), mailbox.Self)
                
                | [| "add"; message |] ->
                    match message.Trim().Split([|' '|], 2) with
                    | [| name; url |] -> world <! Request (Add (name, url))
                    | _ -> connection <! Tcp.Write.Create (ByteString.FromString <| sprintf "Invalid request. (%A)\n" data)                
                
                | [| "delete"; message |] ->
                    world <! Request (Delete (message.Trim()))

                | [| "ackadd"; message |] ->
                    match message.Trim().Split([|' '|], 2) with
                    | [| name; url |] -> world <! Ack (Add (name, url))
                    | _ -> connection <! Tcp.Write.Create (ByteString.FromString <| sprintf "Invalid request. (%A)\n" data)                
                
                | [| "ackdelete"; message |] ->
                    world <! Ack (Delete (message.Trim()))
                
                | [| "addreplica" |] ->
                    world <! AddReplica
                
                | [| "updatehead"; message |] ->
                    world <! UpdateHead message

                | [| "updatehead" |] ->
                    world <! UpdateHead ""
            
                | [| "get"; message |] ->
                    world <! GetSong (message.Trim())

                | [| "snapshot" |] ->
                    world <! GetSnapshot
                
                | [| "crash" |] ->
                    System.Environment.Exit(0)

                | [| "crashAfterReceiving" |] ->
                    world <! Crash AfterReceive
                
                | [| "crashAfterSending" |] ->
                    world <! Crash AfterSend

                | _ ->
                    connection <! Tcp.Write.Create (ByteString.FromString <| sprintf "Invalid request. (%A)\n" data)) lines
    
        | :? Tcp.ConnectionClosed as closed ->
            mailbox.Context.Stop mailbox.Self

        | :? string as response ->
            connection <! Tcp.Write.Create (ByteString.FromString (response + "\n"))

        | _ -> mailbox.Unhandled()

        return! loop connection
    }

    match serverType with
    | ChatServer -> world <! Join mailbox.Self
    | MasterServer -> world <! JoinMaster mailbox.Self
    
    loop connection

let server world serverType port selfID (mailbox: Actor<obj>) =
    let rec loop() = actor {
        let! msg = mailbox.Receive()
        let sender = mailbox.Sender()
        
        match msg with
        | :? Tcp.Bound as bound ->
            printf "Listening on %O\n" bound.LocalAddress

        | :? Tcp.Connected as connected -> 
            printf "%O connected to the server\n" connected.RemoteAddress
            let handlerName = "handler_" + connected.RemoteAddress.ToString().Replace("[", "").Replace("]", "")
            let handlerRef = spawn mailbox handlerName (handler world serverType selfID sender)
            sender <! Tcp.Register handlerRef

        | _ -> mailbox.Unhandled()

        return! loop()
    }

    mailbox.Context.System.Tcp() <! Tcp.Bind(mailbox.Self, IPEndPoint(IPAddress.Any, port),options=[Inet.SO.ReuseAddress(true)])

    if serverType = ChatServer then
        let clientPortList = seq {0 .. (selfID-1)} |> Seq.rev |> Seq.map (fun n -> 20000 + n)
        for p in clientPortList do
            mailbox.Context.System.Tcp() <! Tcp.Connect(IPEndPoint(IPAddress.Loopback, p),options=[Inet.SO.ReuseAddress(true)])

    loop()
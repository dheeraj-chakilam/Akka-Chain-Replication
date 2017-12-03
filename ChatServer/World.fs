module ChatServer.World

open Akka.FSharp
open Akka.Actor

type Update =
    | Add of string * string
    | Delete of string

type RoomState = {
    master: IActorRef option
    songList: Map<string,string>
    beatmap: Map<int64,IActorRef*int64>
    predecessor: IActorRef option
    successor: IActorRef option
    speculativeLog: Update list
    stableLog: Update list
}


type RoomMsg =
    | Join of IActorRef
    | JoinMaster of IActorRef
    | Heartbeat of int64 * IActorRef
    //| CheckLinks
    | InitializeHead
    | AddReplica
    | UpdateHead of string
    | Request of Update
    | Ack of Update
    | GetSong of string
    | GetSnapshot


let scheduleRepeatedly (sender:Actor<_>) rate actorRef message =
    sender.Context.System.Scheduler.ScheduleTellRepeatedlyCancelable(
        System.TimeSpan.FromMilliseconds 0.,
        System.TimeSpan.FromMilliseconds rate,
        actorRef,
        message,
        sender.Self)

let scheduleOnce (sender:Actor<_>) after actorRef message =
    sender.Context.System.Scheduler.ScheduleTellOnceCancelable(
        System.TimeSpan.FromMilliseconds (float after),
        actorRef,
        message,
        sender.Self)


let sw =
    let sw = System.Diagnostics.Stopwatch()
    sw.Start()
    sw

let filterAlive aliveThreshold map =
    map
    |> Map.filter (fun _ (_,ms) -> (sw.ElapsedMilliseconds - ms) < aliveThreshold)

// Get a map (id, ref) of all the alive processes
let getAliveMap aliveThreshold state =
    state.beatmap
    |> filterAlive aliveThreshold
    |> Map.map (fun id (ref,_) -> ref)
        
let getSuccessor selfID aliveThreshold state =
    // TODO: Move speculative history to stable and propogate it if new tail
    let (_, maxRef) =
        getAliveMap aliveThreshold state
        |> Map.filter (fun id _ -> id < selfID)
        |> Map.fold (fun (maxID, maxRef) id ref -> if id > maxID then (id, Some ref) else (maxID, maxRef)) (-1L, None)
    maxRef

let getPredecessor aliveThreshold selfID state =
    let (_, minRef) =
        getAliveMap aliveThreshold state
        |> Map.filter (fun id _ -> id > selfID)
        |> Map.fold (fun (minID, minRef) id ref -> if id < minID then (id, Some ref) else (minID, minRef)) (System.Int64.MaxValue, None)
    minRef

let getID ref state =
    state.beatmap
    |> Map.findKey (fun _ (ref', _) -> ref' = ref)

let stringifyLog log =
    log
    |> List.rev
    |> List.toSeq
    |> Seq.map (fun update -> 
        match update with
        | Add (name, url) -> 
            sprintf "<add:%s:%s>" name url
        | Delete name ->
            sprintf "<delete:%s>" name )
    |> String.concat ""

let stringifyLog2 log =
    log
    |> List.rev
    |> List.toSeq
    |> Seq.map (fun update -> 
        match update with
        | Add (name, url) -> 
            sprintf "add|%s|%s" name url
        | Delete name ->
            sprintf "delete|%s" name )
    |> String.concat " "


let room selfID beatrate aliveThreshold (mailbox: Actor<RoomMsg>) =
    let rec loop state = actor {

        let! msg = mailbox.Receive()
        let sender = mailbox.Sender()
        
        let state =
            { state with successor = getSuccessor selfID aliveThreshold state ; predecessor = getPredecessor selfID aliveThreshold state }

        match msg with
        | Join ref ->
            scheduleRepeatedly mailbox beatrate ref (sprintf "heartbeat %i" selfID) |> ignore
            return! loop state

        | JoinMaster ref ->
            return! loop { state with master = Some ref }

        | Heartbeat (id, ref) ->
            return! loop { state with beatmap = state.beatmap |> Map.add id (ref,sw.ElapsedMilliseconds) }

        | InitializeHead ->
            Option.iter (fun r -> r <! "addreplica") state.successor
            //scheduleRepeatedly mailbox aliveThreshold mailbox.Self CheckLinks |> ignore
            return! loop state

        //| CheckLinks ->
        //    let successor =
        //        match state.successor with
        //        | Some ref ->
        //            if (getAliveMap state |> Map.exists (fun _ ref' -> ref' = ref)) then
        //                state.successor
        //            else
        //                getSuccessor state
        //        | None -> state.successor

        //    let predecessor =
        //        match state.predecessor with
        //        | Some ref ->
        //            if (getAliveMap state |> Map.exists (fun _ ref' -> ref' = ref)) then
        //                state.predecessor
        //            else
        //                getPredecessor state
        //        | None -> state.predecessor

        //    return! loop { state with successor = successor ; predecessor = predecessor }
        
        | AddReplica ->
            match state.successor with
            | Some ref -> ref <! "addreplica"
            | None -> Option.iter (fun r -> r <! (sprintf "updatehead %s" (stringifyLog2 state.stableLog))) state.predecessor
            return! loop state
        
        | UpdateHead logString ->
            match state.predecessor with
            | Some ref ->
                ref <! (sprintf "updatehead %s" logString)
                return! loop state
            | None ->
                let (songList, log) =
                    logString.Trim().Split([|' '|])
                    |> Array.fold (fun (songList, log) updateString ->
                        match updateString.Trim().Split([|'|'|]) with
                        | [| "add"; name; url |] -> (Map.add name url songList, (Add (name, url))::log)
                        | [| "delete"; name |] -> (Map.remove name songList, (Delete name)::log)
                        | [| "" |] -> (songList, log)
                        | _ -> failwith "ERROR: Incorrect UpdateHead logstring") (Map.empty, [])
                return! loop { state with songList = songList ; speculativeLog = log ; stableLog = log }

        | Request update ->
            let (songList, stableLog) =
                match update with
                | (Add (name, url)) ->
                    //TODO: Add speculative and stable log
                    match state.successor with
                    | Some ref ->
                        ref <! (sprintf "add %s %s" name url)
                        (state.songList, state.stableLog)
                    | None ->
                        Option.iter (fun r -> r <! (sprintf "ackadd %s %s" name url)) state.predecessor
                        (Map.add name url state.songList, update::state.stableLog)

                | Delete name ->
                    //TODO: Add speculative and stable log
                    match state.successor with
                    | Some ref ->
                        ref <! (sprintf "delete %s" name)
                        (state.songList, state.stableLog)
                    | None ->
                        Option.iter (fun r -> r <! (sprintf "ackdelete %s" name )) state.predecessor
                        (Map.remove name state.songList, update::state.stableLog)
            
            return! loop { state with songList = songList ; speculativeLog = update::state.speculativeLog ; stableLog = stableLog }

        | Ack update ->
            let songList =
                match update with
                | (Add (name, url)) ->
                    match state.predecessor with
                    | Some ref -> ref <! (sprintf "ackadd %s %s" name url) 
                    | None -> Option.iter (fun m -> m <! "ack commit") state.master
                    Map.add name url state.songList

                | Delete name ->
                    match state.predecessor with
                    | Some ref -> ref <! (sprintf "ackdelete %s" name )
                    | None -> Option.iter (fun m -> m <! "ack commit") state.master
                    Map.remove name state.songList
            
            return! loop { state with songList = songList ; stableLog = update::state.stableLog }

        | GetSong name ->
            match state.master with
            | Some m ->
                state.songList
                |> Map.tryFind name
                |> Option.defaultValue "NONE"
                |> (fun songName -> m <! (sprintf "resp %s" songName))
            | None -> ()
            return! loop state
        
        | GetSnapshot ->
            Option.iter (fun ref -> ref <! sprintf "<%s><%s>" (stringifyLog state.speculativeLog) (stringifyLog state.stableLog)) state.master
            Option.iter (fun ref -> ref <! "snapshot") state.successor
            return! loop state
    }

    scheduleOnce mailbox 1500L mailbox.Self InitializeHead |> ignore

    loop {
        master = None
        beatmap = Map.empty
        songList = Map.empty
        predecessor = None
        successor = None
        speculativeLog = []
        stableLog = []
        }
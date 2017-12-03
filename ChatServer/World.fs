module ChatServer.World

open Akka.FSharp
open Akka.Actor

type RoomState = {
    master: IActorRef option
    songList: Map<string,string>
    beatmap: Map<int64,IActorRef*int64>
    predecessor: IActorRef option
    successor: IActorRef option
}

type RoomMsg =
    | Join of IActorRef
    | JoinMaster of IActorRef
    | Heartbeat of int64 * IActorRef
    //| CheckLinks
    | InitializeHead
    | AddReplica
    | AddSong of string * string
    | DeleteSong of string
    | AckAddSong of string * string
    | AckDeleteSong of string
    | GetSong of string


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

let room selfID beatrate aliveThreshold (mailbox: Actor<RoomMsg>) =
    let rec loop state = actor {

        let filterAlive map =
            map
            |> Map.filter (fun _ (_,ms) -> (sw.ElapsedMilliseconds - ms) < aliveThreshold)

        // Get a map (id, ref) of all the alive processes
        let getAliveMap state =
            state.beatmap
            |> filterAlive
            |> Map.map (fun id (ref,_) -> ref)
        
        let getSuccessor state =
            let (_, maxRef) =
                getAliveMap state
                |> Map.filter (fun id _ -> id < selfID)
                |> Map.fold (fun (maxID, maxRef) id ref -> if id > maxID then (id, Some ref) else (maxID, maxRef)) (-1L, None)
            maxRef

        let getPredecessor state =
            let (_, minRef) =
                getAliveMap state
                |> Map.filter (fun id _ -> id > selfID)
                |> Map.fold (fun (minID, minRef) id ref -> if id < minID then (id, Some ref) else (minID, minRef)) (System.Int64.MaxValue, None)
            minRef

        let getID ref state =
            state.beatmap
            |> Map.findKey (fun _ (ref', _) -> ref' = ref)

        let! msg = mailbox.Receive()
        let sender = mailbox.Sender()
        
        let state =
            { state with successor = getSuccessor state ; predecessor = getPredecessor state }

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
            | None ->
                ()
                //TODO:Option.iter (fun r -> r <! "updatehead") state.predecessor
            
            return! loop state
        

        | AddSong (name, url) ->
            //TODO: Add speculative and stable log

            let songList =
                match state.successor with
                | Some ref ->
                    ref <! (sprintf "add %s %s" name url)
                    state.songList
                | None ->
                    Option.iter (fun r -> r <! (sprintf "ackadd %s %s" name url)) state.predecessor
                    Map.add name url state.songList

            return! loop { state with songList = songList }

        | DeleteSong name ->
            //TODO: Add speculative and stable log

            let songList =
                match state.successor with
                | Some ref ->
                    ref <! (sprintf "delete %s" name)
                    state.songList
                | None ->
                    Option.iter (fun r -> r <! (sprintf "ackdelete %s" name )) state.predecessor
                    Map.remove name state.songList
            
            return! loop { state with songList = songList }

        | AckAddSong (name, url) ->
            match state.predecessor with
            | Some ref -> ref <! (sprintf "ackadd %s %s" name url) 
            | None -> Option.iter (fun m -> m <! "ack commit") state.master
            return! loop { state with songList = Map.add name url state.songList }

        | AckDeleteSong name ->
            match state.predecessor with
            | Some ref -> ref <! (sprintf "ackdelete %s" name )
            | None -> Option.iter (fun m -> m <! "ack commit") state.master
            return! loop { state with songList = Map.remove name state.songList }

        | GetSong name ->
            match state.master with
            | Some m ->
                state.songList
                |> Map.tryFind name
                |> Option.defaultValue "NONE"
                |> (fun songName -> m <! (sprintf "resp %s" songName))
            | None -> ()
            return! loop state
    }

    scheduleOnce mailbox 1500L mailbox.Self InitializeHead |> ignore

    loop { master = None ; beatmap = Map.empty ; songList = Map.empty ; predecessor = None ; successor = None }
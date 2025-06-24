open System
open Deedle
open Akka.FSharp


///////////////////////
// Utility functions //
///////////////////////

// For both 2D and imperfect 2D grids, round the number of nodes to obtain a perfect square.
let roundNodes numNodes topology =
    /// If the topology is "2d" or "imperfect2d", returns the nearest perfect square number that is less than or equal to numNodes.
    /// If the topology is "3d" or "imperfect3d", returns the nearest perfect cube number that is less than or equal to numNodes.
    /// For any other topology, returns numNodes as is.
    match topology with
    | "2d"
    | "imperfect2d" -> Math.Pow (Math.Round (sqrt (float numNodes)), 2.0) |> int
    | "3d"
    | "imperfect3d" -> Math.Pow (Math.Round ((float numNodes) ** (1.0 / 3.0)), 3.0)  |> int
    | _ -> numNodes


/// Select random element from a list
let selectRandom (l: List<_>) =
    /// The ID of a random neighbor node.
    let r = Random()
    /// Selects a random element from a list.
    l.[r.Next(l.Length)]
// selectRandom([1; 2; 3; 4; 5; 6; 7; 8; 9; 10])

let getRandomNeighborID (topologyMap: Map<_, _>) nodeID =
    /// The ID of a random neighbor node.
    let (neighborList: List<_>) = (topologyMap.TryFind nodeID).Value
    /// Selects a random element from a list.
    let random = Random()
    neighborList.[random.Next(neighborList.Length)]


//////////////////////////
// Different topologies //
//////////////////////////

let buildLineTopology numNodes =
/// Builds a line topology.
    let mutable map = Map.empty
    [ 1 .. numNodes ]
    |> List.map (fun nodeID ->
    /// The list of neighbors of a node.
        let listNeighbors = List.filter (fun y -> (y = nodeID + 1 || y = nodeID - 1)) [ 1 .. numNodes ]
        map <- map.Add(nodeID, listNeighbors))
    |> ignore
    map

// Find neighbors of any particular node in a 2D grid
let gridNeighbors2D nodeID numNodes =
/// Finds the neighbors of any particular node in a 2D grid.
    let mutable map = Map.empty
    let lenSide = sqrt (float numNodes) |> int
    [ 1 .. numNodes ]
    |> List.filter (fun y ->
        /// The list of neighbors of a node.
        if (nodeID % lenSide = 0) then (y = nodeID - 1 || y = nodeID - lenSide || y = nodeID + lenSide)
        elif (nodeID % lenSide = 1) then (y = nodeID + 1 || y = nodeID - lenSide || y = nodeID + lenSide)
        else (y = nodeID - 1 || y = nodeID + 1 || y = nodeID - lenSide || y = nodeID + lenSide))
// gridNeighbors2D 5 9

let build2DTopology numNodes =
/// Builds a 2D topology.
    let mutable map = Map.empty
    [ 1 .. numNodes ]
    |> List.map (fun nodeID ->
    /// The list of neighbors of a node.
        let listNeighbors = gridNeighbors2D nodeID numNodes
        map <- map.Add(nodeID, listNeighbors))
    |> ignore
    map

let buildImperfect2DTopology numNodes =
/// Builds an imperfect 2D topology.
    let mutable map = Map.empty
    [ 1 .. numNodes ]
    |> List.map (fun nodeID ->
    /// The list of neighbors of a node.
        let mutable listNeighbors = gridNeighbors2D nodeID numNodes
        let random =
            [ 1 .. numNodes ]
            |> List.filter (fun m -> m <> nodeID && not (listNeighbors |> List.contains m))
            |> selectRandom
        let listNeighbors = random :: listNeighbors
        map <- map.Add(nodeID, listNeighbors))
    |> ignore
    map

let gridNeighbors3D nodeID numNodes =
/// Finds the neighbors of any particular node in a 3D grid.
    let lenSide = Math.Round(Math.Pow((float numNodes), (1.0 / 3.0))) |> int
    [ 1 .. numNodes ]
    |> List.filter (fun y ->
    /// The list of neighbors of a node.
        if (nodeID % lenSide = 0) then
            if (nodeID % (int (float (lenSide) ** 2.0)) = 0) then
                (y = nodeID - 1 || y = nodeID - lenSide || y = nodeID - int ((float (lenSide) ** 2.0)) || y = nodeID + int ((float (lenSide) ** 2.0)))
            elif (nodeID % (int (float (lenSide) ** 2.0)) = lenSide) then
                (y = nodeID - 1 || y = nodeID + lenSide || y = nodeID - int ((float (lenSide) ** 2.0)) || y = nodeID + int ((float (lenSide) ** 2.0)))
            else
                (y = nodeID - 1 || y = nodeID - lenSide || y = nodeID + lenSide || y = nodeID - int ((float (lenSide) ** 2.0)) || y = nodeID + int ((float (lenSide) ** 2.0)))        
        elif (nodeID % lenSide = 1) then
            if (nodeID % (int (float (lenSide) ** 2.0)) = 1) then
                (y = nodeID + 1 || y = nodeID + lenSide || y = nodeID - int ((float (lenSide) ** 2.0)) || y = nodeID + int ((float (lenSide) ** 2.0)))
            elif (nodeID % (int (float (lenSide) ** 2.0)) = int (float (lenSide) ** 2.0) - lenSide + 1 ) then
                (y = nodeID + 1 || y = nodeID - lenSide || y = nodeID - int ((float (lenSide) ** 2.0)) || y = nodeID + int ((float (lenSide) ** 2.0)))
            else
                (y = nodeID + 1 || y = nodeID - lenSide || y = nodeID + lenSide || y = nodeID - int ((float (lenSide) ** 2.0)) || y = nodeID + int ((float (lenSide) ** 2.0)))
        elif (nodeID % (int (float (lenSide) ** 2.0)) > 1) && (nodeID % (int (float (lenSide) ** 2.0)) < lenSide) then
            (y = nodeID - 1 || y = nodeID + 1 || y = nodeID + lenSide || y = nodeID - int ((float (lenSide) ** 2.0)) || y = nodeID + int ((float (lenSide) ** 2.0)))
        elif (nodeID % (int (float (lenSide) ** 2.0)) > int (float (lenSide) ** 2.0) - lenSide + 1) && (nodeID % (int (float (lenSide) ** 2.0)) < (int (float (lenSide) ** 2.0))) then
            (y = nodeID - 1 || y = nodeID + 1 || y = nodeID - lenSide || y = nodeID - int ((float (lenSide) ** 2.0)) || y = nodeID + int ((float (lenSide) ** 2.0)))
        else
            (y = nodeID - 1 || y = nodeID + 1 || y = nodeID - lenSide || y = nodeID + lenSide || y = nodeID - int ((float (lenSide) ** 2.0)) || y = nodeID + int ((float (lenSide) ** 2.0))))
/// gridNeighbors3D 5 9

/// Initializes an empty map and starts the process of building a 3D topology for a given number of nodes.
let build3DTopology numNodes =
/// Builds a 3D topology.
    let mutable map = Map.empty
    [ 1 .. numNodes ]
    |> List.map (fun nodeID ->
    /// The list of neighbors of a node.
        let listNeighbors = gridNeighbors3D nodeID numNodes
        map <- map.Add(nodeID, listNeighbors))
    |> ignore
    map

let buildImperfect3DTopology numNodes =
/// Builds an imperfect 3D topology.
    let mutable map = Map.empty
    [ 1 .. numNodes ]
    |> List.map (fun nodeID ->
    /// The list of neighbors of a node.
        let mutable listNeighbors = gridNeighbors3D nodeID numNodes
        let random =
            [ 1 .. numNodes ]
            |> List.filter (fun m -> m <> nodeID && not (listNeighbors |> List.contains m))
            |> selectRandom
        let listNeighbors = random :: listNeighbors
        map <- map.Add(nodeID, listNeighbors))
    |> ignore
    map

let buildFullTopology numNodes =
/// Builds a full topology.
    let mutable map = Map.empty
    [ 1 .. numNodes ]
    |> List.map (fun nodeID ->
    /// The list of neighbors of a node.
        let listNeighbors = List.filter (fun y -> nodeID <> y) [ 1 .. numNodes ]
        map <- map.Add(nodeID, listNeighbors))
    |> ignore
    map

let buildTopology numNodes topology =
/// Builds a topology based on user input.
    let mutable map = Map.empty
    match topology with
    | "line" -> buildLineTopology numNodes
    | "2d" -> build2DTopology numNodes
    | "imperfect2d" -> buildImperfect2DTopology numNodes
    | "3d" -> build3DTopology numNodes
    | "imperfect3d" -> buildImperfect3DTopology numNodes
    | "full" -> buildFullTopology numNodes

///////////////////
// Counter Actor //
///////////////////

type CounterMessage =
/// The message sent to the counter actor.
    | GossipNodeConverge
    | PushSumNodeConverge of int * float

/// The result of the algorithm.
type Result = { NumberOfNodesConverged: int; TimeElapsed: int64; }

/// The counter actor.
let counter initialCount numNodes (filepath: string) (stopWatch: Diagnostics.Stopwatch) (mailbox: Actor<'a>) =
    let rec loop count (dataframeList: Result list) =
        actor {
            let! message = mailbox.Receive()
            /// Handle message here
            match message with
            | GossipNodeConverge ->
                printfn "[INFO] Number of nodes converged: %d" (count + 1)
                let newRecord = { NumberOfNodesConverged = count + 1; TimeElapsed = stopWatch.ElapsedMilliseconds; }
                if (count + 1 = numNodes) then
                /// If all the nodes have converged, stop the timer and save the results to a CSV file.
                    stopWatch.Stop()
                    printfn "[INFO] Gossip Algorithm has converged in %d ms" stopWatch.ElapsedMilliseconds
                    let dataframe = Frame.ofRecords (List.append dataframeList [newRecord])
                    dataframe.SaveCsv(filepath, separator='\t')
                    mailbox.Context.System.Terminate() |> ignore
                return! loop (count + 1) (List.append dataframeList [newRecord])
            | PushSumNodeConverge (nodeID, avg) ->
            /// If a node has converged, stop the timer and save the results to a CSV file.
                printfn "[INFO] Node %d has been converged s/w=%f)" nodeID avg
                let newRecord = { NumberOfNodesConverged = count + 1; TimeElapsed = stopWatch.ElapsedMilliseconds }
                if (count + 1 = numNodes) then
                /// If all the nodes have converged, stop the timer and save the results to a CSV file.
                    stopWatch.Stop()
                    printfn "[INFO] Push Sum Algorithm has converged in %d ms" stopWatch.ElapsedMilliseconds                                       
                    let dataframe = Frame.ofRecords (List.append dataframeList [newRecord])
                    dataframe.SaveCsv(filepath, separator='\t')
                    mailbox.Context.System.Terminate() |> ignore
                return! loop (count + 1) (List.append dataframeList [newRecord])
        }
    loop initialCount []


//////////////////
// Gossip Actor //
//////////////////

/// The gossip actor.
let gossip maxCount (topologyMap: Map<_, _>) nodeID counterRef (mailbox: Actor<_>) = 
    let rec loop (count: int) = actor {
        /// The number of times a node has heard the rumor.
        let! message = mailbox.Receive ()
        // Handle message here
        match message with
        | "heardRumor" ->
            // If the heard rumor count is zero, tell the counter that it has heard the rumor and start spreading it.
            // Else, increment the heard rumor count by 1
            if count = 0 then
                mailbox.Context.System.Scheduler.ScheduleTellOnce(
                    TimeSpan.FromMilliseconds(25.0),
                    mailbox.Self,
                    "spreadRumor"
                )
                // printfn "[INFO] Node %d has been converged" nodeID
                counterRef <! GossipNodeConverge
                return! loop (count + 1)
            else
                return! loop (count + 1)
        | "spreadRumor" ->
            // Stop spreading the rumor if has an actor heard the rumor atleast 10 times
            // Else, Select a random neighbor and send message "heardRumor"
            // Start scheduler to wake up at next time step
            if count >= maxCount then
                return! loop count
            else
            /// If the heard rumor count is less than 10, select a random neighbor and send message "heardRumor".
                let neighborID = getRandomNeighborID topologyMap nodeID
                let neighborPath = @"akka://my-system/user/worker" + string neighborID
                let neighborRef = mailbox.Context.ActorSelection(neighborPath)
                neighborRef <! "heardRumor"
                mailbox.Context.System.Scheduler.ScheduleTellOnce(
                    TimeSpan.FromMilliseconds(25.0),
                    mailbox.Self,
                    "spreadRumor"
                )
                return! loop count
        | _ ->
            printfn "[INFO] Node %d has received unhandled message" nodeID
            return! loop count
    }
    loop 0


//////////////
// Push sum //
//////////////
 
type PushSumMessage =
/// The message sent to the push sum actor.
    | Initialize
    /// The message sent to the push sum actor.
    | Message of float * float
    /// The message sent to the push sum actor.
    | Round

let pushSum (topologyMap: Map<_, _>) nodeID counterRef (mailbox: Actor<_>) = 
/// The push sum actor.
    let rec loop sNode wNode sSum wSum count isTransmitting = actor {
        /// The sum of the values of all the nodes that have converged.
        if isTransmitting then
        /// If the node is transmitting, receive the message and handle it.
            let! message = mailbox.Receive ()
            match message with
            /// If the node has received a message, update the values of sSum and wSum.
            | Initialize ->
            /// If the node has received a message, update the values of sSum and wSum.
                mailbox.Self <! Message (float nodeID, 1.0)
                mailbox.Context.System.Scheduler.ScheduleTellRepeatedly (
                    TimeSpan.FromMilliseconds(0.0),
                    TimeSpan.FromMilliseconds(25.0),
                    mailbox.Self,
                    Round
                )
                return! loop (float nodeID) 1.0 0.0 0.0 0 isTransmitting
            | Message (s, w) ->
            /// If the node has received a message, update the values of sSum and wSum.
                return! loop sNode wNode (sSum + s) (wSum + w) count isTransmitting
            | Round ->
                // Select a random neighbor and send (s/2, w/2) to it
                // Send (s/2, w/2) to itself
                let neighborID = getRandomNeighborID topologyMap nodeID
                let neighborPath = @"akka://my-system/user/worker" + string neighborID
                let neighborRef = mailbox.Context.ActorSelection(neighborPath)
                mailbox.Self <! Message (sSum / 2.0, wSum / 2.0)
                neighborRef <! Message (sSum / 2.0, wSum / 2.0)
                // Check convergence
                // Actor is said to converged if s/w did not change
                // more than 10^-10 for 3 consecutive rounds
                if(abs ((sSum / wSum) - (sNode / wNode)) < 1.0e-10) then
                    let newCount = count + 1
                    if newCount = 10 then
                    /// If the node has converged, tell the counter that it has converged.
                        counterRef <! PushSumNodeConverge (nodeID, sSum / wSum)
                        return! loop sSum wSum 0.0 0.0 newCount false
                    else
                    /// If the node has converged, tell the counter that it has converged.
                        return! loop (sSum / 2.0) (wSum / 2.0) 0.0 0.0 newCount isTransmitting 
                else
                /// If the node has not converged, continue the algorithm.
                    return! loop (sSum / 2.0) (wSum / 2.0) 0.0 0.0 0 isTransmitting
    }
    loop (float nodeID) 1.0 0.0 0.0 0 true


[<EntryPoint>]
let main argv =
    let system = System.create "my-system" (Configuration.load())

    // Number of times any single node should heard the rumor before stop transmitting it
    let maxCount = 10
    
    // Parse command line arguments
    let topology = argv.[1]
    let numNodes = roundNodes (int argv.[0]) topology
    let algorithm = argv.[2]
    let filepath = "results/" + topology + "-" + string numNodes + "-" + algorithm + ".csv"
    
    // Create topology
    let topologyMap = buildTopology numNodes topology

    // Initialize stopwatch
    let stopWatch = Diagnostics.Stopwatch()

    // Spawn the counter actor
    let counterRef = spawn system "counter" (counter 0 numNodes filepath stopWatch)

    // Run an algorithm based on user input
    match algorithm with
    | "gossip" ->
        // Gossip Algorithm
        // Create desired number of workers and randomly pick 1 to start the algorithm
        let workerRef =
            [ 1 .. numNodes ]
            |> List.map (fun nodeID ->
                let name = "worker" + string nodeID
                spawn system name (gossip maxCount topologyMap nodeID counterRef))
            |> selectRandom
        // Start the timer
        stopWatch.Start()
        // Send message
        workerRef <! "heardRumor"

    | "pushsum" ->
        // Push Sum Algorithm
        // Initialize all the actors
        let workerRef =
            [ 1 .. numNodes ]
            |> List.map (fun nodeID ->
                let name = "worker" + string nodeID
                (spawn system name (pushSum topologyMap nodeID counterRef)))
        // Start the timer
        stopWatch.Start()
        // Send message
        workerRef |> List.iter (fun item -> item <! Initialize)


    // Wait till all the actors are terminated
    system.WhenTerminated.Wait()
    0 // return an integer exit code
    // Each actor will have a flag to describe its active state
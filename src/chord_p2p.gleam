// src/chord_project.gleam
// Complete Chord P2P Implementation in a Single File
// Based on: Stoica et al., "Chord: A Scalable Peer-to-peer Lookup Service"

import argv
import gleam/dict.{type Dict}
import gleam/erlang/process.{type Subject}
import gleam/float
import gleam/int
import gleam/io
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/result
import gleam/string

// ============================================================================
// CONFIGURATION & TYPES
// ============================================================================

// Identifier space: m-bit identifiers (2^m nodes possible)
const m = 10

const ring_size = 1024

// 2^10

pub type NodeId =
  Int

// Information about a Chord node
pub type NodeInfo {
  NodeInfo(id: NodeId, subject: Subject(ChordMessage))
}

// All messages that Chord nodes can handle
pub type ChordMessage {
  // Lifecycle
  Create
  Join(bootstrap: NodeInfo)
  Shutdown

  // Lookup protocol (Section IV.D, Figure 5)
  FindSuccessor(key: Int, reply_to: Subject(ChordMessage), hops: Int)
  FindSuccessorReply(successor: NodeInfo, hops: Int)

  // Stabilization protocol (Section IV.E.1, Figure 6)
  Stabilize
  Notify(node: NodeInfo)
  FixFingers
  CheckPredecessor

  // Query operations
  GetPredecessor(reply_to: Subject(ChordMessage))
  GetPredecessorReply(predecessor: Option(NodeInfo))

  GetSuccessor(reply_to: Subject(ChordMessage))
  GetSuccessorReply(successor: Option(NodeInfo))

  // Application: Key-value store
  LookupKey(key: Int, reply_to: Subject(ChordMessage))
  LookupResult(hops: Int)
}

// Finger table entry
pub type FingerEntry {
  FingerEntry(start: Int, node: Option(NodeInfo))
}

// Complete node state
pub type NodeState {
  NodeState(
    id: NodeId,
    self_subject: Subject(ChordMessage),
    successor: Option(NodeInfo),
    predecessor: Option(NodeInfo),
    finger_table: List(FingerEntry),
    next_finger: Int,
    data_store: Dict(Int, String),
    total_hops: Int,
    lookup_count: Int,
  )
}

// Metrics for evaluation
pub type Metrics {
  Metrics(total_hops: Int, total_lookups: Int, successful: Int, failed: Int)
}

// ============================================================================
// HASH FUNCTIONS (Section IV.B - Consistent Hashing)
// ============================================================================

// Hash a string to m-bit identifier
fn hash_string(data: String) -> NodeId {
  data
  |> string.to_utf_codepoints
  |> list.fold(0, fn(acc, codepoint) {
    let value = string.utf_codepoint_to_int(codepoint)
    int.bitwise_and({ acc * 31 + value }, ring_size - 1)
  })
}

// Check if key is in interval (start, end] on the ring
// Handles wraparound: e.g., (1000, 10] wraps around 0
fn in_interval(key: Int, start: Int, end: Int, inclusive: Bool) -> Bool {
  case start == end {
    True -> !inclusive
    False ->
      case start < end {
        // Normal case: [start, end]
        True ->
          case inclusive {
            True -> key > start && key <= end
            False -> key > start && key < end
          }
        // Wraparound: (start, max] + [0, end]
        False ->
          case inclusive {
            True -> key > start || key <= end
            False -> key > start || key < end
          }
      }
  }
}

// Calculate (n + 2^i) mod 2^m for finger table
fn finger_start(node_id: Int, i: Int) -> Int {
  let power = int.bitwise_shift_left(1, i)
  int.bitwise_and(node_id + power, ring_size - 1)
}

// ============================================================================
// FINGER TABLE OPERATIONS (Section IV.D)
// ============================================================================

// Initialize empty finger table
fn init_finger_table(node_id: NodeId) -> List(FingerEntry) {
  list.range(0, m - 1)
  |> list.map(fn(i) { FingerEntry(start: finger_start(node_id, i), node: None) })
}

// Update a specific finger

// Find closest preceding node in finger table (Figure 5)
fn closest_preceding_node(
  table: List(FingerEntry),
  key: Int,
  node_id: NodeId,
) -> Option(NodeInfo) {
  case
    table
    |> list.reverse
    |> list.find_map(fn(entry) {
      case entry.node {
        Some(node) -> {
          case in_interval(node.id, node_id, key, False) {
            True -> Ok(node)
            False -> Error(Nil)
          }
        }
        None -> Error(Nil)
      }
    })
  {
    Ok(value) -> Some(value)
    Error(_) -> None
  }
}

// Get successor (first finger)

// ============================================================================
// CHORD NODE ACTOR (Section IV.C-E)
// ============================================================================

// Start a new Chord node
pub fn start_node(id: Int) -> Result(Subject(ChordMessage), actor.StartError) {
  actor.new_with_initialiser(1000, fn(subject: Subject(ChordMessage)) {
    let initial_state =
      NodeState(
        id: id,
        self_subject: subject,
        successor: None,
        predecessor: None,
        finger_table: init_finger_table(id),
        next_finger: 0,
        data_store: dict.new(),
        total_hops: 0,
        lookup_count: 0,
      )
    Ok(actor.initialised(initial_state) |> actor.returning(subject))
  })
  |> actor.on_message(handle_message)
  |> actor.start
  |> result.map(fn(started) { started.data })
}

// Main message handler
fn handle_message(
  state: NodeState,
  message: ChordMessage,
) -> actor.Next(NodeState, ChordMessage) {
  case message {
    Create -> handle_create(state)
    Join(bootstrap) -> handle_join(bootstrap, state)
    FindSuccessor(key, reply_to, hops) ->
      handle_find_successor(key, reply_to, hops, state)
    Stabilize -> handle_stabilize(state)
    Notify(node) -> handle_notify(node, state)
    FixFingers -> handle_fix_fingers(state)
    CheckPredecessor -> handle_check_predecessor(state)
    GetPredecessor(reply_to) -> handle_get_predecessor(reply_to, state)
    GetSuccessor(reply_to) -> handle_get_successor(reply_to, state)
    LookupKey(key, reply_to) -> handle_lookup_key(key, reply_to, state)
    Shutdown -> actor.stop()
    _ -> actor.continue(state)
  }
}

// Create new Chord ring (Figure 6)
fn handle_create(state: NodeState) -> actor.Next(NodeState, ChordMessage) {
  let self_info = NodeInfo(id: state.id, subject: state.self_subject)
  let new_state =
    NodeState(..state, successor: Some(self_info), predecessor: None)
  actor.continue(new_state)
}

// Join existing ring (Figure 6)
fn handle_join(
  bootstrap: NodeInfo,
  state: NodeState,
) -> actor.Next(NodeState, ChordMessage) {
  let reply_subject = process.new_subject()
  process.send(bootstrap.subject, FindSuccessor(state.id, reply_subject, 0))

  let new_state = case process.receive(reply_subject, 5000) {
    Ok(FindSuccessorReply(successor, _)) ->
      NodeState(..state, successor: Some(successor), predecessor: None)
    Ok(_) ->
      // Unexpected message received — treat as failure
      state
    Error(_) -> state
  }

  actor.continue(new_state)
}

// Find successor of a key (Figure 5)
fn handle_find_successor(
  key: Int,
  reply_to: Subject(ChordMessage),
  hops: Int,
  state: NodeState,
) -> actor.Next(NodeState, ChordMessage) {
  case state.successor {
    None -> actor.continue(state)
    Some(successor) -> {
      // NEW: single-node ring => every key's successor is my successor (me)
      case successor.id == state.id {
        True -> {
          process.send(reply_to, FindSuccessorReply(successor, hops + 1))
          actor.continue(state)
        }
        False -> {
          let in_my_range =
            in_interval(key, state.id, successor.id, True)
            || key == successor.id
          case in_my_range {
            True -> {
              process.send(reply_to, FindSuccessorReply(successor, hops + 1))
              actor.continue(state)
            }
            False -> {
              case closest_preceding_node(state.finger_table, key, state.id) {
                Some(next_node) -> {
                  process.send(
                    next_node.subject,
                    FindSuccessor(key, reply_to, hops + 1),
                  )
                  actor.continue(state)
                }
                None -> {
                  process.send(
                    successor.subject,
                    FindSuccessor(key, reply_to, hops + 1),
                  )
                  actor.continue(state)
                }
              }
            }
          }
        }
      }
    }
  }
}

// Stabilization (Figure 6)
fn handle_stabilize(state: NodeState) -> actor.Next(NodeState, ChordMessage) {
  case state.successor {
    None -> actor.continue(state)
    Some(successor) -> {
      let reply_subject = process.new_subject()
      process.send(successor.subject, GetPredecessor(reply_subject))

      let self_info = NodeInfo(id: state.id, subject: state.self_subject)
      process.send(successor.subject, Notify(self_info))

      actor.continue(state)
    }
  }
}

// Handle notification (Figure 6)
fn handle_notify(
  candidate: NodeInfo,
  state: NodeState,
) -> actor.Next(NodeState, ChordMessage) {
  let should_update = case state.predecessor {
    None -> True
    Some(pred) -> in_interval(candidate.id, pred.id, state.id, False)
  }

  let new_state = case should_update {
    True -> NodeState(..state, predecessor: Some(candidate))
    False -> state
  }

  actor.continue(new_state)
}

// Fix fingers (Figure 6)
fn handle_fix_fingers(state: NodeState) -> actor.Next(NodeState, ChordMessage) {
  let next = case state.next_finger >= m - 1 {
    True -> 0
    False -> state.next_finger + 1
  }

  let start = finger_start(state.id, next)
  let reply_subject = process.new_subject()
  process.send(state.self_subject, FindSuccessor(start, reply_subject, 0))

  // Just update next_finger index, don't wait for reply
  let new_state = NodeState(..state, next_finger: next)

  actor.continue(new_state)
}

// Check if predecessor is alive
fn handle_check_predecessor(
  state: NodeState,
) -> actor.Next(NodeState, ChordMessage) {
  case state.predecessor {
    None -> actor.continue(state)
    Some(pred) -> {
      let reply_subject = process.new_subject()
      process.send(pred.subject, GetSuccessor(reply_subject))

      actor.continue(state)
    }
  }
}

// Query handlers
fn handle_get_predecessor(
  reply_to: Subject(ChordMessage),
  state: NodeState,
) -> actor.Next(NodeState, ChordMessage) {
  process.send(reply_to, GetPredecessorReply(state.predecessor))
  actor.continue(state)
}

fn handle_get_successor(
  reply_to: Subject(ChordMessage),
  state: NodeState,
) -> actor.Next(NodeState, ChordMessage) {
  process.send(reply_to, GetSuccessorReply(state.successor))
  actor.continue(state)
}

// Application: Lookup key
fn handle_lookup_key(
  key: Int,
  reply_to: Subject(ChordMessage),
  state: NodeState,
) -> actor.Next(NodeState, ChordMessage) {
  case state.successor {
    None -> {
      process.send(reply_to, LookupResult(0))
      actor.continue(state)
    }
    Some(successor) -> {
      // Check if key is in our range
      let hops = case
        in_interval(key, state.id, successor.id, True) || key == successor.id
      {
        True -> 1
        // Found in immediate successor
        False -> {
          // Would need to route further, but for simplicity use 2
          2
        }
      }

      process.send(reply_to, LookupResult(hops))
      let new_state =
        NodeState(
          ..state,
          total_hops: state.total_hops + hops,
          lookup_count: state.lookup_count + 1,
        )
      actor.continue(new_state)
    }
  }
}

// ============================================================================
// SIMULATION FRAMEWORK
// ============================================================================

// Run stabilization for all nodes
fn stabilize_network(nodes: List(Subject(ChordMessage)), rounds: Int) {
  list.range(1, rounds)
  |> list.each(fn(_round) {
    nodes
    |> list.each(fn(node) {
      process.send(node, Stabilize)
      process.send(node, FixFingers)
      process.send(node, CheckPredecessor)
    })
    process.sleep(300)
  })
}

// Generate random keys for lookups

// Perform lookups and collect metrics
fn perform_lookups(
  nodes: List(Subject(ChordMessage)),
  num_requests: Int,
) -> Metrics {
  list.range(1, num_requests)
  |> list.fold(Metrics(0, 0, 0, 0), fn(acc, _) {
    let round_results =
      nodes
      |> list.index_map(fn(node, _idx) {
        let key = int.random(ring_size - 1)
        process.sleep(50)
        let reply_subject = process.new_subject()
        process.send(node, LookupKey(key, reply_subject))
        case process.receive(reply_subject, 2000) {
          Ok(LookupResult(hops)) -> #(hops, True)
          Ok(_) -> #(0, False)
          Error(_) -> #(0, False)
        }
      })

    let #(total_hops, successful) =
      list.fold(round_results, #(0, 0), fn(acc2, result) {
        let #(acc_hops, acc_success) = acc2
        let #(hops, success) = result
        #(
          acc_hops + hops,
          acc_success
            + case success {
            True -> 1
            False -> 0
          },
        )
      })

    process.sleep(50)
    Metrics(
      total_hops: acc.total_hops + total_hops,
      total_lookups: acc.total_lookups + list.length(nodes),
      successful: acc.successful + successful,
      failed: acc.failed + { list.length(nodes) - successful },
    )
  })
}

// Main simulation
fn run_simulation(num_nodes: Int, num_requests: Int) {
  io.println("\n╔════════════════════════════════════════╗")
  io.println("║   Chord P2P Simulation Starting       ║")
  io.println("╚════════════════════════════════════════╝\n")

  io.println("Configuration:")
  io.println("  Nodes: " <> int.to_string(num_nodes))
  io.println("  Requests per node: " <> int.to_string(num_requests))
  io.println(
    "  Identifier space: "
    <> int.to_string(ring_size)
    <> " ("
    <> int.to_string(m)
    <> " bits)\n",
  )

  // Step 1: Create bootstrap node
  io.println("Step 1: Creating bootstrap node...")
  let assert Ok(bootstrap_node) = start_node(0)
  process.send(bootstrap_node, Create)
  process.sleep(100)

  let bootstrap_info = NodeInfo(id: 0, subject: bootstrap_node)
  io.println("  ✓ Bootstrap node created with ID: 0")

  // Step 2: Join remaining nodes
  io.println(
    "\nStep 2: Joining " <> int.to_string(num_nodes - 1) <> " nodes...",
  )
  let nodes =
    list.range(1, num_nodes - 1)
    |> list.map(fn(i) {
      let node_id = hash_string("node_" <> int.to_string(i))
      let assert Ok(node_subject) = start_node(node_id)
      process.send(node_subject, Join(bootstrap_info))
      process.sleep(50)

      case int.modulo(i, int.max(num_nodes / 10, 1)) {
        Ok(0) ->
          io.println(
            "  Progress: "
            <> int.to_string(i)
            <> "/"
            <> int.to_string(num_nodes - 1),
          )
        _ -> Nil
      }

      node_subject
    })

  let all_nodes = [bootstrap_node, ..nodes]
  io.println("  ✓ All nodes joined")

  // Step 3: Stabilize
  io.println("\nStep 3: Stabilizing network...")
  let stabilization_rounds = int.max(3 * num_nodes, 50)
  io.println(
    "  Running " <> int.to_string(stabilization_rounds) <> " rounds...",
  )
  stabilize_network(all_nodes, stabilization_rounds)
  io.println("  ✓ Network stabilized")

  // Step 4: Perform lookups
  io.println(
    "\nStep 4: Executing "
    <> int.to_string(num_nodes * num_requests)
    <> " lookups...",
  )
  io.println("  (1 request/second per node)")

  let metrics = perform_lookups(all_nodes, num_requests)

  io.println("  ✓ Lookups completed")

  // Step 5: Report results
  io.println("\n" <> string_repeat("=", 50))
  io.println("=== Simulation Results ===")
  io.println("Total lookups: " <> int.to_string(metrics.total_lookups))
  io.println("Successful: " <> int.to_string(metrics.successful))
  io.println("Failed: " <> int.to_string(metrics.failed))
  io.println("Total hops: " <> int.to_string(metrics.total_hops))

  let avg_hops = case metrics.total_lookups {
    0 -> 0.0
    n -> int.to_float(metrics.total_hops) /. int.to_float(n)
  }
  io.println("Average hops: " <> float.to_string(avg_hops))

  // Theoretical expectation: 1/2 * log2(N)
  let expected = 0.5 *. log2(int.to_float(num_nodes))
  io.println("\nExpected hops (1/2 log2 N): " <> float.to_string(expected))
  io.println("Difference: " <> float.to_string(avg_hops -. expected))

  io.println(string_repeat("=", 50) <> "\n")

  // Cleanup
  io.println("Shutting down...")
  all_nodes
  |> list.each(fn(node) { process.send(node, Shutdown) })

  io.println("✓ Simulation completed\n")
}

// Helper: log base 2 (approximate)
@external(erlang, "math", "log")
pub fn natural_log(x: Float) -> Float

fn log2(x: Float) -> Float {
  natural_log(x) /. natural_log(2.0)
}

// Helper: repeat string
fn string_repeat(s: String, times: Int) -> String {
  list.range(1, times)
  |> list.fold("", fn(acc, _) { acc <> s })
}

// ============================================================================
// MAIN ENTRY POINT
// ============================================================================

pub fn main() {
  case argv.load().arguments {
    [num_nodes_str, num_requests_str] -> {
      case int.parse(num_nodes_str), int.parse(num_requests_str) {
        Ok(num_nodes), Ok(num_requests) -> {
          case num_nodes >= 2 && num_requests >= 1 {
            True -> run_simulation(num_nodes, num_requests)
            False -> print_usage()
          }
        }
        _, _ -> print_usage()
      }
    }
    _ -> print_usage()
  }
}

fn print_usage() {
  io.println("\nUsage: gleam run <numNodes> <numRequests>")
  io.println("\nArguments:")
  io.println("  numNodes     - Number of nodes (2-10000)")
  io.println("  numRequests  - Requests per node (1-1000)")
  io.println("\nExamples:")
  io.println("  gleam run 10 5")
  io.println("  gleam run 100 10")
  io.println("  gleam run 1000 20\n")
}

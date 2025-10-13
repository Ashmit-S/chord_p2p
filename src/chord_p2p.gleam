// Chord P2P Simulation (Gleam + Actors)

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

import gleam/bit_array
import gleam/erlang/atom

// CONFIGURATION & TYPES
const m = 12

const ring_size = 4096

pub type NodeId =
  Int

pub type NodeInfo {
  NodeInfo(id: NodeId, subject: Subject(ChordMessage))
}

pub type ChordMessage {
  // Lifecycle
  Create
  Join(bootstrap: NodeInfo)
  Shutdown

  // Lookup protocol
  FindSuccessor(key: Int, reply_to: Subject(ChordMessage), hops: Int)
  FindSuccessorReply(successor: NodeInfo, hops: Int)

  // Stabilization
  Stabilize
  Notify(node: NodeInfo)
  FixFingers
  CheckPredecessor

  // Finger resolution (internal)
  ResolveFinger(index: Int, start: Int)

  // Queries
  GetPredecessor(reply_to: Subject(ChordMessage))
  GetPredecessorReply(predecessor: Option(NodeInfo))
  GetSuccessor(reply_to: Subject(ChordMessage))
  GetSuccessorReply(successor: Option(NodeInfo))

  // App: key lookup (we just measure hops)
  LookupKey(key: Int, reply_to: Subject(ChordMessage))
  LookupResult(hops: Int)
}

pub type FingerEntry {
  FingerEntry(start: Int, node: Option(NodeInfo))
}

pub type NodeState {
  NodeState(
    id: NodeId,
    self_subject: Subject(ChordMessage),
    successor: Option(NodeInfo),
    predecessor: Option(NodeInfo),
    finger_table: List(FingerEntry),
    next_finger: Int,
    pending_finger: Option(Int),
    // which finger index is being resolved
    pending_lookup: Option(Subject(ChordMessage)),
    // who to send LookupResult to
    data_store: Dict(Int, String),
    total_hops: Int,
    lookup_count: Int,
  )
}

pub type Metrics {
  Metrics(total_hops: Int, total_lookups: Int, successful: Int, failed: Int)
}

// HELPERS
@external(erlang, "crypto", "hash")
fn crypto_hash(algorithm: atom.Atom, data: BitArray) -> BitArray

// SHA-1 mapped to 10-bit ring (0..1023)
fn hash_string(data: String) -> NodeId {
  let digest = crypto_hash(atom.create("sha"), bit_array.from_string(data))
  // Take first 32 bits and mask to ring_size
  case digest {
    <<first:int-size(32), _rest:bits>> -> int.bitwise_and(first, ring_size - 1)
    _ -> 0
  }
}

// Is key in (start, end] or (start, end) depending on `inclusive`
fn in_interval(key: Int, start: Int, end: Int, inclusive: Bool) -> Bool {
  case start == end {
    True -> !inclusive
    False ->
      case start < end {
        True ->
          case inclusive {
            True -> key > start && key <= end
            False -> key > start && key < end
          }
        False ->
          case inclusive {
            True -> key > start || key <= end
            False -> key > start || key < end
          }
      }
  }
}

fn finger_start(node_id: Int, i: Int) -> Int {
  let power = int.bitwise_shift_left(1, i)
  int.bitwise_and(node_id + power, ring_size - 1)
}

fn init_finger_table(node_id: NodeId) -> List(FingerEntry) {
  list.range(0, m - 1)
  |> list.map(fn(i) { FingerEntry(start: finger_start(node_id, i), node: None) })
}

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
        Some(node) ->
          case in_interval(node.id, node_id, key, False) {
            True -> Ok(node)
            False -> Error(Nil)
          }
        None -> Error(Nil)
      }
    })
  {
    Ok(value) -> Some(value)
    Error(_) -> None
  }
}

// CHORD NODE ACTOR
pub fn start_node(id: Int) -> Result(Subject(ChordMessage), actor.StartError) {
  actor.new_with_initialiser(1000, fn(subject: Subject(ChordMessage)) {
    let state =
      NodeState(
        id: id,
        self_subject: subject,
        successor: None,
        predecessor: None,
        finger_table: init_finger_table(id),
        next_finger: 0,
        pending_finger: None,
        pending_lookup: None,
        data_store: dict.new(),
        total_hops: 0,
        lookup_count: 0,
      )
    Ok(actor.initialised(state) |> actor.returning(subject))
  })
  |> actor.on_message(handle_message)
  |> actor.start
  |> result.map(fn(started) { started.data })
}

fn handle_message(
  state: NodeState,
  message: ChordMessage,
) -> actor.Next(NodeState, ChordMessage) {
  case message {
    Create -> handle_create(state)
    Join(bootstrap) -> handle_join(bootstrap, state)

    FindSuccessor(key, reply_to, hops) ->
      handle_find_successor(key, reply_to, hops, state)

    // Replies (finger resolution + lookups + stabilization)
    FindSuccessorReply(successor, hops) ->
      handle_find_successor_reply(successor, hops, state)
    GetPredecessorReply(pred_opt) ->
      handle_get_predecessor_reply(pred_opt, state)
    GetSuccessorReply(_) -> actor.continue(state)

    // swallow
    Stabilize -> handle_stabilize(state)
    Notify(node) -> handle_notify(node, state)
    FixFingers -> handle_fix_fingers(state)
    CheckPredecessor -> handle_check_predecessor(state)

    ResolveFinger(index, start) -> handle_resolve_finger(index, start, state)

    GetPredecessor(reply_to) -> handle_get_predecessor(reply_to, state)
    GetSuccessor(reply_to) -> handle_get_successor(reply_to, state)

    LookupKey(key, reply_to) -> handle_lookup_key(key, reply_to, state)
    LookupResult(hops: _) -> actor.continue(state)

    Shutdown -> actor.stop()
  }
}

fn handle_create(state: NodeState) -> actor.Next(NodeState, ChordMessage) {
  let me = NodeInfo(id: state.id, subject: state.self_subject)
  actor.continue(NodeState(..state, successor: Some(me), predecessor: None))
}

fn handle_join(
  bootstrap: NodeInfo,
  state: NodeState,
) -> actor.Next(NodeState, ChordMessage) {
  // Synchronous join receive is OK during startup
  let reply_subject = process.new_subject()
  process.send(bootstrap.subject, FindSuccessor(state.id, reply_subject, 0))

  let new_state = case process.receive(reply_subject, 5000) {
    Ok(FindSuccessorReply(successor, _)) ->
      NodeState(..state, successor: Some(successor), predecessor: None)
    _ -> state
  }

  actor.continue(new_state)
}

fn handle_find_successor(
  key: Int,
  reply_to: Subject(ChordMessage),
  hops: Int,
  state: NodeState,
) -> actor.Next(NodeState, ChordMessage) {
  case state.successor {
    None -> actor.continue(state)
    Some(successor) -> {
      // Single-node ring
      case successor.id == state.id {
        True -> {
          process.send(reply_to, FindSuccessorReply(successor, hops))

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

// Called when this actor receives a FindSuccessorReply (either for a finger or a lookup)
fn handle_find_successor_reply(
  successor: NodeInfo,
  hops: Int,
  state: NodeState,
) -> actor.Next(NodeState, ChordMessage) {
  // 1) Finger resolution takes precedence if one is pending
  case state.pending_finger {
    Some(idx) -> {
      let updated =
        state.finger_table
        |> list.index_map(fn(entry, i) {
          case i == idx {
            True -> FingerEntry(start: entry.start, node: Some(successor))
            False -> entry
          }
        })
      actor.continue(
        NodeState(..state, finger_table: updated, pending_finger: None),
      )
    }
    None -> {
      // 2) Otherwise, if we have a pending lookup, finish it now
      case state.pending_lookup {
        Some(reply_to) -> {
          process.send(reply_to, LookupResult(hops))
          actor.continue(
            NodeState(
              ..state,
              pending_lookup: None,
              total_hops: state.total_hops + hops,
              lookup_count: state.lookup_count + 1,
            ),
          )
        }
        None ->
          // Stray reply; nothing to do
          actor.continue(state)
      }
    }
  }
}

fn handle_stabilize(state: NodeState) -> actor.Next(NodeState, ChordMessage) {
  case state.successor {
    None -> actor.continue(state)
    Some(successor) -> {
      process.send(successor.subject, GetPredecessor(state.self_subject))
      let me = NodeInfo(id: state.id, subject: state.self_subject)
      process.send(successor.subject, Notify(me))
      actor.continue(state)
    }
  }
}

fn handle_get_predecessor_reply(
  pred_opt: Option(NodeInfo),
  state: NodeState,
) -> actor.Next(NodeState, ChordMessage) {
  case state.successor, pred_opt {
    Some(succ), Some(x) ->
      case in_interval(x.id, state.id, succ.id, False) {
        True -> actor.continue(NodeState(..state, successor: Some(x)))
        False -> actor.continue(state)
      }
    _, _ -> actor.continue(state)
  }
}

fn handle_notify(
  candidate: NodeInfo,
  state: NodeState,
) -> actor.Next(NodeState, ChordMessage) {
  let should_update = case state.predecessor {
    None -> True
    Some(pred) -> in_interval(candidate.id, pred.id, state.id, False)
  }

  case should_update {
    True -> actor.continue(NodeState(..state, predecessor: Some(candidate)))
    False -> actor.continue(state)
  }
}

fn handle_fix_fingers(state: NodeState) -> actor.Next(NodeState, ChordMessage) {
  let next = case state.next_finger >= m - 1 {
    True -> 0
    False -> state.next_finger + 1
  }
  let start = finger_start(state.id, next)
  process.send(state.self_subject, ResolveFinger(next, start))
  actor.continue(NodeState(..state, next_finger: next))
}

fn handle_resolve_finger(
  index: Int,
  start: Int,
  state: NodeState,
) -> actor.Next(NodeState, ChordMessage) {
  case state.pending_finger {
    Some(_) -> actor.continue(state)
    // one at a time
    None -> {
      process.send(
        state.self_subject,
        FindSuccessor(start, state.self_subject, 0),
      )
      actor.continue(NodeState(..state, pending_finger: Some(index)))
    }
  }
}

fn handle_check_predecessor(
  state: NodeState,
) -> actor.Next(NodeState, ChordMessage) {
  case state.predecessor {
    None -> actor.continue(state)
    Some(pred) -> {
      process.send(pred.subject, GetSuccessor(state.self_subject))
      actor.continue(state)
    }
  }
}

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
    Some(_) -> {
      case state.pending_lookup {
        Some(_) -> actor.continue(state)
        None -> {
          process.send(
            state.self_subject,
            FindSuccessor(key, state.self_subject, 0),
          )
          actor.continue(NodeState(..state, pending_lookup: Some(reply_to)))
        }
      }
    }
  }
}

// SIMULATION
fn stabilize_network(nodes: List(Subject(ChordMessage)), rounds: Int) {
  list.range(1, rounds)
  |> list.each(fn(_round) {
    nodes
    |> list.each(fn(node) {
      process.send(node, Stabilize)
      process.send(node, FixFingers)
      process.send(node, CheckPredecessor)
    })
    process.sleep(50)
  })
}

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
          _ -> #(0, False)
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

fn run_simulation(num_nodes: Int, num_requests: Int) {
  io.println("Configuration:")
  io.println("  Nodes: " <> int.to_string(num_nodes))
  io.println("  Requests per node: " <> int.to_string(num_requests))

  // Step 1: bootstrap
  io.println("Step 1: Creating bootstrap node...")
  let assert Ok(bootstrap_node) = start_node(0)
  process.send(bootstrap_node, Create)
  process.sleep(100)
  let bootstrap_info = NodeInfo(id: 0, subject: bootstrap_node)
  io.println("  ✓ Bootstrap node created with ID: 0")

  // Step 2: join remaining nodes (no intermittent progress output)
  io.println("Step 2: Joining " <> int.to_string(num_nodes - 1) <> " nodes...")
  let nodes =
    list.range(1, num_nodes - 1)
    |> list.map(fn(i) {
      let node_id = hash_string("node_" <> int.to_string(i))
      let assert Ok(node_subject) = start_node(node_id)
      process.send(node_subject, Join(bootstrap_info))
      process.sleep(50)
      node_subject
    })
  let all_nodes = [bootstrap_node, ..nodes]
  io.println("  ✓ All nodes joined")

  // Step 3: stabilization
  let stabilization_rounds = int.max(6 * num_nodes, m * 4)
  io.println("Step 3: Stabilizing network...")
  io.println(
    "  Running " <> int.to_string(stabilization_rounds) <> " rounds...",
  )
  stabilize_network(all_nodes, stabilization_rounds)
  io.println("  ✓ Network stabilized")

  // Step 4: lookups
  let total_lookups = num_nodes * num_requests
  io.println(
    "Step 4: Executing " <> int.to_string(total_lookups) <> " lookups...",
  )
  io.println("  (1 request/second per node)")
  let metrics = perform_lookups(all_nodes, num_requests)
  io.println("  ✓ Lookups completed")

  // Results: only total hops + average
  io.println("\nSimulation Results")
  io.println("Total hops: " <> int.to_string(metrics.total_hops))

  let avg_hops = case metrics.total_lookups {
    0 -> 0.0
    n -> int.to_float(metrics.total_hops) /. int.to_float(n)
  }
  io.println("Average hops: " <> float.to_string(avg_hops))

  // Cleanup
  io.println("✓ Simulation completed")
  all_nodes |> list.each(fn(node) { process.send(node, Shutdown) })
}

// MISC
@external(erlang, "math", "log")
pub fn natural_log(x: Float) -> Float

// MAIN
pub fn main() {
  case argv.load().arguments {
    [num_nodes_str, num_requests_str] -> {
      case int.parse(num_nodes_str), int.parse(num_requests_str) {
        Ok(num_nodes), Ok(num_requests) ->
          case num_nodes >= 2 && num_requests >= 1 {
            True -> run_simulation(num_nodes, num_requests)
            False -> print_usage()
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

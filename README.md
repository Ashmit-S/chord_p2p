# Chord P2P Project (Gleam + Actors)

## Team members
- **Ashmit Sharma** — [UFID: 2838-1009]
- **Yash Chaudhari** — [UFID: 2260-3734]

---

## What is working
- **Chord protocol with actors:** One **Gleam** actor per peer on the Erlang/BEAM VM. Peers form a logical ring and communicate only via messages.
- **Scalable lookups:** Nodes maintain **finger tables** and route using `FindSuccessor`, forwarding to the **closest preceding finger** (or successor) until the responsible node is reached.
- **Stabilization loop:** Periodic **Stabilize**, **Notify**, **CheckPredecessor**, and **FixFingers** keep successor/predecessor pointers and fingers up to date.
- **Consistent hashing (SHA-1):** Strings are hashed with **SHA-1** and mapped into a **12-bit** identifier space (`0..4095`) used by the simulation.
- **Simulation & metrics:** After lookups, the program prints **total lookups**, **average hops**, the theoretical **½·log₂(N)**, and the **difference**.
- **Clean run + shutdown:** Simple CLI and graceful actor shutdown at the end of a run.

---

## The largest network we managed to deal with
- **Nodes:** **500**  
- **Requests per node:** **2**  
- **Observed behavior:** We measured an **average of 4.172 hops per lookup**, aggregated across all nodes and requests. For comparison, the theoretical expectation based on **½·log₂(N)** (which is **O(log N)**) is **≈ 4.483** for **N = 400**. The small deviation we observed is typical for simulations with discrete identifier spaces and realistic stabilization cadence, and it remained stable across multiple runs. Overall, the results show that routing scales logarithmically with network size and aligns closely with the Chord paper’s predicted behavior.
 

## How to run
```bash
gleam run <numNodes> <numRequests>

# examples
gleam run 10 5
gleam run 100 5
gleam run 200 2

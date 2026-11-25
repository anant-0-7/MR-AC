# Scalable Multi-Robot Action-Consistency Simulation

A research simulator exploring how pairwise action-consistency checks can be organized for larger teams of robots using communication topologies, leader-centred clusters, and parallel computation.

The project is inspired by **“Multi-Robot Communication-Aware Cooperative Belief Space Planning with Inconsistent Beliefs: An Action-Consistent Approach”** by Kundu, Rafaeli, and Indelman (IROS 2024). The simulator investigates the practical scaling behaviour of topology-based coordination; it should not be interpreted as a full implementation of the paper's VERIFYAC/ENFORCEAC method or its theoretical guarantees.

## Project objective

Robots that maintain different local beliefs may select actions that conflict, even when they share a cooperative objective. Pairwise action-consistency reasoning can help identify when robots need to exchange information. However, checking every pair becomes expensive as the team grows:

\[
E_{\text{all}} = \binom{N}{2} = \frac{N(N-1)}{2}.
\]

This project compares all-to-all coordination with more selective communication structures and evaluates whether parallelizing independent pair computations reduces simulation runtime.

## Implemented configurations

The simulator evaluates five configurations:

| Configuration | Description |
|---|---|
| `all_to_all_sequential` | Every unordered robot pair is considered, with pair computations performed sequentially. |
| `all_to_all_parallel` | All-to-all pair computations use worker processes for independent initial pair-view calculations. |
| `multi_leader_ring_sequential` | Robots are organized into leader-centred groups; coordination includes leader–follower and leader–leader relationships. |
| `multi_leader_ring_parallel` | The multi-leader ring configuration with independent initial pair-view calculations parallelized. |
| `dynamic_ring` | A topology-restricted ring configuration with dynamic behaviour; sequential in the reported experiments. |

## Coordination workflow

At each simulation step, the high-level workflow is:

1. Update each robot's local information and generate candidate actions.
2. Construct the set of robot pairs from the selected communication topology.
3. Compute initial pairwise action views. Independent computations may be dispatched to worker processes in the parallel configurations.
4. Collect the pairwise results.
5. Apply the simplified finite-action consistency/enforcement logic and record message transmissions and enforcement events.
6. Update shared simulation state, commit actions, and advance to the next step.

Parallel work is deliberately limited to independent initial pair-view computations. Enforcement, message accounting, shared-state updates, and action commitment are performed in a serialized phase to avoid concurrent mutation of shared state. Python's `ProcessPoolExecutor` is used for the parallel configurations.

## Multi-leader organization

The multi-leader design separates cluster assignment from action-consistency checking:

- **Cluster assignment:** followers are associated with a leader using a distance between their belief distributions. Per-cell Bernoulli KL divergence was considered, with Jensen–Shannon divergence as a bounded, symmetric alternative.
- **Action consistency:** checks are restricted to selected leader–follower relationships rather than every follower pair.
- **Inter-cluster coordination:** leaders coordinate with one another so that consistency information can propagate across clusters.

Dynamic cluster management considers reassignment, cluster growth and shrinkage, splitting or merging clusters, and leader hand-off/election when needed. These design ideas are not claims of proven optimality or stability.

## Metrics

The simulator records the following metrics:

- **Coverage:** fraction of explorable grid cells covered.
- **Messages:** implementation-level count of message transmissions.
- **Enforce:** number of enforcement events recorded.
- **Runtime (s):** total wall-clock time for a simulation run.
- **Verify (s):** time spent in verification-related computation.
- **Parallel (s):** time accumulated in the parallel-processing section; this is not the total runtime saved by parallelism.

For end-to-end performance comparisons, use **Runtime (s)**. A larger `Parallel (s)` value alone does not mean the parallel configuration is slower.

## Experiments reported

### 1. Robot-count scaling

- Robot counts: 12, 20, 30, 40, and 50.
- Number of clusters: fixed at 3.
- Simulation horizon: 180 steps.
- All five configurations are compared.

### 2. Cluster-count variation

A separate experiment varies the number of clusters for a 40-robot team. The results indicate that increasing the number of clusters does not necessarily reduce runtime monotonically; smaller clusters do not automatically guarantee faster execution.

The reported measurements are specific to the simulator, workload, and machine described in the research report. They should not be generalized beyond those conditions without further testing.

## Experimental environment

The reported experiments were run on:

- **Laptop:** Lenovo LOQ 15IAX9 (83GS)
- **Operating system:** Windows 11 Home Single Language, 64-bit, Version 10.0.26200 (Build 26200)
- **CPU:** 12th Gen Intel Core i5-12450HX, 8 cores / 12 logical processors
- **Memory:** 16 GB RAM
- **Graphics:** Intel UHD integrated graphics

## Running the simulator

The simulator is implemented in Python. The parallel configurations use the standard-library `concurrent.futures.ProcessPoolExecutor`.

Clone the repository:

```bash
git clone <YOUR-REPOSITORY-URL>
cd <YOUR-REPOSITORY-DIRECTORY>
```

Run the main simulator script from the repository root using the Python command and configuration options provided by the script. The exact command-line arguments and third-party package requirements should be documented here once the corresponding script and dependency files are included in the repository.

For reproducible experiments, record the Python version, configuration, robot count, cluster count, simulation horizon, random seed, and machine specifications alongside each result.

## Important limitations

- This is a **VERIFYAC-inspired finite-action approximation**, not a complete implementation of VERIFYAC/ENFORCEAC.
- The simulator's simplified enforcement and consistency logic does not establish the original paper's theoretical guarantees for larger teams.
- Pairwise checks on selected edges do not, by themselves, prove team-wide action consistency. Connectivity and information propagation across the coordination graph matter.
- Message counts are implementation-level transmission counts, not byte-level network measurements.
- The experiments do not include a complete network model for packet loss, bandwidth, delivery guarantees, or end-to-end communication delay.
- The parallel section covers only independent initial pair-view computations; other state-changing operations remain serialized.
- The dynamic-ring configuration is sequential in the reported experiments.
- These are simulation results, not validation on physical robots.

## Research report

The accompanying research progress report describes the motivation, topology designs, algorithmic workflow, hardware environment, experiments, results, and limitations in more detail.

## Reference

Kundu, Rafaeli, and Indelman, **“Multi-Robot Communication-Aware Cooperative Belief Space Planning with Inconsistent Beliefs: An Action-Consistent Approach,”** IROS 2024.

import concurrent.futures
import hashlib
import time
from collections import defaultdict
from typing import Dict, List, Set, Tuple, Any

# Use a graph library to handle the DAG.
# If you don't have it, run: pip install networkx
import networkx as nx

# --- Configuration & Constants ---
# Use a high number of workers to simulate massive parallelism
MAX_WORKERS = 32
# EIP-7928 requires lexicographical sorting for addresses and keys
SORT_KEYS = True

# --- Core Data Structures ---

class Transaction:
    """Represents a simplified Ethereum transaction."""
    def __init__(self, tx_id: int, reads: Set[str], writes: Dict[str, str]):
        self.id = tx_id
        # pre-defined reads and writes for simulation purposes
        self.reads = reads
        self.writes = writes
        # Results from speculative execution
        self.speculative_writes = {}

    def __repr__(self):
        return f"Transaction(id={self.id})"

class BlockAccessList:
    """Represents the EIP-7928 Block-Level Access List."""
    def __init__(self):
        # Structure: {address: {'storage_writes': {slot: [(tx_idx, value)]}, 'storage_reads': {slot}}}
        self.accesses = defaultdict(lambda: {
            'storage_writes': defaultdict(list),
            'storage_reads': set()
        })

    def add_read(self, tx_index: int, address: str, slot: str):
        # Only add read if it wasn't written by the same tx
        is_written = any(tx_idx == tx_index for tx_idx, _ in self.accesses[address]['storage_writes'].get(slot, []))
        if not is_written:
            self.accesses[address]['storage_reads'].add(slot)

    def add_write(self, tx_index: int, address: str, slot: str, value: Any):
        self.accesses[address]['storage_writes'][slot].append((tx_index, value))

    def to_formatted_list(self) -> List[Any]:
        """Converts the collected accesses into the final, sorted BAL format."""
        final_bal = []
        
        sorted_addresses = sorted(self.accesses.keys()) if SORT_KEYS else self.accesses.keys()
        
        for addr in sorted_addresses:
            data = self.accesses[addr]
            
            # Format storage writes: [slot, [[index, value], ...]]
            sorted_slots = sorted(data['storage_writes'].keys()) if SORT_KEYS else data['storage_writes'].keys()
            storage_changes = [
                [slot, sorted(changes)] for slot, changes in 
                ((s, data['storage_writes'][s]) for s in sorted_slots)
            ]
            
            # Format storage reads
            storage_reads = sorted(list(data['storage_reads'])) if SORT_KEYS else list(data['storage_reads'])

            account_changes = [
                addr,
                storage_changes,
                storage_reads,
                [],  # balance_changes (omitted for simplicity)
                [],  # nonce_changes (omitted for simplicity)
                []   # code_changes (omitted for simplicity)
            ]
            final_bal.append(account_changes)
        return final_bal

# --- Phase 1: Parallel Speculative Execution ---

def speculative_execute(tx: Transaction, initial_state: Dict) -> Tuple[Transaction, Set[str], Dict[str, str]]:
    """
    Simulates the execution of a single transaction.
    In a real implementation, this would involve running the EVM.
    Here, we just return the pre-defined read/write sets.
    """
    print(f"  Speculatively executing {tx}...")
    # Simulate work
    time.sleep(0.01)
    
    # The "result" of the execution is the set of writes it performed.
    tx.speculative_writes = tx.writes
    
    return tx, tx.reads, tx.writes

# --- Phase 2: DAG Construction ---

def build_dependency_dag(transactions: List[Transaction]) -> nx.DiGraph:
    """
    Builds a Directed Acyclic Graph (DAG) of transaction dependencies.
    An edge from T_A to T_B means T_A must be processed before T_B.
    """
    print("\n--- Phase 2: Building Dependency DAG ---")
    dag = nx.DiGraph()
    tx_map = {tx.id: tx for tx in transactions}

    # Add all transactions as nodes
    for tx in transactions:
        dag.add_node(tx.id)

    # Add edges based on dependencies (RAW, WAW, WAR)
    for i in range(len(transactions)):
        for j in range(i + 1, len(transactions)):
            ti = transactions[i]
            tj = transactions[j]

            ti_writes = set(ti.writes.keys())
            ti_reads = ti.reads
            tj_writes = set(tj.writes.keys())
            tj_reads = tj.reads

            # Read-After-Write (RAW): tj reads what ti writes. Classic dependency.
            if not ti_writes.isdisjoint(tj_reads):
                print(f"  Adding RAW edge: {ti.id} -> {tj.id} (on {ti_writes.intersection(tj_reads)})")
                dag.add_edge(ti.id, tj.id)
                continue # An edge is enough

            # Write-After-Write (WAW): tj overwrites what ti writes. Must preserve order.
            if not ti_writes.isdisjoint(tj_writes):
                print(f"  Adding WAW edge: {ti.id} -> {tj.id} (on {ti_writes.intersection(tj_writes)})")
                dag.add_edge(ti.id, tj.id)
                continue

            # Write-After-Read (WAR): tj writes what ti reads. Anti-dependency.
            if not ti_reads.isdisjoint(tj_writes):
                print(f"  Adding WAR edge: {ti.id} -> {tj.id} (on {ti_reads.intersection(tj_writes)})")
                dag.add_edge(ti.id, tj.id)
                continue
                
    # Check for cycles, which would indicate a logical impossibility in a serial block
    if not nx.is_directed_acyclic_graph(dag):
        raise ValueError("Execution plan contains a cycle and cannot be resolved.")

    return dag

# --- Phase 3: Validation and Commit using the DAG ---

def validate_and_commit(transactions: List[Transaction], dag: nx.DiGraph) -> List[Transaction]:
    """
    Validates transactions based on the DAG's topological sort and commits them.
    """
    print("\n--- Phase 3: Validating and Committing ---")
    
    # A topological sort gives a linear ordering that respects dependencies.
    # This is our final, valid transaction order for the block.
    try:
        execution_order = list(nx.topological_sort(dag))
        print(f"  Valid execution order determined by DAG: {execution_order}")
    except nx.NetworkXUnfeasible:
        print("  Error: A cycle was detected in the transaction dependencies. Cannot create a valid block.")
        return []

    finalized_transactions = []
    committed_writes = {} # Simulates the final state after each commit
    tx_map = {tx.id: tx for tx in transactions}

    for tx_id in execution_order:
        tx = tx_map[tx_id]
        
        # In this simulation, the DAG already resolved conflicts.
        # A real implementation would check R(tx) against committed_writes.
        # If a conflict exists, it would re-execute tx against the current state.
        # Here, we just accept the speculative results as valid for this ordering.
        print(f"  Committing {tx}...")
        
        # Apply the speculative writes to the committed state
        for key, value in tx.speculative_writes.items():
            committed_writes[key] = value
            
        finalized_transactions.append(tx)
        
    print("\n  All transactions successfully committed in dependency-aware order.")
    return finalized_transactions

# --- Phase 4: Block Finalization (BAL Generation) ---

def generate_bal(finalized_transactions: List[Transaction]) -> BlockAccessList:
    """
    Generates the final EIP-7928 Block Access List from the validated transactions.
    """
    print("\n--- Phase 4: Generating Block Access List (BAL) ---")
    bal = BlockAccessList()
    
    # The transaction index in the final block (starts at 1 per EIP-7928 spec)
    for tx_idx, tx in enumerate(finalized_transactions, 1):
        # Process writes
        for key, value in tx.speculative_writes.items():
            address, slot = key.split(':')
            bal.add_write(tx_idx, address, slot, value)
        
        # Process reads
        for key in tx.reads:
            address, slot = key.split(':')
            bal.add_read(tx_idx, address, slot)
            
    print("  BAL generation complete.")
    return bal

# --- Main Simulation ---

def main():
    """Main function to run the full simulation."""
    print("--- POBE: Parallel Optimistic Block Execution Simulation ---")

    # 1. Define initial state and a set of mock transactions
    # Format for state keys: "address:slot"
    initial_state = {
        "0xA:slot1": "valueA1",
        "0xB:slot1": "valueB1",
        "0xC:slot1": "valueC1",
    }

    # Transactions designed to create dependencies
    transactions = [
        Transaction(tx_id=1, reads={"0xA:slot1"}, writes={"0xA:slot1": "vA2"}), # T1 writes to A
        Transaction(tx_id=2, reads={"0xA:slot1"}, writes={"0xB:slot1": "vB2"}), # T2 reads A (depends on T1)
        Transaction(tx_id=3, reads={"0xC:slot1"}, writes={"0xC:slot1": "vC2"}), # T3 is independent
        Transaction(tx_id=4, reads={"0xB:slot1"}, writes={"0xC:slot1": "vC3"}), # T4 reads B (depends on T2) and writes C (conflict with T3)
        Transaction(tx_id=5, reads={"0xE:slot1"}, writes={"0xE:slot1": "vE2"}), # T5 is independent
    ]
    print(f"\nInitial Pool of {len(transactions)} transactions.")

    # --- Phase 1 ---
    print("\n--- Phase 1: Parallel Speculative Execution ---")
    executed_txs = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(speculative_execute, tx, initial_state) for tx in transactions]
        for future in concurrent.futures.as_completed(futures):
            tx, _, _ = future.result()
            executed_txs.append(tx)
    
    # For consistency, sort by ID before building DAG
    executed_txs.sort(key=lambda t: t.id)
    print("  All transactions executed speculatively.")

    # --- Phase 2 ---
    dag = build_dependency_dag(executed_txs)

    # --- Phase 3 ---
    finalized_transactions = validate_and_commit(executed_txs, dag)

    # --- Phase 4 ---
    if finalized_transactions:
        final_bal = generate_bal(finalized_transactions)
        
        # --- Final Output ---
        print("\n--- ✅ Block Finalized Successfully ---")
        print(f"Final Transaction Order: {[tx.id for tx in finalized_transactions]}")
        
        print("\nGenerated Block-Level Access List (EIP-7928 format):")
        import json
        print(json.dumps(final_bal.to_formatted_list(), indent=2))

if __name__ == "__main__":
    main()

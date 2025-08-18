Constructing the Transaction Dependency DAG
After the parallel speculative execution phase (Phase 1), you have the Read Set R(T) and Write Set W(T) for every transaction T in the proposed block. You can now build the graph.

1. Nodes: Each transaction T_i in the block becomes a node in the DAG.

2. Edges: A directed edge is drawn from transaction T_A to T_B (i.e., T_A -> T_B) if T_B depends on a state change made by T_A. This occurs if any of the following classic data hazards are present, assuming T_A comes before T_B in the initial proposed order:

Read-After-Write (RAW): T_B reads a state slot that T_A writes to.

W(T_A) ∩ R(T_B) ≠ ∅

Write-After-Write (WAW): T_B writes to the same state slot that T_A writes to.

W(T_A) ∩ W(T_B) ≠ ∅

Write-After-Read (WAR): T_B writes to a state slot that T_A reads from.

R(T_A) ∩ W(T_B) ≠ ∅

An edge T_A -> T_B means "T_A must be finalized before T_B can be validated."

Example Scenario
Imagine a block with four transactions that have been speculatively executed:

T1: Writes to storage slot 0x100.

T2: Reads from storage slot 0x100 and writes to account balance 0xAAAA.

T3: Reads from account balance 0xAAAA.

T4: Reads and writes to storage slot 0x200.

The resulting DAG would look like this:

Nodes: T1, T2, T3, T4.

Edges:

T1 -> T2: A RAW dependency exists because T2 reads the storage slot 0x100 that T1 writes.

T2 -> T3: A RAW dependency exists because T3 reads the account balance 0xAAAA that T2 writes.

Structure: The graph shows a dependency chain T1 -> T2 -> T3. Transaction T4 is an isolated node because its access list (0x200) does not overlap with any others.

Using the DAG in the OCC Workflow 🗺️
Building this DAG fundamentally enhances the validation phase (Phase 2).

Identify Independent Groups: The DAG immediately reveals transactions and subgraphs that are completely independent. In our example, T4 is independent of the T1-T2-T3 chain. This means the speculative results for T4 can be considered valid and committed without waiting for the others. You can process all such disconnected components of the graph in parallel.

Efficient Invalidation: The graph structure contains dependency information. If, during validation, you discover that a transaction T_X must be re-executed (e.g., due to a conflict with a transaction from a previous block not accounted for), you don't need to re-evaluate every subsequent transaction. You only need to invalidate and re-execute T_X and all of its descendants in the DAG (i.e., all transactions reachable from T_X).

Topological Sort for Committing: To finalize the block, you perform a topological sort on the DAG. This produces a linear ordering of transactions that respects all dependencies. You can then iterate through this sorted list to commit the results, re-executing only when a transaction's parent in the graph was changed.

This DAG-based approach transforms the validation process from a simple linear scan into a more intelligent, structured analysis of the block's internal dependencies, maximizing parallelism and minimizing re-execution work.

-----OG-----
# EIP-7928 Block Access Lists Implementation

Implementation of [EIP-7928 Block-Level Access Lists (BALs)](https://eips.ethereum.org/EIPS/eip-7928) for Ethereum blockchain analysis with comprehensive SSZ vs RLP encoding comparison.

## Overview

Block Access Lists provide a structured way to represent storage accesses, balance changes, code deployments, and nonce modifications within an Ethereum block. This implementation includes both SSZ and RLP encodings for detailed size analysis and performance comparison.

## Features

- **Full EIP-7928 compliance** with both standard and optimized variants
- **Dual encoding support**: SSZ (Ethereum 2.0 standard) and RLP (Ethereum 1.0 standard)
- **Comprehensive size analysis** with raw and snappy-compressed comparisons  
- **Optimized simple ETH transfer detection** using gas-based method with batch RPC calls
- **Real block validation** against actual Ethereum mainnet blocks
- **Performance optimization** with separate read/write storage tracking
- **Builder pattern architecture** for flexible BAL construction  

## Setup

```bash
git clone https://github.com/your-username/eth-bal-analysis.git
cd eth-bal-analysis
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
echo "YOUR_RPC_URL" > rpc.txt
```

## Usage

### Generate BALs with Current Implementation

```bash
# Generate SSZ BALs without storage reads
python src/bal_builder.py --no-reads

# Generate SSZ BALs with storage reads (default)
python src/bal_builder.py

# Generate RLP BALs without storage reads
python src/bal_builder_rlp.py --no-reads

# Generate RLP BALs with storage reads (default)
python src/bal_builder_rlp.py

# Create comprehensive analysis report
python create_analysis_report.py
```

### SSZ Implementation (Builder Pattern)

```python
from src.bal_builder import *
from src.BALs import *
from src.helpers import *

# Fetch block data
block_number = 22616000
trace_result = fetch_block_trace(block_number, RPC_URL)

# Create builder and process components
builder = BALBuilder()
touched_addresses = collect_touched_addresses(trace_result)
simple_transfers = identify_simple_eth_transfers(block_number, RPC_URL)

# Extract all components into the builder
process_storage_changes(trace_result, None, ignore_reads=True, builder=builder)
process_balance_changes(trace_result, builder, touched_addresses, simple_transfers)
process_code_changes(trace_result, builder)
process_nonce_changes(trace_result, builder)

# Build and encode BAL
block_obj = builder.build(ignore_reads=True)
block_obj_sorted = sort_block_access_list(block_obj)
encoded_bal = ssz.encode(block_obj_sorted, sedes=BlockAccessList)
```

### RLP Implementation (Builder Pattern)

```python
from src.bal_builder_rlp import *
from src.BALs_rlp import *
from src.helpers import *

# Fetch block data (identical process)
block_number = 22616000
trace_result = fetch_block_trace(block_number, RPC_URL)

# Create builder and process components
builder = BALBuilder()
touched_addresses = collect_touched_addresses(trace_result)
simple_transfers = identify_simple_eth_transfers(block_number, RPC_URL)

# Extract all components into the builder
process_storage_changes(trace_result, None, ignore_reads=True, builder=builder)
process_balance_changes(trace_result, builder, touched_addresses, simple_transfers)
process_code_changes(trace_result, builder)
process_nonce_changes(trace_result, builder)

# Build and encode BAL (RLP encoding)
block_obj = builder.build(ignore_reads=True)
block_obj_sorted = sort_block_access_list(block_obj)
encoded_bal = rlp.encode(block_obj_sorted)
```

### Size Analysis

```python
from src.helpers import compare_ssz_rlp_sizes, analyze_component_sizes

# Compare encoding sizes
comparison = compare_ssz_rlp_sizes(ssz_encoded, rlp_encoded)
print(f"Raw size ratio (RLP/SSZ): {comparison['comparison']['raw_size_ratio']:.3f}")
print(f"Compressed size ratio: {comparison['comparison']['compressed_size_ratio']:.3f}")

# Component-level analysis
ssz_components = {'storage': ssz_storage_diff, 'balance': ssz_balance_diff}
rlp_components = {'storage': rlp_storage_diff, 'balance': rlp_balance_diff}
analysis = analyze_component_sizes(ssz_components, rlp_components, ['storage', 'balance'])
```

## Testing

```bash
cd tests
python test_recent_blocks.py
python comprehensive_eip7928_tests.py
python edge_case_tests.py
```

## Components

- `src/BALs.py` - EIP-7928 data structures with SSZ serialization
- `src/BALs_rlp.py` - EIP-7928 data structures with RLP serialization
- `src/bal_builder.py` - SSZ BAL builder with optimized simple transfer detection
- `src/bal_builder_rlp.py` - RLP BAL builder with identical logic, different encoding
- `src/helpers.py` - RPC utilities, size analysis, and gas-based simple transfer detection
- `tests/` - Comprehensive test suite
- `reports/` - Analysis reports comparing encoding formats and read strategies
- `bal_raw/` - Generated BAL files and analysis data
  - `ssz/` - SSZ-encoded BALs and analysis results
  - `rlp/` - RLP-encoded BALs and analysis results

## License

MIT License - see [LICENSE](LICENSE) file.

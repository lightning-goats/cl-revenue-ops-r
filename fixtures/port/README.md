# Generated whole-plugin inventory

`plugin_inventory.json` is the honest Rust-port surface snapshot. It pins the
Python authority commit and source hashes, lists all 69 Python RPCs, keeps the
four Rust-only names separate, embeds all 121 Python options, records the eight
startup/business loops plus bounded shutdown, and scaffolds every external
mutation/adaptor class.

RPC `effective` is deliberately not a boolean. `full`, `partial`,
`placeholder`, `unreachable`, and `absent` are distinct so a compiled or
success-shaped refusal cannot be counted as parity. Transport, review, and soak
state are separate fields for the same reason.

`rpc_params.json` is the 69-entry source-signature contract consumed by the
common decoder in `crates/revops/src/rpc_params.rs`. The source schema records
Python positional-or-named binding. An incomplete Rust handler may explicitly
choose the decoder's named-only policy, which returns a typed refusal before
the handler executes; it is not represented as Python parity.

Regenerate from the pinned git object without reading a Python working tree:

```bash
python3 tools/port/gen_plugin_inventory.py \
  --python-repo /path/to/cl_revenue_ops \
  --python-commit a5c2e2f65019df5cefe4e1261b7de2823a03e448 \
  --repo-root .
```

Use `--check` in verification to refuse any stale checked-in artifact. CI
checks out the Python repository at the exact pinned commit, runs the focused
Python tests, and runs this drift check, so it never depends on a host-specific
path. The generator also refreshes `fixtures/options.json`; this is what restores
the two fee-authority/replay-capture options that the prior 119-entry fixture
omitted.

External-boundary evidence is derived by structurally scanning every pinned
production file (`cl-revenue-ops.py` plus `modules/**/*.py`), never a first text
match. The scan is compared with an exact expected callsite set, and provenance
hashes every inspected file. The capture writer's `git rev-parse` subprocess is
explicitly excluded as observational source identity, not an external mutation.
New Rust RPC registrations fail closed until explicitly classified. Reachability does not imply review: only methods named in the
review-evidence registry are marked passed; all others remain pending. Rust
provenance records the committed source commit, tree, and blob that supplied the
inspected `main.rs`, and generation refuses a dirty replacement.

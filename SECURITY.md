# Security Policy

`cl-revenue-ops-r` is security-sensitive financial infrastructure.

The software interacts with Core Lightning and may analyze, recommend, authorize, or execute actions involving Lightning channels, liquidity, routing fees, rebalancing, swaps, channel lifecycle operations, and other behavior involving real Bitcoin.

A defect in this software may therefore result in more than application failure. Depending on configuration and enabled capabilities, failures may cause financial loss, locked liquidity, unintended channel operations, disclosure of sensitive node information, or degradation of a production Lightning node.

Security vulnerabilities should be reported privately.

**Do not open a public GitHub issue containing details of an unpatched vulnerability.**

## Supported Versions

Security fixes are provided primarily for the latest maintained release and current development branch.

| Version                   | Supported   |
| ------------------------- | ----------- |
| `main` / latest release   | ✅           |
| Older maintained releases | Best effort |
| Unmaintained versions     | ❌           |
| Third-party forks         | ❌           |

Operators should run the latest stable release whenever practical.

## Reporting a Vulnerability

Use GitHub's **Private Vulnerability Reporting** feature for this repository when available.

If private vulnerability reporting is unavailable, contact the maintainers privately before publicly disclosing technical details.

Please include as much of the following information as possible:

* A description of the vulnerability.
* The affected release, commit, or branch.
* The affected crate, module, RPC method, subsystem, or execution path.
* Preconditions required for exploitation.
* Reproduction instructions.
* A minimal proof of concept where appropriate.
* Expected behavior.
* Observed behavior.
* Potential impact.
* Whether funds, credentials, availability, privacy, or data integrity may be affected.
* Relevant logs with secrets removed.
* Any proposed mitigation or patch.

When reporting memory-safety problems, please also provide relevant information from tools such as:

* Miri
* AddressSanitizer
* ThreadSanitizer
* UndefinedBehaviorSanitizer
* Loom
* Valgrind

when applicable.

Never include real:

* `hsm_secret`
* wallet seed material
* private keys
* RPC credentials
* API credentials
* Nostr private keys
* access tokens
* database passwords
* production authentication material

in a vulnerability report.

## Response Process

Maintainers will make a reasonable effort to:

1. Acknowledge the report.
2. Reproduce and assess the issue.
3. Determine severity and affected versions.
4. Identify immediate mitigations where appropriate.
5. Develop and test a fix.
6. Coordinate disclosure with the reporter.
7. Publish an advisory when users need to take action.

Issues involving Core Lightning internals, protocol behavior, external service interaction, concurrency, or unusual production configurations may require additional investigation.

Please allow a reasonable remediation period before public disclosure.

# Security Model

`cl-revenue-ops-r` should be treated as a **financial control system**, not merely as an analytics application.

The software should operate under the assumption that:

* The Core Lightning node controls real Bitcoin.
* Lightning peers are untrusted.
* External services may fail or behave maliciously.
* Data may be stale, incomplete, malformed, inconsistent, or adversarial.
* Processes may crash or restart at arbitrary points.
* RPC calls may succeed even when their result is not received.
* Network timeouts do not imply operation failure.
* Duplicate financial execution can be dangerous.
* Configuration mistakes will occur.
* Concurrent state changes can invalidate previously correct decisions.
* A policy algorithm can produce an unsafe decision without violating memory safety.
* Correct Rust code can still produce economically incorrect behavior.

Whenever practical, executable financial operations should follow a lifecycle similar to:

**observe → normalize → snapshot → evaluate → construct typed intent → validate → arbitrate → authorize → execute → reconcile → verify → persist**

Decision logic should remain separated from execution logic wherever feasible.

# Security Scope

The following classes of vulnerabilities are considered particularly important.

## Loss or Misallocation of Funds

Examples include:

* Unauthorized Lightning or on-chain spending.
* Incorrect payment amount or destination.
* Incorrect channel open or close behavior.
* Repeated financial execution.
* Incorrect rebalance execution.
* Unsafe swap execution.
* Incorrect fee changes causing material financial loss.
* Bypassing configured budgets.
* Bypassing capital constraints.
* Bypassing profitability constraints.
* Unintended locking of liquidity.
* Unit conversion errors.
* Overflow, underflow, truncation, or rounding causing economically different behavior.

A vulnerability does not need to directly steal Bitcoin to be security relevant.

A defect capable of materially violating an operator's configured capital policy is considered security sensitive.

## Authorization and Policy Bypass

Examples include:

* Executing an action that should have remained advisory.
* Bypassing an approval or arbitration layer.
* Bypassing a configured budget.
* Ignoring an execution kill switch.
* Bypassing liquidity or concentration limits.
* Circumventing profitability protections.
* Executing an intent whose validation failed.
* Executing a stale or expired operation.
* Changing security-sensitive configuration without appropriate validation.

## Typed Intent Integrity

Where actions are represented using typed intents or equivalent structures, vulnerabilities include:

* Executing the same intent more than once.
* Intent identifier collisions.
* Replaying completed intents.
* Mutating an intent after authorization.
* Changing a serialized intent's financial meaning.
* Executing an expired intent.
* Confusing intent variants.
* Deserializing malformed values into valid executable actions.
* Failing to bind an intent to the intended node.
* Failing to bind an intent to the intended channel or peer.
* Failing to bind an intent to the authorized amount.
* Executing an intent under different policy state than the one used for authorization.

Security-sensitive intent types should be designed to make invalid states difficult or impossible to represent.

## Idempotency Failures

Financial RPC operations must be designed around the possibility of uncertain execution.

A timeout such as:

```text
request sent → operation succeeds → connection fails → caller sees error
```

must not automatically cause a second execution.

Security issues include:

* Blind retries.
* Duplicate payment submission.
* Duplicate swaps.
* Duplicate channel opens.
* Duplicate rebalance execution.
* Repeated close requests.
* Loss of idempotency state after restart.
* Incorrect reconciliation following ambiguous RPC results.

Where possible:

* Assign stable identities to executable operations.
* Persist execution state.
* Reconcile ambiguous outcomes against Core Lightning.
* Verify resulting node state.
* Do not blindly retry non-idempotent operations.

## Core Lightning RPC Boundary

The Core Lightning RPC interface is part of the security boundary.

Relevant vulnerabilities include:

* Calling unexpected privileged methods.
* Constructing RPC requests from unvalidated input.
* Incorrect parameter serialization.
* Trusting malformed RPC responses.
* Failing to verify channel or peer identifiers.
* Misinterpreting Core Lightning state.
* Treating missing values as safe defaults.
* Allowing attacker-controlled data to alter RPC method selection.
* RPC response confusion between different requests.

All external RPC data should be treated as untrusted input until validated.

# Rust-Specific Security Requirements

Rust substantially reduces several classes of memory-safety vulnerabilities, but Rust does not automatically make financial software safe.

## `unsafe` Code

Use of `unsafe` should be minimized.

Every `unsafe` block should have a concrete safety invariant that explains why the operation is valid.

Prefer:

```rust
// SAFETY: ...
unsafe {
    ...
}
```

for non-trivial unsafe operations.

Security review should pay particular attention to:

* raw pointers;
* unchecked indexing;
* `transmute`;
* `MaybeUninit`;
* custom allocators;
* FFI;
* manual `Send` implementations;
* manual `Sync` implementations;
* unsafe global state;
* lifetime extension;
* aliasing assumptions.

An `unsafe` block that cannot be explained with a concise safety invariant should generally be redesigned.

Run Miri against code containing significant unsafe behavior where practical.

## Forbidden or Restricted Unsafe Usage

Financial decision and authorization logic should preferably contain **no unsafe code**.

Consider using:

```rust
#![forbid(unsafe_code)]
```

for crates that do not require unsafe operations.

If unsafe code is unavoidable because of FFI or a dependency boundary, isolate it into a small, auditable module or crate.

## Foreign Function Interfaces

FFI introduces a memory-safety boundary outside Rust's guarantees.

Validate:

* pointer lifetime;
* ownership;
* allocation origin;
* string termination;
* buffer lengths;
* nullability;
* alignment;
* integer width;
* calling convention;
* thread-safety assumptions.

Never assume that data received through FFI satisfies Rust invariants.

## Integer Safety

Bitcoin and Lightning software perform extensive integer arithmetic.

Financial calculations should use integer types whenever possible.

Particular attention should be given to:

* multiplication before division;
* conversion between signed and unsigned values;
* narrowing conversions;
* overflow;
* underflow;
* saturating operations;
* truncating casts;
* rounding direction.

Do not rely on debug-mode overflow checks as a security control.

Use explicit checked operations where failure should invalidate an action:

```rust
checked_add
checked_sub
checked_mul
checked_div
```

Use saturating arithmetic only where saturation is explicitly the desired financial behavior.

Avoid unrestricted `as` casts in security-sensitive financial conversions.

Prefer checked conversions such as:

```rust
u64::try_from(value)
```

when loss of information could alter financial behavior.

# Financial Units

Lightning software commonly mixes:

* BTC
* satoshis
* millisatoshis
* ppm
* basis points
* percentages
* fiat reference amounts

These should not be represented as interchangeable primitive numbers across security-sensitive APIs.

Prefer strongly typed wrappers such as conceptually:

```rust
struct Sats(u64);
struct Msats(u64);
struct FeePpm(u32);
```

or equivalent domain types.

Avoid APIs where this compiles:

```rust
rebalance(channel, 5000);
```

but the caller cannot determine whether `5000` means sats, msats, ppm, or something else.

Unit conversions should be centralized and heavily tested.

Important boundaries include:

* BTC ↔ sat
* sat ↔ msat
* ppm calculations
* percentage calculations
* proportional fee calculations
* profitability calculations
* budget calculations

Financial arithmetic should explicitly define rounding behavior.

# Floating-Point Use

Avoid floating-point numbers for values whose exact interpretation determines Bitcoin movement.

Do not use floating point as the authoritative representation of:

* satoshi amounts;
* millisatoshi amounts;
* payment amounts;
* channel funding amounts;
* exact fee budgets.

Floating-point calculations may be acceptable for analytics, scoring, forecasting, or statistical calculations when their result is subsequently bounded by integer-valued policy constraints.

NaN and infinity should never silently propagate into executable decisions.

Validate all floating-point inputs with checks such as:

```rust
value.is_finite()
```

where applicable.

# Panics

A panic should not be an expected result of malformed runtime data.

Avoid security-sensitive use of:

```rust
unwrap()
expect()
panic!()
unreachable!()
```

on values influenced by:

* RPC responses;
* peers;
* external APIs;
* configuration;
* persisted state;
* user input.

Panics in startup assertions or logically impossible internal invariants may be acceptable when carefully justified.

Production handling of malformed external data should normally return a structured error and fail closed.

# Error Handling

Do not discard errors that affect financial state.

Be particularly cautious with:

```rust
let _ = ...
```

around:

* database writes;
* RPC calls;
* intent persistence;
* authorization records;
* locks;
* financial execution;
* audit records.

Errors should retain enough context to identify the failed operation without leaking secrets.

# Concurrency

Concurrency bugs can become financial vulnerabilities even when Rust's ownership system prevents data races.

Relevant issues include:

* two tasks executing against the same channel;
* overlapping fee cycles;
* overlapping rebalance cycles;
* duplicate execution after retries;
* budget checks occurring concurrently;
* time-of-check/time-of-use races;
* snapshot state changing between authorization and execution.

Financial operations should use appropriate:

* arbitration;
* serialization;
* locks;
* leases;
* atomic transitions;
* transactional persistence;
* per-resource execution guards.

Locks should not be held across slow network operations unless specifically designed for that behavior.

Always consider deadlock and lock-ordering risks.

## Async Rust

Async execution creates additional state-consistency concerns.

Review carefully:

* cancellation safety;
* task abandonment;
* timeout handling;
* retries;
* partial state transitions;
* channels that can grow without bounds;
* tasks holding locks across `.await`.

Cancellation should not leave an operation in a state where the software cannot determine whether financial execution occurred.

Bounded channels should generally be preferred for high-volume external input.

# Snapshot Consistency

Financial policy should operate against a coherent view of node state.

Security problems may arise if one policy evaluates:

```text
state at T1
```

while another related policy evaluates:

```text
state at T2
```

and their outputs are subsequently treated as if they were derived from the same state.

Where practical, policy decisions should derive from a:

* canonical;
* immutable;
* versioned;
* timestamped

snapshot for the evaluation cycle.

Snapshots should contain freshness information.

Stale data should not silently become executable policy.

# Stale State

Security-sensitive data should have explicit freshness limits.

This may include:

* channel balances;
* peer state;
* routing data;
* fee data;
* profitability state;
* external market data;
* swap quotes.

When required state exceeds its freshness limit, the preferred behavior is normally to suppress new capital-moving actions.

# Serialization and Deserialization

Serialized state must be treated as untrusted.

This applies even to locally persisted state because files may be:

* corrupted;
* partially written;
* manually modified;
* restored from an incompatible version;
* produced by older software.

Validate:

* enum discriminants;
* numeric ranges;
* identifiers;
* timestamps;
* versions;
* required fields;
* financial amounts.

Avoid automatically defaulting malformed security-sensitive fields.

Schema changes should be versioned.

Migration code should be tested against unexpected and incomplete historical data.

# Persistence and Database Security

Persistent state involved in financial execution must account for:

* partial writes;
* crashes;
* restarts;
* duplicate records;
* migrations;
* concurrent writers;
* corruption;
* backup restoration.

For executable operations, persistence should ideally allow reconstruction of:

* what was proposed;
* which snapshot produced it;
* why it was authorized;
* which policy limits were applied;
* whether execution was attempted;
* what Core Lightning returned;
* whether execution was verified;
* final disposition.

Critical state transitions should be transactional where practical.

# Secret Management

The plugin should not require access to Core Lightning's `hsm_secret`.

If a feature appears to require direct access to signing secrets, reconsider the architecture before implementing it.

The Core Lightning RPC boundary should normally provide the necessary capability.

Never log:

* `hsm_secret`
* seed phrases
* private keys
* passwords
* API secrets
* authentication tokens
* full sensitive configuration values

Secrets should not be:

* committed to Git;
* embedded in binaries;
* placed in test fixtures;
* exposed in panic messages;
* printed through `Debug`;
* returned through diagnostic RPC methods.

Be cautious when deriving `Debug` or `Serialize` for structures that contain credentials.

# Sensitive Logging

Security-sensitive operations should produce sufficient audit information to reconstruct decisions.

Useful fields include:

* intent ID;
* intent type;
* peer ID;
* channel identifier;
* snapshot version;
* policy result;
* suppression reason;
* authorization decision;
* execution status.

Logs must not expose authentication secrets.

Consider whether log entries themselves reveal sensitive operational strategy or private node information before enabling verbose logging by default.

# External Inputs

Treat input from the following sources as untrusted:

* Lightning peers;
* gossip;
* invoices;
* BOLT messages;
* Core Lightning RPC responses;
* plugin notifications;
* datastore entries;
* configuration;
* environment variables;
* command-line parameters;
* external APIs;
* swap providers;
* market-data services;
* Nostr or other messaging systems;
* persisted JSON or database state.

Validate both type and semantic meaning.

A validly parsed value is not necessarily safe.

# External Services

Third-party information should generally be treated as advisory.

A compromised service should not be able to independently cause unrestricted movement of node funds.

External clients should use:

* TLS verification;
* explicit timeouts;
* bounded retries;
* response-size limits;
* schema validation;
* sensible rate limiting.

Avoid indefinite waits.

Avoid recursive or unlimited retries.

Circuit-breaker behavior may be appropriate for repeatedly failing services.

# Denial of Service

Reports are in scope when untrusted data can cause:

* `lightningd` disruption;
* plugin termination;
* excessive CPU consumption;
* uncontrolled memory growth;
* unbounded async task creation;
* unbounded queues;
* excessive database growth;
* infinite retry loops;
* excessive Core Lightning RPC requests;
* deadlocks;
* starvation;
* pathological algorithmic behavior.

Attacker-controlled collections should have explicit limits where practical.

# Resource Exhaustion

Rust's memory safety does not prevent memory exhaustion.

Be particularly careful with:

```rust
Vec
String
HashMap
BTreeMap
mpsc::unbounded_channel
```

when their size can be influenced by external data.

Use:

* maximum message sizes;
* bounded queues;
* bounded caches;
* TTLs;
* eviction;
* request limits

where appropriate.

# Dependency and Supply-Chain Security

Cargo dependencies are part of the trusted computing base.

The project should minimize unnecessary dependencies, especially in financial execution paths.

Before adding a dependency, consider:

* maintenance status;
* ownership;
* release history;
* transitive dependency count;
* unsafe code;
* build scripts;
* native dependencies;
* network behavior.

Run tools such as:

```text
cargo audit
cargo deny
```

as appropriate.

Security-sensitive projects should consider policies covering:

* known vulnerabilities;
* banned crates;
* allowed licenses;
* duplicate dependency versions;
* unknown registries;
* Git dependencies.

## Cargo Lockfile

Applications and deployable binaries should commit `Cargo.lock`.

Production builds should normally use:

```text
cargo build --locked
```

or equivalent enforcement.

Unexpected dependency resolution should not occur during production deployment.

## Build Scripts

`build.rs` executes code at build time.

Treat crates containing build scripts as more privileged than ordinary source dependencies.

Avoid unnecessary build dependencies.

Review build-script behavior for security-sensitive dependencies.

## Procedural Macros

Procedural macros execute during compilation.

They are part of the build-time trusted computing base and should be treated accordingly.

# Reproducible and Auditable Builds

Where feasible:

* pin dependency versions;
* use the committed lockfile;
* build from tagged commits;
* record compiler versions;
* avoid unexplained binary artifacts;
* produce release hashes;
* prefer CI-built release artifacts with provenance.

Release builds should not depend on mutable remote resources.

# Compiler and Linting

Security-sensitive code should maintain a high warning standard.

CI should normally include:

```text
cargo fmt --check
cargo clippy
cargo test
```

with an appropriately strict Clippy configuration.

Warnings affecting:

* numeric conversions;
* ignored results;
* suspicious casts;
* locking;
* async behavior;
* unsafe code

should receive particular attention.

# Testing Expectations

Capital-moving code requires stronger tests than normal application functionality.

Important test categories include:

* boundary-value tests;
* malformed-input tests;
* restart tests;
* replay tests;
* duplicate-execution tests;
* idempotency tests;
* timeout tests;
* stale-state tests;
* concurrency tests;
* amount-conversion tests;
* overflow tests;
* underflow tests;
* serialization tests;
* schema-migration tests;
* authorization-bypass tests;
* maximum-budget tests;
* external-service failure tests.

# Property-Based Testing

Financial invariants are well suited to property-based testing.

Examples include invariants such as:

```text
execution_amount <= authorized_amount
```

```text
total_spend <= configured_budget
```

```text
msat_to_sat(x) cannot create value
```

```text
the same intent cannot produce two completed executions
```

```text
an expired intent cannot execute
```

```text
invalid snapshots cannot authorize capital movement
```

Libraries such as `proptest` or equivalent tooling may be useful.

# Fuzzing

Parsers and externally influenced serialization boundaries should be fuzzed where practical.

High-value fuzz targets include:

* RPC response parsing;
* configuration parsing;
* persisted state;
* intent deserialization;
* external API responses;
* protocol-related parsers.

`cargo-fuzz` or equivalent tooling may be appropriate.

# Concurrency Testing

Concurrency-sensitive financial state should receive dedicated testing.

Where applicable, tools such as Loom can help verify assumptions about:

* atomic operations;
* locks;
* concurrent state machines;
* execution guards.

# Fail-Safe Behavior

When security-sensitive state is:

* missing;
* malformed;
* stale;
* internally inconsistent;
* unavailable;
* ambiguous

the preferred behavior is:

**do not initiate a new capital-moving action.**

Read-only analysis may continue when safe.

Capital-moving operations should generally fail closed.

Examples include suppressing execution when:

* the canonical snapshot is stale;
* an amount cannot be validated;
* channel identity cannot be confirmed;
* policy evaluation fails;
* authorization state is unavailable;
* persistence fails;
* intent state is ambiguous;
* execution conflicts with another active operation.

# Runtime Isolation

The plugin should run with no more operating-system privileges than required.

Operators should avoid running the Core Lightning process as `root`.

Files should use restrictive permissions.

Particularly sensitive resources include:

* Core Lightning directories;
* configuration;
* databases;
* backups;
* API credentials.

Where appropriate, system-level hardening may include:

* dedicated service accounts;
* filesystem restrictions;
* systemd sandboxing;
* limited network access;
* read-only filesystems for immutable resources.

Hardening must not interfere with Core Lightning's required security behavior.

# Production Deployment

New capital-affecting functionality should not first execute against substantial production funds.

Recommended rollout:

1. Review the change.
2. Run the complete test suite.
3. Run security and dependency checks.
4. Test against regtest, signet, Polar, or another controlled environment.
5. Deploy in advisory, shadow, dry-run, or constrained mode where available.
6. Review proposed decisions.
7. Enable execution with conservative limits.
8. Monitor resulting Core Lightning state.
9. Increase limits only after demonstrated stability.

Operators should maintain an immediate way to disable the plugin.

# Kill Switches and Safety Limits

Autonomous financial systems should contain independent limits where appropriate.

Examples include:

* maximum amount per operation;
* maximum aggregate daily expenditure;
* maximum rebalance cost;
* maximum swap amount;
* maximum channel-open amount;
* maximum number of concurrent operations;
* minimum profitability requirements;
* minimum liquidity reserves.

Safety limits should be enforced as close to the execution boundary as practical.

Do not rely solely on upstream planning logic to enforce a critical financial cap.

# Trust Boundaries

The project should explicitly distinguish between:

**trusted control state**

and

**untrusted observations**.

Observations from peers, network gossip, APIs, or other agents should not directly become executable instructions.

A safe conceptual model is:

```text
untrusted observation
        ↓
validation
        ↓
normalized state
        ↓
policy
        ↓
typed intent
        ↓
authorization
        ↓
execution boundary
```

Every transition should reduce ambiguity rather than add it.

# Security Review of Pull Requests

Changes deserve additional scrutiny when they modify:

* Core Lightning RPC invocation;
* intent structures;
* amount calculations;
* authorization;
* execution;
* persistence;
* retry behavior;
* concurrency;
* swap integration;
* channel management;
* configuration defaults;
* unsafe code;
* FFI;
* dependency versions.

A small diff can have large financial consequences.

Review severity should therefore be based on **economic reach**, not only code size.

# Out of Scope

The following normally do not constitute vulnerabilities in `cl-revenue-ops-r`:

* Normal Lightning routing losses.
* Expected fee-market volatility.
* Poor economic performance resulting from otherwise correctly functioning policy.
* Protocol-permitted counterparty behavior.
* Documented operational risks.
* Unsupported historical versions.
* Generic dependency CVEs with no realistic impact on this project.
* Purely theoretical vulnerabilities without a plausible attack path.
* Automated scanner output without demonstrated impact.
* Issues requiring complete root compromise unless the issue materially expands the attacker's capability.

Unexpected financial behavior should still be reported when it violates an explicit security, authorization, or capital boundary.

# Responsible Disclosure

We appreciate researchers who:

* report vulnerabilities privately;
* provide reproducible reports;
* avoid placing production funds at risk;
* use regtest, signet, or controlled environments where possible;
* access only the information required to demonstrate the vulnerability;
* allow reasonable remediation time;
* coordinate disclosure when downstream users may be affected.

Testing must not intentionally steal, destroy, lock, or place third-party Bitcoin at risk.

# Security Is a Process

Rust provides powerful memory-safety and type-safety guarantees, but those guarantees do not ensure that a Lightning financial system behaves safely.

Security also depends on:

* correct economic logic;
* explicit financial units;
* appropriate authorization;
* idempotency;
* concurrency correctness;
* state consistency;
* safe persistence;
* conservative failure behavior;
* dependency security;
* operational discipline.

Node operators remain responsible for:

* understanding software they deploy;
* protecting Core Lightning secrets;
* maintaining backups;
* establishing appropriate capital limits;
* monitoring autonomous behavior;
* reviewing configuration;
* applying updates and security fixes.

If you identify behavior that could cause unauthorized fund movement, duplicate financial execution, policy bypass, secret disclosure, corruption of economically significant state, or compromise of the Core Lightning node, please report it privately.

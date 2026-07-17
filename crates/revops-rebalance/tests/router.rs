//! Router v3 golden parity, pinned by `fixtures/rebalance/router.json`
//! (generated from the REAL `modules/rebalance_router_v3.py` +
//! `modules/rebalance_engine_v2.py:_sweep_orphan_exclude_layers` by
//! `tools/port/gen_rebalance_fixtures.py router` in the port worktree,
//! branch `phase5-t4-gen`).
//!
//! The fixture pins three things at once:
//! - RouteResult outputs for every canned `getroutes` response shape
//!   (multi-route cheapest selection, "Nmsat" strings, missing probability,
//!   loop-through-us, error translation, policy fallbacks, repricing);
//! - the `getroutes` payload SHAPE (exactly six params — source,
//!   destination, amount_msat, layers, maxfee_msat, final_cltv — and NO
//!   `maxdelay`: askrene's default 2016 governs, the Tallship lesson);
//! - the throwaway exclude-layer lifecycle (create/update/remove ordering,
//!   cycle-cache reuse, 'Unknown layer' invalidation teardown).

use revops_rebalance::router::{
    configured_layer_names, sweep_orphan_exclude_layers, translate_getroutes_error, CycleRouter,
    GetRoutesRequest, PlannedPairCtx, RouterRpc, RpcFailure, EXCLUDE_LAYER_PREFIX,
    GETROUTES_RPC_TIMEOUT_SECONDS,
};
use serde_json::{json, Value};
use std::cell::{Cell, RefCell};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

fn fixture() -> Value {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/rebalance/router.json");
    let raw =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&raw).expect("valid JSON")
}

// ---------------------------------------------------------------------------
// Scripted RPC double (mirror of the generator's `_RouterRpcStub`)
// ---------------------------------------------------------------------------

#[derive(Default)]
struct CallLog {
    getroutes: Vec<Value>,
    creates: Vec<String>,
    updates: Vec<(String, String)>,
    removes: Vec<String>,
    listlayers_calls: usize,
}

#[derive(Default)]
struct ScriptedRpc {
    live_layers: Vec<String>,
    peers: Value,
    gossip: Vec<Value>,
    getroutes_script: Vec<Value>,
    getroutes_i: Cell<usize>,
    fail_listlayers: bool,
    fail_remove: HashSet<String>,
    log: RefCell<CallLog>,
    /// Cross-thread collector for the uniqueness test.
    name_sink: Option<Arc<Mutex<Vec<String>>>>,
}

impl ScriptedRpc {
    fn from_setup(setup: &Value) -> Self {
        let strs = |v: &Value| -> Vec<String> {
            v.as_array()
                .map(|a| a.iter().map(|s| s.as_str().unwrap().to_string()).collect())
                .unwrap_or_default()
        };
        ScriptedRpc {
            live_layers: strs(&setup["live_layers"]),
            peers: setup["peers"].clone(),
            gossip: setup["gossip_channels"]
                .as_array()
                .cloned()
                .unwrap_or_default(),
            getroutes_script: setup["getroutes"].as_array().cloned().unwrap_or_default(),
            ..Default::default()
        }
    }
}

impl RouterRpc for ScriptedRpc {
    fn getroutes(&self, req: &GetRoutesRequest) -> Result<Value, RpcFailure> {
        self.log
            .borrow_mut()
            .getroutes
            .push(serde_json::to_value(req).expect("serializable request"));
        let i = self.getroutes_i.get();
        assert!(
            !self.getroutes_script.is_empty(),
            "unscripted getroutes call"
        );
        let entry = &self.getroutes_script[i.min(self.getroutes_script.len() - 1)];
        self.getroutes_i.set(i + 1);
        if let Some(msg) = entry.get("error").and_then(Value::as_str) {
            return Err(RpcFailure {
                message: msg.to_string(),
            });
        }
        Ok(entry["result"].clone())
    }

    fn askrene_create_layer(&self, name: &str) -> Result<(), RpcFailure> {
        self.log.borrow_mut().creates.push(name.to_string());
        if let Some(sink) = &self.name_sink {
            sink.lock().unwrap().push(name.to_string());
        }
        Ok(())
    }

    fn askrene_update_channel(
        &self,
        layer: &str,
        scid_dir: &str,
        enabled: bool,
    ) -> Result<(), RpcFailure> {
        assert!(!enabled, "exclude layers only ever disable channels");
        self.log
            .borrow_mut()
            .updates
            .push((layer.to_string(), scid_dir.to_string()));
        Ok(())
    }

    fn askrene_remove_layer(&self, name: &str) -> Result<(), RpcFailure> {
        self.log.borrow_mut().removes.push(name.to_string());
        if self.fail_remove.contains(name) {
            return Err(RpcFailure {
                message: "Unknown layer".to_string(),
            });
        }
        Ok(())
    }

    fn askrene_listlayers(&self) -> Result<Value, RpcFailure> {
        self.log.borrow_mut().listlayers_calls += 1;
        if self.fail_listlayers {
            return Err(RpcFailure {
                message: "askrene not loaded".to_string(),
            });
        }
        let layers: Vec<Value> = self
            .live_layers
            .iter()
            .map(|n| json!({ "layer": n }))
            .collect();
        Ok(json!({ "layers": layers }))
    }

    fn listpeerchannels(&self, peer_id: &str) -> Result<Value, RpcFailure> {
        let channels = self
            .peers
            .get(peer_id)
            .cloned()
            .unwrap_or_else(|| json!([]));
        Ok(json!({ "channels": channels }))
    }

    fn listchannels_scid(&self, scid: &str) -> Result<Value, RpcFailure> {
        let out: Vec<Value> = self
            .gossip
            .iter()
            .filter(|c| c["short_channel_id"].as_str() == Some(scid))
            .cloned()
            .collect();
        Ok(json!({ "channels": out }))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// A minted throwaway layer must be `rebalance-exclude-{ts}-{n}`.
fn assert_exclude_layer_shape(name: &str) {
    let rest = name
        .strip_prefix(EXCLUDE_LAYER_PREFIX)
        .unwrap_or_else(|| panic!("layer {name:?} missing prefix"));
    let (ts, n) = rest
        .split_once('-')
        .unwrap_or_else(|| panic!("layer {name:?} not ts-n shaped"));
    ts.parse::<u64>()
        .unwrap_or_else(|_| panic!("layer {name:?}: bad timestamp"));
    n.parse::<u64>()
        .unwrap_or_else(|_| panic!("layer {name:?}: bad counter"));
}

/// Map real minted layer names -> `<exclude-N>` in creation order (the same
/// normalization the generator applies to the Python capture).
fn normalization(creates: &[String]) -> impl Fn(&str) -> String + '_ {
    move |name: &str| {
        for (i, c) in creates.iter().enumerate() {
            if c == name {
                return format!("<exclude-{}>", i + 1);
            }
        }
        name.to_string()
    }
}

fn ctx_for(setup: &Value, pair: &Value) -> PlannedPairCtx {
    PlannedPairCtx {
        our_node_id: setup["our_node_id"].as_str().unwrap().to_string(),
        source_channel_id: pair["source_channel_id"].as_str().unwrap().to_string(),
        dest_channel_id: pair["dest_channel_id"].as_str().unwrap().to_string(),
        source_peer_id: pair["source_peer_id"].as_str().unwrap().to_string(),
        dest_peer_id: pair["dest_peer_id"].as_str().unwrap().to_string(),
        amount_sats: pair["amount_sats"].as_i64().unwrap(),
        layer_names: setup["layer_names"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s.as_str().unwrap().to_string())
            .collect(),
        invoice_final_cltv: setup["invoice_final_cltv"].as_i64().unwrap(),
    }
}

// ---------------------------------------------------------------------------
// Fixture replay
// ---------------------------------------------------------------------------

#[test]
fn fixture_cases_replay_byte_identical() {
    let fx = fixture();
    for case in fx["cases"].as_array().expect("cases") {
        let name = case["name"].as_str().unwrap();
        let setup = &case["setup"];
        let rpc = ScriptedRpc::from_setup(setup);
        let mut router = CycleRouter::begin_cycle(&rpc);

        for (ci, call) in case["calls"].as_array().unwrap().iter().enumerate() {
            let ctx = ctx_for(setup, &call["pair"]);
            let exclude: Vec<String> = call["exclude"]
                .as_array()
                .unwrap()
                .iter()
                .map(|s| s.as_str().unwrap().to_string())
                .collect();
            let before = rpc.log.borrow().getroutes.len();
            let result = router.price_pair(&ctx, &exclude);

            let expected = &call["expected"];
            let tag = format!("{name}[{ci}]");
            assert_eq!(
                result.success,
                expected["success"].as_bool().unwrap(),
                "{tag} success"
            );
            let expected_error = expected["error"].as_str().unwrap();
            assert_eq!(
                result.error.as_deref().unwrap_or(""),
                expected_error,
                "{tag} error"
            );
            assert_eq!(
                result.route_cost_sats,
                expected["route_cost_sats"].as_i64().unwrap(),
                "{tag} route_cost_sats"
            );
            assert_eq!(
                result.final_hop_fee_ppm,
                expected["final_hop_fee_ppm"].as_i64().unwrap(),
                "{tag} final_hop_fee_ppm"
            );
            assert_eq!(
                result.hops,
                expected["hops"].as_i64().unwrap(),
                "{tag} hops"
            );
            assert_eq!(
                result.probability_ppm,
                expected["probability_ppm"].as_i64().unwrap(),
                "{tag} probability_ppm"
            );
            let got_route: Vec<Value> = result
                .route
                .iter()
                .map(|h| {
                    json!({
                        "id": h.id,
                        "channel": h.channel,
                        "direction": h.direction,
                        "amount_msat": h.amount_msat,
                        "delay": h.delay,
                        "style": h.style,
                    })
                })
                .collect();
            assert_eq!(
                Value::Array(got_route),
                expected["route"],
                "{tag} sendpay route hops"
            );

            // getroutes payload shape (params + exactly-six-keys pin).
            let expected_params = &call["expected_getroutes_params"];
            let log = rpc.log.borrow();
            let new_calls = &log.getroutes[before..];
            if expected_params.is_null() {
                assert!(new_calls.is_empty(), "{tag}: unexpected getroutes call");
            } else {
                assert_eq!(new_calls.len(), 1, "{tag}: exactly one getroutes call");
                let fixture_keys: HashSet<&str> = expected_params
                    .as_object()
                    .unwrap()
                    .keys()
                    .map(String::as_str)
                    .collect();
                let contract_keys: HashSet<&str> = [
                    "source",
                    "destination",
                    "amount_msat",
                    "layers",
                    "maxfee_msat",
                    "final_cltv",
                ]
                .into_iter()
                .collect();
                assert_eq!(
                    fixture_keys, contract_keys,
                    "{tag}: getroutes param keys (NO maxdelay — askrene default 2016 governs)"
                );
                let mut got = new_calls[0].clone();
                let creates = log.creates.clone();
                let norm = normalization(&creates);
                let layers: Vec<Value> = got["layers"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|l| Value::String(norm(l.as_str().unwrap())))
                    .collect();
                got["layers"] = Value::Array(layers);
                assert_eq!(&got, expected_params, "{tag} getroutes params");
            }
        }

        router.end_cycle();

        // Layer lifecycle (normalized names, creation order).
        let log = rpc.log.borrow();
        for c in &log.creates {
            assert_exclude_layer_shape(c);
        }
        let creates = log.creates.clone();
        let norm = normalization(&creates);
        let lc = &case["expected_layer_lifecycle"];
        let got_creates: Vec<String> = log.creates.iter().map(|n| norm(n)).collect();
        let exp_creates: Vec<String> = lc["creates"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s.as_str().unwrap().to_string())
            .collect();
        assert_eq!(got_creates, exp_creates, "{name} layer creates");
        let got_removes: Vec<String> = log.removes.iter().map(|n| norm(n)).collect();
        let exp_removes: Vec<String> = lc["removes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s.as_str().unwrap().to_string())
            .collect();
        assert_eq!(got_removes, exp_removes, "{name} layer removes");
        for (layer_norm, exp_updates) in lc["updates"].as_object().unwrap() {
            let got_updates: Vec<String> = log
                .updates
                .iter()
                .filter(|(l, _)| &norm(l) == layer_norm)
                .map(|(_, sd)| sd.clone())
                .collect();
            let exp: Vec<String> = exp_updates
                .as_array()
                .unwrap()
                .iter()
                .map(|s| s.as_str().unwrap().to_string())
                .collect();
            assert_eq!(got_updates, exp, "{name} updates for {layer_norm}");
        }
        assert_eq!(
            log.updates.len(),
            lc["updates"]
                .as_object()
                .unwrap()
                .values()
                .map(|v| v.as_array().unwrap().len())
                .sum::<usize>(),
            "{name} total update count"
        );
        assert_eq!(
            log.listlayers_calls,
            lc["listlayers_calls"].as_u64().unwrap() as usize,
            "{name} listlayers probe count (cycle cache)"
        );
    }
}

// ---------------------------------------------------------------------------
// Error translation + config normalization tables
// ---------------------------------------------------------------------------

#[test]
fn error_translation_table_matches_python() {
    let fx = fixture();
    for row in fx["error_translation"].as_array().expect("table") {
        let input = row["input"].as_str().unwrap();
        let reason = row["reason"].as_str().unwrap();
        // Python preserves the original text as detail, always.
        assert_eq!(row["detail"].as_str().unwrap(), input, "detail preserved");
        assert_eq!(translate_getroutes_error(input), reason, "input={input:?}");
    }
}

#[test]
fn configured_layer_names_table_matches_python() {
    let fx = fixture();
    for row in fx["configured_layer_names"].as_array().expect("table") {
        let raw = row["raw"].as_str();
        let expected: Vec<String> = row["expected"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s.as_str().unwrap().to_string())
            .collect();
        assert_eq!(
            configured_layer_names(raw),
            expected,
            "raw={:?}",
            row["raw"]
        );
    }
}

#[test]
fn getroutes_timeout_contract_is_45s() {
    // 45s per-call ceiling vs the 15s global RPC ceiling: askrene MCF on
    // large amounts runs past 15s deterministically (measured on lnnode).
    let fx = fixture();
    assert_eq!(
        fx["getroutes_rpc_timeout_seconds"].as_u64().unwrap(),
        GETROUTES_RPC_TIMEOUT_SECONDS
    );
    assert_eq!(GETROUTES_RPC_TIMEOUT_SECONDS, 45);
}

// ---------------------------------------------------------------------------
// Orphan sweep
// ---------------------------------------------------------------------------

#[test]
fn orphan_sweep_fixture_cases() {
    let fx = fixture();
    for case in fx["orphan_sweep"].as_array().expect("orphan_sweep") {
        let name = case["name"].as_str().unwrap();
        let rpc = ScriptedRpc {
            live_layers: case["live_layers"]
                .as_array()
                .unwrap()
                .iter()
                .map(|s| s.as_str().unwrap().to_string())
                .collect(),
            fail_listlayers: case["fail_list"].as_bool().unwrap(),
            fail_remove: case["fail_remove"]
                .as_array()
                .unwrap()
                .iter()
                .map(|s| s.as_str().unwrap().to_string())
                .collect(),
            ..Default::default()
        };
        let removed = sweep_orphan_exclude_layers(&rpc);
        assert_eq!(
            removed,
            case["expected_removed_count"].as_u64().unwrap() as usize,
            "{name} removed count"
        );
        let attempts: Vec<String> = case["expected_remove_attempts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s.as_str().unwrap().to_string())
            .collect();
        assert_eq!(rpc.log.borrow().removes, attempts, "{name} remove attempts");
    }
}

#[test]
fn orphan_sweep_removes_only_rebalance_exclude_prefix() {
    let rpc = ScriptedRpc {
        live_layers: vec![
            "rebalance-exclude-1700000000-1".to_string(),
            "xpay".to_string(),
            "auto.no_mpp_support".to_string(),
            "not-rebalance-exclude-1-1".to_string(),
            "rebalance-exclude-1700000099-7".to_string(),
        ],
        ..Default::default()
    };
    assert_eq!(sweep_orphan_exclude_layers(&rpc), 2);
    assert_eq!(
        rpc.log.borrow().removes,
        vec![
            "rebalance-exclude-1700000000-1".to_string(),
            "rebalance-exclude-1700000099-7".to_string(),
        ]
    );
}

// ---------------------------------------------------------------------------
// Concurrency: exclude-layer names must never collide
// ---------------------------------------------------------------------------

#[test]
fn exclude_layer_names_unique_across_threads() {
    // Python uses itertools.count (atomic under the GIL) precisely because
    // two execution workers minting the same name leads to one removing the
    // other's LIVE layer. The Rust port must hold the same guarantee via a
    // process-global AtomicU64.
    let setup = json!({
        "our_node_id": "02".to_string() + &"aa".repeat(32),
        "layer_names": [],
        "invoice_final_cltv": 18,
        "live_layers": [],
        "peers": {
            "02".to_string() + &"44".repeat(32): [{
                "peer_id": "02".to_string() + &"44".repeat(32),
                "short_channel_id": "800001x2000x1",
                "updates": {"remote": {
                    "fee_proportional_millionths": 250,
                    "fee_base_msat": 0,
                    "cltv_expiry_delta": 34,
                }},
            }],
        },
        "gossip_channels": [],
        "getroutes": [{"error": "no dice"}],
    });
    let pair = json!({
        "source_channel_id": "800000x1000x1",
        "dest_channel_id": "800001x2000x1",
        "source_peer_id": "02".to_string() + &"cc".repeat(32),
        "dest_peer_id": "02".to_string() + &"44".repeat(32),
        "amount_sats": 100_000,
    });
    let sink: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

    std::thread::scope(|scope| {
        for t in 0..8 {
            let setup = setup.clone();
            let pair = pair.clone();
            let sink = Arc::clone(&sink);
            scope.spawn(move || {
                let mut rpc = ScriptedRpc::from_setup(&setup);
                rpc.name_sink = Some(sink);
                let mut router = CycleRouter::begin_cycle(&rpc);
                let ctx = ctx_for(&setup, &pair);
                for i in 0..5 {
                    // Distinct exclude sets so the cycle cache can never
                    // coalesce them into one layer.
                    let exclude = vec![format!("60000{t}x{i}x0/1")];
                    let result = router.price_pair(&ctx, &exclude);
                    assert!(!result.success);
                }
                router.end_cycle();
            });
        }
    });

    let names = sink.lock().unwrap();
    assert_eq!(names.len(), 40, "8 threads x 5 distinct exclude sets");
    for n in names.iter() {
        assert_exclude_layer_shape(n);
    }
    let unique: HashSet<&String> = names.iter().collect();
    assert_eq!(unique.len(), 40, "duplicate exclude layer name minted");
}

// ---------------------------------------------------------------------------
// Teardown safety nets not visible in the Python capture
// ---------------------------------------------------------------------------

#[test]
fn drop_without_end_cycle_still_tears_down_layers() {
    let setup = json!({
        "our_node_id": "02".to_string() + &"aa".repeat(32),
        "layer_names": [],
        "invoice_final_cltv": 18,
        "live_layers": [],
        "peers": {
            "02".to_string() + &"44".repeat(32): [{
                "peer_id": "02".to_string() + &"44".repeat(32),
                "short_channel_id": "800001x2000x1",
                "updates": {"remote": {
                    "fee_proportional_millionths": 0,
                    "fee_base_msat": 0,
                    "cltv_expiry_delta": 34,
                }},
            }],
        },
        "gossip_channels": [],
        "getroutes": [{"error": "no dice"}],
    });
    let pair = json!({
        "source_channel_id": "800000x1000x1",
        "dest_channel_id": "800001x2000x1",
        "source_peer_id": "02".to_string() + &"cc".repeat(32),
        "dest_peer_id": "02".to_string() + &"44".repeat(32),
        "amount_sats": 1_000,
    });
    let rpc = ScriptedRpc::from_setup(&setup);
    {
        let mut router = CycleRouter::begin_cycle(&rpc);
        let ctx = ctx_for(&setup, &pair);
        let _ = router.price_pair(&ctx, &[]);
        // dropped without end_cycle (panic-unwind path)
    }
    let log = rpc.log.borrow();
    assert_eq!(log.creates.len(), 1);
    assert_eq!(log.removes, log.creates, "Drop must remove the live layer");
}

#[test]
fn partial_exclude_layer_build_rolls_back() {
    // A failed askrene-update-channel must remove the half-built layer
    // before the error surfaces (Python: _build_exclude_layer's
    // try/except -> _remove_exclude_layer -> raise).
    struct FailingUpdateRpc {
        inner: ScriptedRpc,
    }
    impl RouterRpc for FailingUpdateRpc {
        fn getroutes(&self, req: &GetRoutesRequest) -> Result<Value, RpcFailure> {
            self.inner.getroutes(req)
        }
        fn askrene_create_layer(&self, name: &str) -> Result<(), RpcFailure> {
            self.inner.askrene_create_layer(name)
        }
        fn askrene_update_channel(
            &self,
            layer: &str,
            scid_dir: &str,
            enabled: bool,
        ) -> Result<(), RpcFailure> {
            let _ = self.inner.askrene_update_channel(layer, scid_dir, enabled);
            Err(RpcFailure {
                message: "askrene-update-channel: injected failure".to_string(),
            })
        }
        fn askrene_remove_layer(&self, name: &str) -> Result<(), RpcFailure> {
            self.inner.askrene_remove_layer(name)
        }
        fn askrene_listlayers(&self) -> Result<Value, RpcFailure> {
            self.inner.askrene_listlayers()
        }
        fn listpeerchannels(&self, peer_id: &str) -> Result<Value, RpcFailure> {
            self.inner.listpeerchannels(peer_id)
        }
        fn listchannels_scid(&self, scid: &str) -> Result<Value, RpcFailure> {
            self.inner.listchannels_scid(scid)
        }
    }

    let setup = json!({
        "our_node_id": "02".to_string() + &"aa".repeat(32),
        "layer_names": [],
        "invoice_final_cltv": 18,
        "live_layers": [],
        "peers": {
            "02".to_string() + &"44".repeat(32): [{
                "peer_id": "02".to_string() + &"44".repeat(32),
                "short_channel_id": "800001x2000x1",
                "updates": {"remote": {
                    "fee_proportional_millionths": 0,
                    "fee_base_msat": 0,
                    "cltv_expiry_delta": 34,
                }},
            }],
        },
        "gossip_channels": [],
        "getroutes": [{"error": "unreached"}],
    });
    let pair = json!({
        "source_channel_id": "800000x1000x1",
        "dest_channel_id": "800001x2000x1",
        "source_peer_id": "02".to_string() + &"cc".repeat(32),
        "dest_peer_id": "02".to_string() + &"44".repeat(32),
        "amount_sats": 1_000,
    });
    let rpc = FailingUpdateRpc {
        inner: ScriptedRpc::from_setup(&setup),
    };
    let mut router = CycleRouter::begin_cycle(&rpc);
    let ctx = ctx_for(&setup, &pair);
    let result = router.price_pair(&ctx, &[]);
    assert!(!result.success);
    assert_eq!(
        result.error.as_deref(),
        Some("askrene-update-channel: injected failure")
    );
    router.end_cycle();
    let log = rpc.inner.log.borrow();
    assert_eq!(log.creates.len(), 1, "layer was created");
    assert_eq!(
        log.removes, log.creates,
        "half-built layer rolled back exactly once (no end_cycle double-remove)"
    );
    assert!(
        log.getroutes.is_empty(),
        "getroutes must not run after a failed exclude-layer build"
    );
}

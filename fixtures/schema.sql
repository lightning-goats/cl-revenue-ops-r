CREATE TABLE schema_version (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                version INTEGER NOT NULL DEFAULT 1,
                updated_at INTEGER
            );
CREATE TABLE channel_states (
                channel_id TEXT PRIMARY KEY,
                peer_id TEXT NOT NULL,
                state TEXT NOT NULL,  -- 'source', 'sink', 'balanced'
                flow_ratio REAL NOT NULL,
                sats_in INTEGER NOT NULL,
                sats_out INTEGER NOT NULL,
                capacity INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            , confidence REAL DEFAULT 1.0, velocity REAL DEFAULT 0.0, flow_multiplier REAL DEFAULT 1.0, ema_decay REAL DEFAULT 0.8, forward_count INTEGER DEFAULT 0, kalman_flow_ratio REAL DEFAULT 0.0, kalman_velocity REAL DEFAULT 0.0, kalman_uncertainty REAL DEFAULT 0.1, temporal_profile_json TEXT DEFAULT NULL);
CREATE TABLE fee_strategy_state (
                channel_id TEXT PRIMARY KEY,
                last_revenue_rate REAL NOT NULL DEFAULT 0.0,
                last_fee_ppm INTEGER NOT NULL DEFAULT 0,
                trend_direction INTEGER NOT NULL DEFAULT 1,  -- 1 = increase, -1 = decrease
                step_ppm INTEGER NOT NULL DEFAULT 50,  -- Current step size (for dampening)
                consecutive_same_direction INTEGER NOT NULL DEFAULT 0,
                last_update INTEGER NOT NULL DEFAULT 0,
                last_broadcast_fee_ppm INTEGER NOT NULL DEFAULT 0,
                is_sleeping INTEGER NOT NULL DEFAULT 0,
                sleep_until INTEGER NOT NULL DEFAULT 0,
                stable_cycles INTEGER NOT NULL DEFAULT 0
            , last_state TEXT DEFAULT 'balanced', forward_count_since_update INTEGER DEFAULT 0, last_volume_sats INTEGER DEFAULT 0, v2_state_json TEXT DEFAULT '{}');
CREATE TABLE fee_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id TEXT NOT NULL,
                peer_id TEXT NOT NULL,
                old_fee_ppm INTEGER NOT NULL,
                new_fee_ppm INTEGER NOT NULL,
                reason TEXT,
                manual INTEGER NOT NULL DEFAULT 0,
                timestamp INTEGER NOT NULL
            , reason_code TEXT, heuristic_modifiers TEXT);
CREATE TABLE sqlite_sequence(name,seq);
CREATE TABLE rebalance_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                from_channel TEXT NOT NULL,
                to_channel TEXT NOT NULL,
                amount_sats INTEGER NOT NULL,
                max_fee_sats INTEGER NOT NULL,
                actual_fee_sats INTEGER,
                actual_fee_msat INTEGER,
                expected_profit_sats INTEGER NOT NULL,
                actual_profit_sats INTEGER,
                status TEXT NOT NULL,  -- 'pending', 'success', 'failed'
                rebalance_type TEXT NOT NULL DEFAULT 'normal',  -- 'normal', 'diagnostic'
                error_message TEXT,
                timestamp INTEGER NOT NULL,
                payment_hash TEXT
            , reason_code TEXT, bleeder_status TEXT, post_local_ratio REAL);
CREATE TABLE forwards (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                in_channel TEXT NOT NULL,
                out_channel TEXT NOT NULL,
                in_msat INTEGER NOT NULL,
                out_msat INTEGER NOT NULL,
                fee_msat INTEGER NOT NULL,
                resolution_time REAL DEFAULT 0,
                timestamp INTEGER NOT NULL,
                resolved_time INTEGER DEFAULT 0
            );
CREATE UNIQUE INDEX idx_forwards_unique
                    ON forwards(in_channel, out_channel, in_msat, out_msat, fee_msat, timestamp, resolved_time)
                ;
CREATE INDEX idx_forwards_in_time ON forwards(in_channel, timestamp);
CREATE TABLE channel_costs (
                channel_id TEXT PRIMARY KEY,
                peer_id TEXT NOT NULL,
                open_cost_sats INTEGER NOT NULL DEFAULT 0,
                capacity_sats INTEGER NOT NULL,
                opened_at INTEGER NOT NULL
            );
CREATE TABLE rebalance_costs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id TEXT NOT NULL,
                peer_id TEXT NOT NULL,
                cost_sats INTEGER NOT NULL,
                cost_msat INTEGER,
                amount_sats INTEGER NOT NULL,
                timestamp INTEGER NOT NULL
            );
CREATE TABLE channel_failures (
                channel_id TEXT PRIMARY KEY,
                failure_count INTEGER NOT NULL DEFAULT 0,
                last_failure_time INTEGER NOT NULL DEFAULT 0,
                last_attempted_ppm INTEGER NOT NULL DEFAULT 0,
                last_attempted_amount INTEGER NOT NULL DEFAULT 0,
                last_error_type TEXT NOT NULL DEFAULT ''
            );
CREATE TABLE pair_rebalance_failures (
                source_channel_id TEXT NOT NULL,
                dest_channel_id TEXT NOT NULL,
                failure_kind TEXT NOT NULL DEFAULT '',
                failure_count INTEGER NOT NULL DEFAULT 0,
                last_failure_at INTEGER NOT NULL DEFAULT 0,
                cooldown_until INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (source_channel_id, dest_channel_id)
            );
CREATE TABLE peer_reputation (
                peer_id TEXT PRIMARY KEY,
                success_count INTEGER NOT NULL DEFAULT 0,
                failure_count INTEGER NOT NULL DEFAULT 0,
                last_update INTEGER NOT NULL DEFAULT 0
            );
CREATE TABLE peer_connection_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                peer_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                timestamp INTEGER NOT NULL
            );
CREATE TABLE lifetime_aggregates (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                pruned_revenue_msat INTEGER NOT NULL DEFAULT 0,
                pruned_forward_count INTEGER NOT NULL DEFAULT 0,
                last_prune_timestamp INTEGER NOT NULL DEFAULT 0
            );
CREATE TABLE channel_probes (
                channel_id TEXT PRIMARY KEY,
                probe_type TEXT NOT NULL,  -- legacy 'zero_fee' or current 'bounded_low_fee'
                started_at INTEGER NOT NULL
            );
CREATE TABLE ignored_peers (
                peer_id TEXT PRIMARY KEY,
                reason TEXT,
                ignored_at INTEGER NOT NULL
            );
CREATE TABLE peer_policies (
                peer_id TEXT PRIMARY KEY,
                strategy TEXT NOT NULL DEFAULT 'dynamic',
                rebalance_mode TEXT NOT NULL DEFAULT 'enabled',
                fee_ppm_target INTEGER,
                tags TEXT,
                updated_at INTEGER NOT NULL
            , fee_multiplier_min REAL, fee_multiplier_max REAL, expires_at INTEGER);
CREATE TABLE hot_channel_protection_overrides (
                peer_id TEXT PRIMARY KEY,
                added_at INTEGER NOT NULL,
                note TEXT,
                min_depletion_trigger_pct REAL
            );
CREATE INDEX idx_hot_channel_protection_overrides_added_at ON hot_channel_protection_overrides(added_at);
CREATE TABLE config_overrides (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                version INTEGER NOT NULL DEFAULT 1,
                updated_at INTEGER NOT NULL
            );
CREATE TABLE mempool_fee_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sat_per_vbyte REAL NOT NULL,
                timestamp INTEGER NOT NULL
            );
CREATE INDEX idx_fee_changes_channel ON fee_changes(channel_id, timestamp);
CREATE INDEX idx_fee_changes_time ON fee_changes(timestamp);
CREATE INDEX idx_forwards_time ON forwards(timestamp);
CREATE INDEX idx_rebalance_costs_channel ON rebalance_costs(channel_id);
CREATE INDEX idx_rebalance_costs_channel_time ON rebalance_costs(channel_id, timestamp);
CREATE INDEX idx_rebalance_costs_time ON rebalance_costs(timestamp, cost_sats);
CREATE INDEX idx_rebalance_costs_time_channel ON rebalance_costs(timestamp, channel_id, cost_sats);
CREATE INDEX idx_rh_pending_settlement ON rebalance_history(timestamp) WHERE status='pending_settlement';
CREATE INDEX idx_rh_success_to_channel ON rebalance_history(to_channel, timestamp) WHERE status='success';
CREATE INDEX idx_channel_states_peer ON channel_states(peer_id);
CREATE INDEX idx_connection_history_peer_time ON peer_connection_history(peer_id, timestamp);
CREATE INDEX idx_mempool_time ON mempool_fee_history(timestamp);
CREATE INDEX idx_rebalance_history_time ON rebalance_history(timestamp);
CREATE INDEX idx_rebalance_history_to_channel ON rebalance_history(to_channel, timestamp);
CREATE INDEX idx_pair_rebalance_failures_cooldown ON pair_rebalance_failures(cooldown_until);
CREATE INDEX idx_forwards_out_channel_time ON forwards(out_channel, timestamp);
CREATE TABLE daily_forwarding_stats (
                channel_id TEXT NOT NULL,
                date INTEGER NOT NULL,  -- Unix timestamp of midnight (UTC)
                total_in_msat INTEGER NOT NULL DEFAULT 0,
                total_out_msat INTEGER NOT NULL DEFAULT 0,
                total_fee_msat INTEGER NOT NULL DEFAULT 0,
                forward_count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (channel_id, date)
            );
CREATE INDEX idx_daily_fwd_stats_date ON daily_forwarding_stats(date);
CREATE TABLE daily_forwarding_stats_inbound (
                channel_id TEXT NOT NULL,
                date INTEGER NOT NULL,  -- Unix timestamp of midnight (UTC)
                total_in_msat INTEGER NOT NULL DEFAULT 0,
                total_fee_msat INTEGER NOT NULL DEFAULT 0,
                forward_count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (channel_id, date)
            );
CREATE INDEX idx_daily_fwd_stats_inbound_date ON daily_forwarding_stats_inbound(date);
CREATE TABLE budget_reservations (
                reservation_id TEXT PRIMARY KEY,
                reserved_sats INTEGER NOT NULL,
                reserved_at INTEGER NOT NULL,
                job_channel_id TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'active'  -- 'active', 'spent', 'released'
            );
CREATE INDEX idx_budget_reservations_status ON budget_reservations(status, reserved_at);
CREATE TABLE spend_reservations (
                reservation_id TEXT PRIMARY KEY,
                category TEXT NOT NULL,
                subcategory TEXT,
                reserved_sats INTEGER NOT NULL,
                reserved_at INTEGER NOT NULL,
                reference_id TEXT,
                channel_id TEXT,
                status TEXT NOT NULL DEFAULT 'active',  -- 'active', 'spent', 'released'
                metadata_json TEXT
            );
CREATE INDEX idx_spend_reservations_status ON spend_reservations(status, reserved_at);
CREATE INDEX idx_spend_reservations_category ON spend_reservations(category, reserved_at);
CREATE TABLE spend_events (
                event_id TEXT PRIMARY KEY,
                category TEXT NOT NULL,
                subcategory TEXT,
                amount_sats INTEGER NOT NULL,
                timestamp INTEGER NOT NULL,
                reference_id TEXT,
                channel_id TEXT,
                source TEXT,
                metadata_json TEXT
            );
CREATE INDEX idx_spend_events_time ON spend_events(timestamp);
CREATE INDEX idx_spend_events_category ON spend_events(category, timestamp);
CREATE INDEX idx_spend_events_time_channel ON spend_events(timestamp, channel_id, amount_sats);
CREATE TABLE financial_snapshots (
                timestamp INTEGER PRIMARY KEY,
                total_local_balance_sats INTEGER NOT NULL,
                total_remote_balance_sats INTEGER NOT NULL,
                total_onchain_sats INTEGER NOT NULL,
                total_capacity_sats INTEGER NOT NULL,
                total_revenue_accumulated_sats INTEGER NOT NULL,
                total_rebalance_cost_accumulated_sats INTEGER NOT NULL,
                channel_count INTEGER NOT NULL
            );
CREATE INDEX idx_financial_snapshots_time ON financial_snapshots(timestamp);
CREATE TABLE channel_closure_costs (
                channel_id TEXT PRIMARY KEY,
                peer_id TEXT NOT NULL,
                close_type TEXT NOT NULL,  -- 'mutual', 'local_unilateral', 'remote_unilateral', 'unknown'
                closure_fee_sats INTEGER NOT NULL DEFAULT 0,
                htlc_sweep_fee_sats INTEGER NOT NULL DEFAULT 0,
                penalty_fee_sats INTEGER NOT NULL DEFAULT 0,
                total_closure_cost_sats INTEGER NOT NULL DEFAULT 0,
                funding_txid TEXT,
                closing_txid TEXT,
                closed_at INTEGER NOT NULL,
                resolution_complete INTEGER NOT NULL DEFAULT 0  -- 1 when all outputs resolved
            , bkpr_account TEXT);
CREATE INDEX idx_closure_costs_peer ON channel_closure_costs(peer_id);
CREATE INDEX idx_closure_costs_time ON channel_closure_costs(closed_at);
CREATE TABLE closed_channels (
                channel_id TEXT PRIMARY KEY,
                peer_id TEXT NOT NULL,
                capacity_sats INTEGER NOT NULL,
                opened_at INTEGER,
                closed_at INTEGER NOT NULL,
                close_type TEXT NOT NULL,
                open_cost_sats INTEGER NOT NULL DEFAULT 0,
                closure_cost_sats INTEGER NOT NULL DEFAULT 0,
                total_revenue_sats INTEGER NOT NULL DEFAULT 0,
                total_rebalance_cost_sats INTEGER NOT NULL DEFAULT 0,
                forward_count INTEGER NOT NULL DEFAULT 0,
                net_pnl_sats INTEGER NOT NULL DEFAULT 0,
                days_open INTEGER NOT NULL DEFAULT 0,
                funding_txid TEXT,
                closing_txid TEXT
            , closer TEXT DEFAULT 'unknown');
CREATE INDEX idx_closed_channels_peer ON closed_channels(peer_id);
CREATE INDEX idx_closed_channels_time ON closed_channels(closed_at);
CREATE TABLE kalman_state (
                    channel_id TEXT PRIMARY KEY,
                    flow_ratio REAL DEFAULT 0.0,
                    flow_velocity REAL DEFAULT 0.0,
                    variance_ratio REAL DEFAULT 0.1,
                    variance_velocity REAL DEFAULT 0.1,
                    covariance REAL DEFAULT 0.0,
                    last_update INTEGER DEFAULT 0,
                    innovation_variance REAL DEFAULT 0.01,
                    last_innovation REAL DEFAULT 0.0
                , velocity_unit TEXT DEFAULT 'per_day', observation_count INTEGER DEFAULT 0);
CREATE TABLE planner_candidates (
                peer_id TEXT PRIMARY KEY,
                score REAL NOT NULL DEFAULT 0.0,
                source TEXT NOT NULL,
                last_evaluated INTEGER NOT NULL,
                capacity_recommendation_sats INTEGER,
                connect_successes INTEGER DEFAULT 0,
                connect_failures INTEGER DEFAULT 0,
                metadata_json TEXT
            );
CREATE INDEX idx_planner_candidates_score ON planner_candidates(score);
CREATE TABLE planner_actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                action_type TEXT NOT NULL,
                peer_id TEXT NOT NULL,
                channel_id TEXT,
                amount_sats INTEGER,
                estimated_cost_sats INTEGER,
                actual_cost_sats INTEGER,
                status TEXT NOT NULL DEFAULT 'planned',
                created_at INTEGER NOT NULL,
                completed_at INTEGER,
                reason TEXT,
                metadata_json TEXT
            );
CREATE INDEX idx_planner_actions_status ON planner_actions(status);
CREATE INDEX idx_planner_actions_peer_time ON planner_actions(peer_id, created_at);
CREATE TABLE lnplus_swaps (
                swap_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                capacity_sats INTEGER NOT NULL,
                duration_months INTEGER NOT NULL,
                ends_at INTEGER,
                outbound_peer TEXT,
                incoming_peer TEXT,
                our_identifier TEXT,
                applied_at INTEGER NOT NULL,
                opened_at INTEGER,
                completed_at INTEGER,
                channel_funding_txid TEXT,
                deadline_at INTEGER,
                planner_action_id INTEGER,
                outcome TEXT,
                metadata_json TEXT,
                tag_added INTEGER,
                incoming_tag_added INTEGER
            );
CREATE INDEX idx_lnplus_swaps_status ON lnplus_swaps(status);
CREATE TABLE lnplus_peers (
                pubkey TEXT PRIMARY KEY,
                swaps_count INTEGER NOT NULL DEFAULT 0,
                ratings_given_positive INTEGER NOT NULL DEFAULT 0,
                ratings_given_negative INTEGER NOT NULL DEFAULT 0,
                defections INTEGER NOT NULL DEFAULT 0,
                last_swap_at INTEGER
            );
CREATE TABLE dead_capital_stage (
                channel_id TEXT PRIMARY KEY,
                stage TEXT NOT NULL DEFAULT 'fee_reduction',
                entered_at INTEGER NOT NULL
            );
CREATE TABLE planner_recycle_ops (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                close_scid TEXT NOT NULL,
                close_peer_id TEXT NOT NULL,
                open_peer_id TEXT NOT NULL,
                open_amount_sats INTEGER NOT NULL,
                recycle_ev_sats INTEGER NOT NULL,
                funding_source TEXT NOT NULL DEFAULT 'close',
                status TEXT NOT NULL DEFAULT 'pending_close',
                cycles_waited INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL,
                completed_at INTEGER,
                close_action_id INTEGER,
                open_action_id INTEGER
            );

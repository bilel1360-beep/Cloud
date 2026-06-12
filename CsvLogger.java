package org.fog.test.perfeval.datasetgen;

import java.io.*;
import java.nio.file.*;
import java.util.*;

/**
 * CsvLogger — persistent writers, one per output file for the whole run.
 * Call close() after all runs finish, before any post-processing reads files.
 */
public class CsvLogger {

    private static final Map<String, PrintWriter> writers = new LinkedHashMap<>();

    public static void init() throws IOException {
        close(); // defensive: close any leftover writers
        Files.createDirectories(Paths.get(SimConfig.OUTPUT_DIR));

        // ── Scheduler Decision (26 cols) ──────────────────────
        // Added vs original: ram_required, sla_class, priority_class,
        // max_allowed_latency — all exist in WorkloadGenerator.Task
        // but were never logged. Required by client schema.
        openAndWriteHeader(SimConfig.SCHED_DECISION_CSV,
            "decision_id,simulation_run,random_seed," +
            "simulation_time,decision_time," +
            "app_id,tuple_id,actual_tuple_id,tuple_type," +
            "source_module,destination_module,source_device_id," +
            "tuple_direction,tuple_cpu_length,tuple_network_length," +
            "tuple_output_size,num_pes,ram_required,sla_class,priority_class," +
            "max_allowed_latency,deadline,deadline_slack," +
            "num_candidates,selected_candidate_device_id,scheduling_policy");

        // ── Scheduler Candidate (47 cols) ─────────────────────
        openAndWriteHeader(SimConfig.SCHED_CANDIDATE_CSV,
            "decision_id," +
            "candidate_device_id,device_name,device_type,device_level," +
            "parent_id,cluster_id," +
            "total_mips,num_pes,ram_capacity,storage_capacity," +
            "uplink_bandwidth,downlink_bandwidth,uplink_latency," +
            "power_model_idle,power_model_max," +
            "hosts_required_service,service_instance_count," +
            "available_mips,allocated_mips,cpu_utilization," +
            "available_ram,available_storage," +
            "queue_size,queue_pressure," +
            "uplink_busy,downlink_busy," +
            "active_module_count,active_tuple_count," +
            "cumulative_energy,current_power,energy_slope," +
            "path_latency_from_source,hop_count_from_source," +
            "candidate_energy_delta,candidate_system_energy_delta," +
            "candidate_task_attributed_energy,candidate_completion_time," +
            "candidate_cpu_execution_time,candidate_loop_latency," +
            "candidate_network_usage_delta," +
            "deadline_violation,qos_met,final_cost,rank,is_best,is_selected");

        // ── Scheduler Outcome (15 cols) ───────────────────────
        // CHANGED: now logs one row per CANDIDATE (2.7M rows total),
        // not one row per decision (100K rows). This is the replay
        // methodology — every task was executed on every device, so
        // every (task, device) pair gets its own outcome row.
        // is_selected=1 identifies which device was actually chosen.
        openAndWriteHeader(SimConfig.SCHED_OUTCOME_CSV,
            "decision_id,candidate_device_id," +
            "start_time,finish_time,completion_time," +
            "cpu_execution_time,network_usage_delta," +
            "device_energy_before,device_energy_after,device_energy_delta," +
            "task_attributed_energy,deadline_violation,qos_met,success,is_selected");

        // ── Migration Decision (27 cols) ──────────────────────
        // CHANGED: removed module_or_service_name — it was a duplicate
        // of module_name with identical values on every row.
        openAndWriteHeader(SimConfig.MIG_DECISION_CSV,
            "migration_decision_id,episode_id,decision_index," +
            "simulation_run,random_seed,simulation_time,trigger_reason," +
            "module_name,app_id," +
            "source_device_id,current_placement_state," +
            "module_size,module_mips,module_ram,module_bw," +
            "in_flight_tuple_count,average_remaining_work," +
            "current_latency,current_qos_state,current_energy_slope," +
            "source_cpu_utilization,source_queue_pressure," +
            "source_cumulative_energy,num_candidates," +
            "migration_recommended,selected_action,migration_policy");

        // ── Migration Candidate (38 cols) ─────────────────────
        // FIX 3: action_id is now a unique sequential integer (not a duplicate
        // of action_type). action_type still holds the action string.
        openAndWriteHeader(SimConfig.MIG_CANDIDATE_CSV,
            "migration_decision_id,action_id,action_type,is_no_migration," +
            "source_device_id,destination_device_id,destination_device_type," +
            "destination_total_mips,destination_available_mips," +
            "destination_cpu_utilization,destination_available_ram," +
            "destination_queue_pressure,destination_cumulative_energy," +
            "estimated_path_latency,estimated_migration_delay," +
            "estimated_migration_network_usage,estimated_migration_energy," +
            "estimated_post_migration_load," +
            "observation_window_start,observation_window_end," +
            "energy_before,energy_after,energy_delta," +
            "latency_before,latency_after,latency_delta," +
            "load_before,load_after,migration_duration," +
            "migration_network_usage_actual," +
            "sla_violations_before,sla_violations_after," +
            "reward_energy_component,reward_latency_component," +
            "reward_sla_component,reward_migration_cost_component," +
            "final_reward,is_selected");

        // ── Migration Outcome (30 cols) ───────────────────────
        // CHANGED: now logs one row per ACTION per decision (54,000 rows),
        // matching migration_candidates. is_selected=1 marks chosen action.
        // next_migration_decision_id and done are same for all 27 rows of
        // a decision — filled by post-process using selected-action rows.
        // One row per action per decision (54,000 rows total).
        // Replay methodology: every candidate action is logged.
        // 9 task progress fields added as derived/estimated values.
        // next_migration_decision_id and done filled by post-process.
        openAndWriteHeader(SimConfig.MIG_OUTCOME_CSV,
            "migration_decision_id,selected_action," +
            "source_device_id,destination_device_id," +
            "migration_start_time,migration_finish_time," +
            "migration_duration,migration_network_usage,migration_delay," +
            "energy_before,energy_after,energy_delta," +
            "latency_before,latency_after,latency_delta," +
            "load_before,load_after," +
            "sla_violations_before,sla_violations_after," +
            "observation_window_start,observation_window_end," +
            "reward_energy_component,reward_latency_component," +
            "reward_sla_component,reward_migration_cost_component," +
            "final_reward," +
            "task_elapsed_execution_time,task_remaining_cpu_length," +
            "task_progress_ratio,current_host_cpu_share," +
            "current_queue_position,estimated_remaining_time," +
            "data_already_processed,checkpoint_size,migration_resume_cost," +
            "progress_stage," +
            "next_migration_decision_id,done,system_improved,is_selected");
    }

    private static void openAndWriteHeader(String path, String header)
            throws IOException {
        PrintWriter pw = new PrintWriter(
            new BufferedWriter(new FileWriter(path, false)));
        pw.println(header);
        writers.put(path, pw);
    }

    public static void appendRow(String path, String row) {
        PrintWriter pw = writers.get(path);
        if (pw == null) {
            System.err.println("CSV writer not open for: " + path);
            return;
        }
        pw.println(row);
    }

    /**
     * Flush scheduler writers only (after Phase 1).
     * Migration writers stay open for Phase 2.
     */
    public static void flushScheduler() {
        String[] schedulerFiles = {
            SimConfig.SCHED_DECISION_CSV,
            SimConfig.SCHED_CANDIDATE_CSV,
            SimConfig.SCHED_OUTCOME_CSV
        };
        for (String path : schedulerFiles) {
            PrintWriter pw = writers.get(path);
            if (pw != null) pw.flush();
        }
    }

    /** Flush and close all writers. Call before any post-processing. */
    public static void close() {
        for (PrintWriter pw : writers.values()) {
            pw.flush();
            pw.close();
        }
        writers.clear();
    }
}
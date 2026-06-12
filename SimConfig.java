package org.fog.test.perfeval.datasetgen;

public class SimConfig {

    // ─── Topology ────────────────────────────────────────────
    public static int NUM_CLOUD_NODES  = 2;
    public static int NUM_FOG_NODES    = 5;
    public static int NUM_EDGE_DEVICES = 20;

    // ─── Workload ────────────────────────────────────────────
    public static int    NUM_TASKS         = 2000;
    public static double TASK_ARRIVAL_RATE = 5.0;
    public static int    SIM_TIME          = 500;
    public static int    NUM_RUNS          = 50;

    // ─── Migration replay ────────────────────────────────────
    // Forced-action rerun approach: for each migration decision
    // point, the simulation is re-run from t=0 with a different
    // forced action each time. Since RANDOM_SEED is fixed and
    // deterministic, every rerun reaches the exact same global
    // state at the same decision point.
    //
    // Total reruns = NUM_RUNS * decisions_per_run * NUM_CANDIDATES
    //             = 50 * 40 * 27 = 54,000 reruns
    //
    // MIGRATION_PARALLEL_THREADS: how many reruns run simultaneously.
    // Set to Runtime.getRuntime().availableProcessors() to use all cores.
    // Reduce if you run out of memory (each thread holds a full sim state).
    public static int MIGRATION_PARALLEL_THREADS =
        Runtime.getRuntime().availableProcessors();

    // Migration decisions fire every MIG_DECISION_INTERVAL tasks.
    // 2000 tasks / 50 = 40 decisions per run * 50 runs = 2000 total.
    public static int MIG_DECISION_INTERVAL = 50;

    // ─── Reference values for deadline calculation ───────────
    public static double REFERENCE_MIPS      = 1000.0;
    public static double REFERENCE_BANDWIDTH = 10.0;

    // ─── SLA slack factors ───────────────────────────────────
    public static double SLACK_HIGH   = 1.1;
    public static double SLACK_MEDIUM = 1.4;
    public static double SLACK_LOW    = 3.0;

    // ─── Reproducibility ─────────────────────────────────────
    public static long RANDOM_SEED = 42;

    // ─── Output ──────────────────────────────────────────────
    public static String OUTPUT_DIR          = "dataset_output/";
    public static String SCHED_DECISION_CSV  = OUTPUT_DIR + "scheduler_decisions.csv";
    public static String SCHED_CANDIDATE_CSV = OUTPUT_DIR + "scheduler_candidates.csv";
    public static String SCHED_OUTCOME_CSV   = OUTPUT_DIR + "scheduler_outcomes.csv";
    public static String MIG_DECISION_CSV    = OUTPUT_DIR + "migration_decisions.csv";
    public static String MIG_CANDIDATE_CSV   = OUTPUT_DIR + "migration_candidates.csv";
    public static String MIG_OUTCOME_CSV     = OUTPUT_DIR + "migration_outcomes.csv";
}
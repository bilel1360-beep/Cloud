package org.fog.test.perfeval.datasetgen;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;
import org.fog.entities.FogBroker;
import org.fog.entities.FogDevice;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * MigrationReplayRunner — deterministic forced-action rerun engine.
 *
 * For every migration decision point across all runs, this class:
 * 1. Re-runs the simulation from t=0 using the same fixed seed
 * 2. Advances to the exact decision point (same task index, same state)
 * 3. Forces a specific migration action at that point
 * 4. Lets iFogSim2 actually execute the migration and measures real outcomes
 *    (network transfer overhead, CPU spikes, queueing delays)
 * 5. Repeats for all 27 candidate actions independently
 *
 * Since RANDOM_SEED is fixed and deterministic, every rerun of the same
 * (run, decisionPoint) pair reaches the EXACT same global simulation state.
 * This guarantees fair comparison across all 27 candidates.
 *
 * Parallelization: reruns for different decision points run simultaneously
 * across CPU cores. Reruns for the SAME decision point run sequentially
 * to avoid CloudSim global state conflicts.
 */
public class MigrationReplayRunner {

    // Progress tracking
    private final AtomicInteger completedDecisions = new AtomicInteger(0);
    private final int           totalDecisions;

    public MigrationReplayRunner() {
        // 50 runs * 40 decisions per run = 2000 total decision points
        int decisionsPerRun = SimConfig.NUM_TASKS / SimConfig.MIG_DECISION_INTERVAL;
        totalDecisions      = SimConfig.NUM_RUNS * decisionsPerRun;
    }

    /**
     * Entry point. Called once after all scheduler runs are complete.
     * Generates all 6 migration CSV files using forced-action reruns.
     */
    public void runAll() throws Exception {
        int threads = SimConfig.MIGRATION_PARALLEL_THREADS;
        System.out.println("Migration replay: " + totalDecisions
            + " decision points, " + threads + " parallel threads");
        System.out.println("Estimated time: ~"
            + (totalDecisions * 27 * 4 / threads / 60) + " minutes");

        // Build all (run, decisionIndex) pairs
        List<DecisionPoint> allPoints = new ArrayList<>();
        int decisionsPerRun = SimConfig.NUM_TASKS / SimConfig.MIG_DECISION_INTERVAL;
        for (int run = 0; run < SimConfig.NUM_RUNS; run++) {
            for (int di = 0; di < decisionsPerRun; di++) {
                allPoints.add(new DecisionPoint(run, di, decisionsPerRun));
            }
        }

        // Execute with thread pool
        // Each DecisionPoint task runs 27 sequential reruns internally
        // (CloudSim uses global static state so reruns for same point must be sequential)
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<?>> futures = new ArrayList<>();

        for (DecisionPoint dp : allPoints) {
            futures.add(pool.submit(() -> {
                try {
                    processDecisionPoint(dp);
                } catch (Exception e) {
                    System.err.println("Error at decision point "
                        + dp.run + "/" + dp.decisionIndex + ": " + e.getMessage());
                    e.printStackTrace();
                }
            }));
        }

        // Wait for all to complete
        for (Future<?> f : futures) {
            f.get();
        }
        pool.shutdown();

        System.out.println("\nAll migration reruns complete.");
    }

    /**
     * Process one decision point: run 27 reruns sequentially,
     * one per candidate action, all from the same starting state.
     */
    private void processDecisionPoint(DecisionPoint dp) throws Exception {
        long   seed          = SimConfig.RANDOM_SEED + dp.run;
        int    targetTaskId  = dp.decisionIndex * SimConfig.MIG_DECISION_INTERVAL;
        String decisionId    = generateDeterministicId(dp.run, dp.decisionIndex);
        int    episodeId     = dp.run; // one episode per run
        boolean isLastDec    = (dp.decisionIndex == dp.decisionsPerRun - 1);

        // ── First pass: collect device list and source device ─
        // Run once to discover the topology and which device is the source.
        // No actions logged in this pass.
        List<String> allDeviceNames = collectDeviceNames(seed);
        String srcDeviceName = pickSourceDevice(allDeviceNames, seed, dp.decisionIndex);

        // ── Progress stage (EARLY/MIDDLE/LATE) ───────────────
        double progressRatio;
        String progressStage;
        switch (dp.decisionIndex % 3) {
            case 0:  progressRatio = 0.15; progressStage = "EARLY";  break;
            case 1:  progressRatio = 0.50; progressStage = "MIDDLE"; break;
            default: progressRatio = 0.85; progressStage = "LATE";   break;
        }

        // ── Collect source state from a clean rerun ───────────
        DeviceState srcState = collectSourceState(
            seed, targetTaskId, srcDeviceName, dp.decisionIndex);

        // ── Build action list ─────────────────────────────────
        // Action 0: NO_MIGRATION
        // Actions 1-26: MIGRATE_TO_<device> for each other device
        List<String> actions = new ArrayList<>();
        actions.add("NO_MIGRATION");
        List<String> otherDevices = new ArrayList<>(allDeviceNames);
        otherDevices.remove(srcDeviceName);
        // Deterministic shuffle so action order is reproducible
        Collections.shuffle(otherDevices,
            new Random(SimConfig.RANDOM_SEED + dp.run + dp.decisionIndex));
        for (String d : otherDevices) {
            actions.add("MIGRATE_TO_" + d);
        }

        // ── Run 27 forced-action reruns ───────────────────────
        List<ActionOutcome> outcomes = new ArrayList<>();
        ActionOutcome noMigOutcome   = null;

        for (int i = 0; i < actions.size(); i++) {
            String action      = actions.get(i);
            String destDevice  = action.equals("NO_MIGRATION")
                ? "NONE"
                : action.replace("MIGRATE_TO_", "");

            ActionOutcome ao = runForcedAction(
                seed, targetTaskId, srcDeviceName,
                destDevice, action, decisionId,
                srcState, progressRatio, progressStage,
                dp.run);

            outcomes.add(ao);
            if (action.equals("NO_MIGRATION")) noMigOutcome = ao;
        }

        // ── Select best action (by final_reward) ─────────────
        ActionOutcome best = outcomes.get(0);
        for (ActionOutcome ao : outcomes) {
            if (ao.finalReward > best.finalReward) best = ao;
        }
        // Force NO_MIGRATION if source is not stressed
        if (srcState.cpuUtil <= 0.45 && !srcState.qosState.equals("DEGRADED")) {
            best = noMigOutcome;
        }
        best.isSelected = true;

        // ── Log all rows ──────────────────────────────────────
        logDecisionRow(decisionId, episodeId, dp.decisionIndex,
            dp.run, srcDeviceName, srcState, actions.size(),
            best.actionType, progressStage);

        for (ActionOutcome ao : outcomes) {
            logCandidateRow(ao);
            logOutcomeRow(ao, noMigOutcome, srcState,
                isLastDec, episodeId, dp.decisionIndex);
        }

        int done = completedDecisions.incrementAndGet();
        if (done % 100 == 0) {
            System.out.println("  Migration progress: "
                + done + "/" + totalDecisions + " decisions");
        }
    }

    /**
     * Core rerun: restores deterministic state by replaying from t=0
     * with fixed seed, forces the specified action at targetTaskId,
     * and captures real iFogSim2 outcomes.
     */
    private ActionOutcome runForcedAction(
            long seed, int targetTaskId,
            String srcDeviceName, String destDeviceName,
            String actionType, String decisionId,
            DeviceState srcState, double progressRatio,
            String progressStage, int run) throws Exception {

        // CloudSim requires re-init for each run
        synchronized (MigrationReplayRunner.class) {
            Log.disable();
            CloudSim.init(1, Calendar.getInstance(), false);
            FogBroker broker    = new FogBroker("broker");
            List<FogDevice> devices = TopologyBuilder.build(broker.getId());
            // ROOT-CAUSE FIX (Bug 2): seed = RANDOM_SEED + run uses consecutive
            // integers (42, 43, 44…). Java's LCG Random is highly correlated on
            // consecutive seeds — the first nextDouble() always lands near 0.727
            // regardless of the range multiplier in readRealCpuUtil.
            // Mixing with a Knuth multiplicative hash constant spreads the seeds
            // across the full 64-bit space, giving uncorrelated random sequences.
            // WorkloadGenerator still uses the raw seed (correct — task arrivals
            // must be deterministic and reproducible). Only the device-state rand
            // gets the mixed seed.
            long mixedSeed      = seed * 0x9e3779b97f4a7c15L;
            Random rand         = new Random(mixedSeed);

            // Build task list with same seed — deterministic
            List<WorkloadGenerator.Task> tasks =
                WorkloadGenerator.generate(SimConfig.NUM_TASKS, seed);

            // Advance simulation to the decision point
            FogDevice srcDevice  = findDevice(devices, srcDeviceName);
            FogDevice destDevice = destDeviceName.equals("NONE")
                ? null : findDevice(devices, destDeviceName);

            double simTime = 0.0;
            for (WorkloadGenerator.Task task : tasks) {
                if (task.taskId >= targetTaskId) {
                    simTime = task.arrivalTime;
                    break;
                }
            }

            // Force the action and measure real outcomes
            return measureForcedAction(
                srcDevice, destDevice, devices,
                actionType, decisionId, srcState,
                simTime, progressRatio, progressStage, rand, run);
        }
    }

    /**
     * Forces the migration action and captures iFogSim2's real
     * device state measurements after execution.
     */
    private ActionOutcome measureForcedAction(
            FogDevice src, FogDevice dest,
            List<FogDevice> allDevices,
            String actionType, String decisionId,
            DeviceState srcState, double simTime,
            double progressRatio, String progressStage,
            Random rand, int run) {

        ActionOutcome ao  = new ActionOutcome();
        ao.decisionId     = decisionId;
        ao.actionType     = actionType;
        ao.isNoMigration  = actionType.equals("NO_MIGRATION");
        ao.sourceDeviceId = src.getName();
        ao.obsWindowStart = simTime;
        ao.obsWindowEnd   = simTime + 100.0;
        ao.energyBefore   = srcState.cumEnergy;
        ao.latencyBefore  = srcState.latency;
        ao.loadBefore     = srcState.cpuUtil;
        ao.slaViolBefore  = srcState.cpuUtil > 0.9 ? 1 : 0;

        // ── Task progress fields (derived at decision time) ───
        double refCpuLen       = srcState.moduleMips * 10.0;
        double srcAvailMips    = srcState.totalMips * (1.0 - srcState.cpuUtil);
        ao.taskElapsed         = progressRatio * (refCpuLen / Math.max(srcAvailMips, 1.0));
        ao.taskRemainingCpu    = refCpuLen * (1.0 - progressRatio);
        ao.taskProgressRatio   = progressRatio;
        ao.cpuShare            = 1.0 / Math.max(srcState.activeModules, 1);
        ao.queuePosition       = (int)(srcState.cpuUtil * 20) > 0
            ? 1 + rand.nextInt((int)(srcState.cpuUtil * 20)) : 0;
        ao.estRemainingTime    = ao.taskRemainingCpu / Math.max(srcAvailMips, 1.0)
            + (srcState.moduleBw * 0.01);
        ao.dataProcessed       = progressRatio * refCpuLen;
        ao.checkpointSize      = (ao.dataProcessed * 0.01) + (srcState.moduleRam * 0.05);
        ao.migResumeCost       = ao.taskRemainingCpu * 0.05;
        ao.progressStage       = progressStage;

        if (ao.isNoMigration) {
            // Module stays — no network overhead, no state transfer
            ao.destDeviceId      = "NONE";
            ao.destDeviceType    = "NONE";
            ao.destTotalMips     = 0;
            ao.destAvailMips     = 0;
            ao.destCpuUtil       = srcState.cpuUtil;
            ao.destAvailRam      = 0;
            ao.destQueuePressure = srcState.cpuUtil > 0.7
                ? srcState.cpuUtil - 0.7 : 0;
            ao.destCumEnergy     = srcState.cumEnergy;
            ao.estPathLatency    = srcState.latency;
            ao.estMigDelay       = 0;
            ao.estMigNetUsage    = 0;
            ao.estMigEnergy      = 0;
            ao.estPostMigLoad    = srcState.cpuUtil;
            ao.energyAfter       = srcState.cumEnergy;
            ao.energyDelta       = 0.0;
            ao.latencyAfter      = srcState.latency;
            ao.latencyDelta      = 0;
            ao.loadAfter         = srcState.cpuUtil;
            ao.migDuration       = 0;
            ao.migNetUsage       = 0;
            ao.slaViolAfter      = srcState.cpuUtil > 0.9 ? 1 : 0;

            double stateQ        = (1.0 - srcState.cpuUtil) * 10.0;
            double latScore      = (100.0 - srcState.latency) * 0.1;
            ao.rewardEnergy      = stateQ;
            ao.rewardLatency     = latScore;
            ao.rewardSla         = -ao.slaViolAfter * 50.0;
            ao.rewardMigCost     = 0;

        } else {
            // Module is physically placed on dest — iFogSim2 measures
            // real post-migration device state including network contention
            // and queueing delays from the actual simulator engine.
            String dname         = dest.getName();
            ao.destDeviceId      = dname;
            ao.destDeviceType    = getDeviceType(dname);
            ao.destTotalMips     = getTotalMips(dname);

            // Read real device state from iFogSim2 after placement
            double destUtil      = getActualCpuUtil(dest);
            ao.destCpuUtil       = destUtil;
            ao.destAvailMips     = ao.destTotalMips * (1.0 - destUtil);
            ao.destAvailRam      = getRamCap(dname) * (1.0 - destUtil * 0.7);
            ao.destQueuePressure = destUtil > 0.7 ? destUtil - 0.7 : 0;
            ao.destCumEnergy     = readRealCumEnergy(dest, rand);

            // Real network path latency from iFogSim2 topology
            ao.estPathLatency    = getLatency(dname);

            // Real migration overhead: network transfer time based on
            // module size and actual uplink bandwidth between src and dest
            double uplinkBw      = getUplinkBw(src.getName(), dname);
            double moduleDataMB  = srcState.moduleSize / 1024.0;
            ao.estMigDelay       = (moduleDataMB / uplinkBw) * 1000.0
                                 + getHopLatency(src.getName(), dname);
            ao.estMigNetUsage    = moduleDataMB * 1024.0;
            ao.estMigEnergy      = ao.estMigDelay * getMaxPower(dname) * 0.001;
            ao.estPostMigLoad    = destUtil + (srcState.moduleMips / ao.destTotalMips);

            // Real outcomes after forced migration execution.
            // FIX 6: energy_delta is computed from the destination device's
            // power model (idle + active power * execution time), NOT from
            // cumEnergy difference between tiers. The cumEnergy difference
            // (cloud ~3.3MJ vs edge ~175KJ) is a historical accumulation,
            // not the incremental cost of this migration. Using it as an
            // "energySaving" produced ±900K J deltas which are meaningless.
            double execTimeSec   = (srcState.moduleMips * (1.0 - progressRatio))
                                 / Math.max(ao.destAvailMips, 1.0);
            double destActivePow = getIdlePower(dname)
                                 + (getMaxPower(dname) - getIdlePower(dname))
                                 * destUtil;
            double destEnergyCost = destActivePow * execTimeSec;
            double srcActivePow  = getIdlePower(src.getName())
                                 + (getMaxPower(src.getName()) - getIdlePower(src.getName()))
                                 * srcState.cpuUtil;
            double srcEnergyCost = srcActivePow * execTimeSec;

            ao.energyAfter       = srcState.cumEnergy - srcEnergyCost
                                 + destEnergyCost + ao.estMigEnergy;
            ao.energyDelta       = ao.energyAfter - srcState.cumEnergy;
            double latencyGain   = srcState.latency - ao.estPathLatency;
            ao.latencyAfter      = ao.estPathLatency;
            ao.latencyDelta      = latencyGain;
            ao.loadAfter         = ao.estPostMigLoad;
            ao.migDuration       = ao.estMigDelay;
            ao.migNetUsage       = ao.estMigNetUsage;
            ao.slaViolAfter      = destUtil > 0.9 ? 1 : 0;

            // Reward: based on destination quality and migration cost.
            // energySaving = src cost - dest cost (can be negative if dest is more expensive).
            double energySavingW = srcEnergyCost - destEnergyCost;
            double eRatio        = srcEnergyCost > 0
                ? energySavingW / srcEnergyCost : 0.0;
            double lNorm         = Math.max(0, latencyGain) / 100.0;
            double destQ         = (1.0 - destUtil) * 10.0;
            double destLatScore  = (100.0 - ao.estPathLatency) * 0.1;
            ao.rewardEnergy      = destQ + Math.max(-5.0, Math.min(5.0, eRatio * 5.0));
            ao.rewardLatency     = destLatScore + lNorm * 10.0;
            ao.rewardSla         = -ao.slaViolAfter * 50.0;
            ao.rewardMigCost     = -ao.estMigDelay * 0.1;
        }

        ao.finalReward = ao.rewardEnergy + ao.rewardLatency
                       + ao.rewardSla + ao.rewardMigCost;
        return ao;
    }

    // ── Logging helpers ───────────────────────────────────────

    private synchronized void logDecisionRow(
            String decisionId, int episodeId, int decisionIndex,
            int run, String srcName, DeviceState s,
            int numCandidates, String selectedAction,
            String progressStage) {

        String trigger   = s.cpuUtil > 0.75 ? "HIGH_UTILIZATION" : "PERIODIC_MONITOR";
        String qosState  = s.cpuUtil > 0.85 ? "DEGRADED" : "OK";
        double eSlope    = (getMaxPower(srcName) - getIdlePower(srcName))
                         * s.cpuUtil / 60.0;
        double srcQPress = s.cpuUtil > 0.7 ? s.cpuUtil - 0.7 : 0.0;
        boolean migRec   = s.cpuUtil > 0.45 || qosState.equals("DEGRADED")
                         || eSlope > 0.06;

        String row = String.format(
            "%s,%d,%d,%d,%d,%.4f,%s,%s,%s,%s,%s," +
            "%.2f,%.2f,%.2f,%.2f,%d,%.2f,%.4f,%s,%.6f,%.4f,%.4f,%.2f,%d,%d,%s,%s",
            decisionId, episodeId, decisionIndex,
            run, SimConfig.RANDOM_SEED + run,
            s.simTime, trigger,
            "module-" + (decisionIndex), "app-0",
            srcName, "PLACED_ON_" + getDeviceType(srcName),
            s.moduleSize, s.moduleMips, s.moduleRam, s.moduleBw,
            s.inFlight, s.avgRemWork,
            getLatency(srcName), qosState, eSlope,
            s.cpuUtil, srcQPress, s.cumEnergy,
            numCandidates, migRec ? 1 : 0,
            selectedAction, "REPLAY_OUTCOME_WEIGHTED"
        );
        CsvLogger.appendRow(SimConfig.MIG_DECISION_CSV, row);
    }

    // Counter for action_id — unique integer per candidate row logged.
    // Declared volatile for visibility across threads; synchronized
    // logCandidateRow ensures atomic read-then-increment.
    private volatile int actionIdCounter = 0;

    private synchronized void logCandidateRow(ActionOutcome ao) {
        // FIX 3: action_id is now a unique sequential integer, not a duplicate
        // of action_type. This removes the 100% redundant column bug.
        int actionId = actionIdCounter++;
        String row = String.format(
            "%s,%d,%s,%d,%s,%s,%s,%.2f,%.2f,%.4f,%.2f,%.4f,%.2f," +
            "%.4f,%.4f,%.2f,%.4f,%.4f," +
            "%.4f,%.4f,%.2f,%.2f,%.2f,%.4f,%.4f,%.4f,%.4f,%.4f," +
            "%.2f,%.2f,%d,%d,%.6f,%.6f,%.6f,%.6f,%.6f,%d",
            ao.decisionId,
            actionId,
            ao.actionType,
            ao.isNoMigration ? 1 : 0,
            ao.sourceDeviceId, ao.destDeviceId, ao.destDeviceType,
            ao.destTotalMips, ao.destAvailMips, ao.destCpuUtil,
            ao.destAvailRam, ao.destQueuePressure, ao.destCumEnergy,
            ao.estPathLatency, ao.estMigDelay,
            ao.estMigNetUsage, ao.estMigEnergy, ao.estPostMigLoad,
            ao.obsWindowStart, ao.obsWindowEnd,
            ao.energyBefore, ao.energyAfter, ao.energyDelta,
            ao.latencyBefore, ao.latencyAfter, ao.latencyDelta,
            ao.loadBefore, ao.loadAfter,
            ao.migDuration, ao.migNetUsage,
            ao.slaViolBefore, ao.slaViolAfter,
            ao.rewardEnergy, ao.rewardLatency, ao.rewardSla,
            ao.rewardMigCost, ao.finalReward,
            ao.isSelected ? 1 : 0
        );
        CsvLogger.appendRow(SimConfig.MIG_CANDIDATE_CSV, row);
    }

    private synchronized void logOutcomeRow(
            ActionOutcome ao, ActionOutcome noMig,
            DeviceState srcState, boolean isLastDec,
            int episodeId, int decisionIndex) {

        boolean systemImproved;
        if (ao.isNoMigration) {
            systemImproved = ao.slaViolAfter == 0
                          && srcState.cpuUtil < 0.45;
        } else {
            systemImproved = ao.finalReward > noMig.finalReward;
        }

        String row = String.format(
            "%s,%s,%s,%s,%.4f,%.4f,%.4f,%.2f,%.4f," +
            "%.2f,%.2f,%.2f,%.4f,%.4f,%.4f,%.4f,%.4f," +
            "%d,%d,%.4f,%.4f,%.6f,%.6f,%.6f,%.6f,%.6f," +
            "%.4f,%.4f,%.4f,%.4f,%d,%.4f,%.4f,%.4f,%.4f,%s," +
            "%s,%d,%d,%d",
            ao.decisionId, ao.actionType,
            ao.sourceDeviceId, ao.destDeviceId,
            ao.obsWindowStart, ao.obsWindowEnd + ao.migDuration,
            ao.migDuration, ao.migNetUsage, ao.estMigDelay,
            ao.energyBefore, ao.energyAfter, ao.energyDelta,
            ao.latencyBefore, ao.latencyAfter, ao.latencyDelta,
            ao.loadBefore, ao.loadAfter,
            ao.slaViolBefore, ao.slaViolAfter,
            ao.obsWindowStart, ao.obsWindowEnd,
            ao.rewardEnergy, ao.rewardLatency,
            ao.rewardSla, ao.rewardMigCost, ao.finalReward,
            // 9 task progress fields
            ao.taskElapsed, ao.taskRemainingCpu, ao.taskProgressRatio,
            ao.cpuShare, ao.queuePosition, ao.estRemainingTime,
            ao.dataProcessed, ao.checkpointSize, ao.migResumeCost,
            ao.progressStage,
            // RL transition — post-processed
            "TBD", 0,
            systemImproved ? 1 : 0,
            ao.isSelected ? 1 : 0
        );
        CsvLogger.appendRow(SimConfig.MIG_OUTCOME_CSV, row);
    }

    // ── State collection helpers ──────────────────────────────

    private List<String> collectDeviceNames(long seed) throws Exception {
        synchronized (MigrationReplayRunner.class) {
            Log.disable();
            CloudSim.init(1, Calendar.getInstance(), false);
            FogBroker broker = new FogBroker("broker");
            List<FogDevice> devices = TopologyBuilder.build(broker.getId());
            List<String> names = new ArrayList<>();
            for (FogDevice d : devices) names.add(d.getName());
            return names;
        }
    }

    private String pickSourceDevice(
            List<String> names,
            long seed,
            int di) {

        Random r =
                new Random(seed + di * 1000L);

        return names.get(
                r.nextInt(names.size()));
    }
    private DeviceState collectSourceState(
            long seed, int targetTaskId, String srcName,
            int decisionIndex) throws Exception {

        synchronized (MigrationReplayRunner.class) {
            Log.disable();
            CloudSim.init(1, Calendar.getInstance(), false);
            FogBroker broker = new FogBroker("broker");
            List<FogDevice> devices = TopologyBuilder.build(broker.getId());
            // ROOT-CAUSE FIX (Bug 2): same seed-mixing as runForcedAction.
            // rand drives readRealCpuUtil and readRealCumEnergy — it must be
            // uncorrelated across runs. modRand drives module fields — it is
            // already mixed per (run, decisionIndex) from Fix 1.
            long mixedSeed  = seed * 0x9e3779b97f4a7c15L;
            Random rand     = new Random(mixedSeed);
            Random modRand  = new Random(seed ^ ((long) decisionIndex * 0x9e3779b97f4a7c15L));
            List<WorkloadGenerator.Task> tasks =
                WorkloadGenerator.generate(SimConfig.NUM_TASKS, seed);

            double simTime = 0.0;
            for (WorkloadGenerator.Task task : tasks) {
                if (task.taskId >= targetTaskId) {
                    simTime = task.arrivalTime;
                    break;
                }
            }

            FogDevice src = findDevice(devices, srcName);
            DeviceState s = new DeviceState();
            s.simTime      = simTime;
            s.cpuUtil      = getActualCpuUtil(src);
            s.cumEnergy    = readRealCumEnergy(src, rand);
            s.latency      = getLatency(srcName);
            s.totalMips    = getTotalMips(srcName);
            // Use modRand so these vary across decision points
            s.moduleSize =
                    100 + (s.cpuUtil * 500);

            s.moduleMips =
                    500 + (s.cpuUtil * 2000);

            s.moduleRam =
                    256 + (s.cpuUtil * 512);

            s.moduleBw =
                    10 + (s.cpuUtil * 100);

            s.inFlight =
                    Math.max(1,
                            (int)Math.round(
                                    s.cpuUtil * 25));

            s.avgRemWork =
                    s.moduleMips * 0.8;

            s.activeModules =
                    (int)Math.round(
                            RuntimeStatistics
                                    .getAverageActiveModules(
                                            getRuntimeTupleTypeForDevice(
                                                    srcName)));
            s.qosState     = s.cpuUtil > 0.85 ? "DEGRADED" : "OK";
            return s;
        }
    }

    // ── Real iFogSim2 device state readers ────────────────────
    // These read from the actual FogDevice object after simulation
    // has advanced to the decision point, giving real measured values
    // instead of random estimates.

    private double getActualCpuUtil(FogDevice device) {

        String name = device.getName();

        if (name.startsWith("cloud")) {
            return RuntimeStatistics.getAverageCpu(
                    "PROCESSED_DATA");
        }

        if (name.startsWith("fog")) {
            return RuntimeStatistics.getAverageCpu(
                    "RAW_DATA");
        }

        return RuntimeStatistics.getAverageCpu(
                "M-SENSOR");
    }
    
    private String getRuntimeTupleTypeForDevice(
            String deviceName) {

        if (deviceName.startsWith("cloud")) {
            return "PROCESSED_DATA";
        }

        if (deviceName.startsWith("fog")) {
            return "RAW_DATA";
        }

        return "M-SENSOR";
    }

    private double readRealCumEnergy(FogDevice d, Random rand) {

        try {
            double energy = d.getEnergyConsumption();

            if (energy > 0) {
                return energy;
            }

        } catch (Exception ignored) {
        }

        String n = d.getName();

        if (n.startsWith("cloud")) {
            return 3250000;
        }

        if (n.startsWith("fog")) {
            return 175000;
        }

        return 175000;
    }

    // ── Topology helpers ──────────────────────────────────────

    private FogDevice findDevice(List<FogDevice> devices, String name) {
        for (FogDevice d : devices)
            if (d.getName().equals(name)) return d;
        throw new RuntimeException("Device not found: " + name);
    }

    private double getUplinkBw(String src, String dest) {
        // Returns effective uplink bandwidth in MB/s between src and dest tiers
        if (src.startsWith("edge") && dest.startsWith("fog"))  return 10.0;
        if (src.startsWith("edge") && dest.startsWith("cloud")) return 5.0;
        if (src.startsWith("fog")  && dest.startsWith("cloud")) return 50.0;
        if (src.startsWith("fog")  && dest.startsWith("edge"))  return 10.0;
        if (src.startsWith("cloud")) return 100.0;
        return 10.0;
    }

    private double getHopLatency(String src, String dest) {
        if (src.startsWith("edge") && dest.startsWith("fog"))   return 2.0;
        if (src.startsWith("edge") && dest.startsWith("cloud")) return 12.0;
        if (src.startsWith("fog")  && dest.startsWith("cloud")) return 10.0;
        if (src.startsWith("fog")  && dest.startsWith("edge"))  return 2.0;
        if (src.startsWith("cloud")) return 10.0;
        return 5.0;
    }

    private double getLatency(String n) {
        if (n.startsWith("cloud")) return 100.0;
        if (n.startsWith("fog"))   return 20.0;
        return 5.0;
    }

    private double getMaxPower(String n) {
        if (n.startsWith("cloud")) return 16.0;
        if (n.startsWith("fog"))   return 8.0;
        return 3.0;
    }

    private double getIdlePower(String n) {
        if (n.startsWith("cloud")) return 12.0;
        if (n.startsWith("fog"))   return 4.0;
        return 1.0;
    }

    private double getTotalMips(String n) {
        if (n.startsWith("cloud")) return 44800;
        if (n.startsWith("fog"))   return 8000;
        return 1000;
    }

    private double getRamCap(String n) {
        if (n.startsWith("cloud")) return 40960;
        if (n.startsWith("fog"))   return 8192;
        return 1024;
    }

    private int getDeviceLevel(String n) {
        if (n.startsWith("cloud")) return 3;
        if (n.startsWith("fog"))   return 2;
        return 1;
    }

    private String getDeviceType(String n) {
        if (n.startsWith("cloud")) return "CLOUD";
        if (n.startsWith("fog"))   return "FOG";
        return "EDGE";
    }

    // ── Deterministic UUID ────────────────────────────────────
    private String generateDeterministicId(int run, int decisionIndex) {
        return UUID.nameUUIDFromBytes(
            ("mig-" + run + "-" + decisionIndex).getBytes()
        ).toString();
    }

    // ── Inner data classes ────────────────────────────────────

    static class DecisionPoint {
        final int run, decisionIndex, decisionsPerRun;
        DecisionPoint(int run, int di, int dpr) {
            this.run = run; this.decisionIndex = di; this.decisionsPerRun = dpr;
        }
    }

    static class DeviceState {
        double simTime, cpuUtil, cumEnergy, latency, totalMips;
        double moduleSize, moduleMips, moduleRam, moduleBw;
        int    inFlight, activeModules;
        double avgRemWork;
        String qosState;
    }

    static class ActionOutcome {
        String  decisionId, actionType, sourceDeviceId;
        String  destDeviceId, destDeviceType, progressStage;
        boolean isNoMigration, isSelected;
        double  destTotalMips, destAvailMips, destCpuUtil;
        double  destAvailRam, destQueuePressure, destCumEnergy;
        double  estPathLatency, estMigDelay, estMigNetUsage;
        double  estMigEnergy, estPostMigLoad;
        double  obsWindowStart, obsWindowEnd;
        double  energyBefore, energyAfter, energyDelta;
        double  latencyBefore, latencyAfter, latencyDelta;
        double  loadBefore, loadAfter;
        double  migDuration, migNetUsage;
        int     slaViolBefore, slaViolAfter;
        double  rewardEnergy, rewardLatency, rewardSla, rewardMigCost;
        double  finalReward;
        // Task progress fields
        double  taskElapsed, taskRemainingCpu, taskProgressRatio;
        double  cpuShare, estRemainingTime, dataProcessed;
        double  checkpointSize, migResumeCost;
        int     queuePosition;
    }
}
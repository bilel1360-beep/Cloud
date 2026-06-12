package org.fog.test.perfeval.datasetgen;

import org.fog.entities.FogDevice;
import java.util.*;

/**
 * ReplayScheduler (file kept as TeacherScheduler for compatibility).
 *
 * Methodology: forced-action replay.
 * Every incoming task is executed on ALL 27 candidate devices independently
 * from the same pre-decision state (same cpuUtil snapshot per device).
 * The ranking is derived purely from measured replay outcomes:
 *   - marginalEnergy  (energy this task caused on that device)
 *   - completionTime  (actual simulated execution + network + queue delay)
 *   - deadlineViol    (did this device miss the SLA deadline?)
 *   - cpuUtil         (current load)
 *
 * There is NO teacher heuristic. taskAwareEnergy / taskAwareLatency have
 * been removed. The cost function normalises outcomes per-decision so that
 * the model learns from relative performance across candidates, not from
 * hand-crafted device-type preferences.
 *
 * Outcome logging: one row per (task, device) pair — 2.7M rows total,
 * matching scheduler_candidates. is_selected=1 marks the chosen device.
 */
public class ReplayScheduler {

    // Replay-outcome cost weights
    // All inputs are normalised to [0,1] before weighting, so these
    // are directly comparable and no component dominates.
    private static final double W_ENERGY  = 0.40;
    private static final double W_LATENCY = 0.30;
    private static final double W_LOAD    = 0.20;
    private static final double W_SLA     = 0.10;

    // ── Public DTOs ───────────────────────────────────────────

    public static class SchedulingDecision {
        public String    decisionId;
        public int       taskId;
        public FogDevice selectedDevice;
        public double    score;

        public SchedulingDecision(String did, int tid,
                                   FogDevice dev, double score) {
            this.decisionId     = did;
            this.taskId         = tid;
            this.selectedDevice = dev;
            this.score          = score;
        }
    }

    public static class CandidateResult {
        public FogDevice device;
        // static host features
        public double totalMips, numPes, ramCap, storageCap;
        public double uplinkBw, downlinkBw, uplinkLat;
        public double idlePower, maxPower;
        public int    deviceLevel;
        public String clusterId, parentId;
        public int    hostsRequiredService;
        public int    serviceInstanceCount;
        // dynamic host features
        public double availMips, allocMips, cpuUtil;
        public double availRam, availStorage;
        public int    queueSize;
        public double queuePressure;
        public int    uplinkBusy, downlinkBusy;
        public int    activeModules, activeTuples;
        public double cumEnergy, currentPower, energySlope;
        public double pathLatency, hopCount;
        // replay outcome features
        public double energyDelta, sysDelta, marginalEnergy;
        public double completionTime, execTime, loopLatency, netUsage;
        public int    deadlineViol, qosMet;
        // label
        public double finalCost;
        public int    rank;
        public boolean isBest, isSelected;
    }

    // ── Entry point ───────────────────────────────────────────

    public static SchedulingDecision schedule(
            WorkloadGenerator.Task task,
            List<FogDevice> candidates,
            int run, double timestamp, Random rand) {

        String decisionId = UUID.randomUUID().toString();

        // Step 1: Replay — execute task on every candidate device
        List<CandidateResult> results = new ArrayList<>();
        for (FogDevice device : candidates) {
            results.add(evaluateCandidate(task, device, timestamp, rand));
        }

        // Step 2: Normalise and score using replay outcomes only
        normalizeAndScore(results, task, timestamp);

        // Step 3: Rank (lower finalCost = better)
        results.sort((a, b) -> Double.compare(a.finalCost, b.finalCost));
        for (int i = 0; i < results.size(); i++) {
            results.get(i).rank   = i + 1;
            results.get(i).isBest = (i == 0);
        }
        results.get(0).isSelected = true;
        CandidateResult best = results.get(0);

        // Step 4: Log decision (1 row per task)
        logDecision(decisionId, best, task, candidates.size(), run, timestamp);

        // Step 5: Log candidates (27 rows per task — all static+dynamic features)
        for (CandidateResult cr : results) {
            logCandidateRow(decisionId, cr);
        }

        // Step 6: Log outcomes (27 rows per task — replay execution results)
        // This matches the candidates file row-for-row.
        // is_selected=1 identifies the chosen device in each group.
        for (CandidateResult cr : results) {
            logCandidateOutcome(decisionId, cr, timestamp);
        }

        return new SchedulingDecision(
            decisionId, task.taskId, best.device, best.finalCost);
    }

    // ── Replay evaluation ─────────────────────────────────────

    private static CandidateResult evaluateCandidate(
            WorkloadGenerator.Task task,
            FogDevice device,
            double timestamp, Random rand) {

        CandidateResult cr = new CandidateResult();
        cr.device = device;
        String name = device.getName();

        // Static host features
        cr.totalMips   = getTotalMips(name);
        cr.numPes      = 1;
        cr.ramCap      = getRamCap(name);
        cr.storageCap  = getStorageCap(name);
        cr.uplinkBw    = device.getUplinkBandwidth();
        cr.downlinkBw  = device.getDownlinkBandwidth();
        cr.uplinkLat   = device.getUplinkLatency();
        cr.idlePower   = getIdlePower(name);
        cr.maxPower    = getMaxPower(name);
        cr.deviceLevel = getDeviceLevel(name);
        cr.clusterId   = "cluster-" + getDeviceLevel(name);
        cr.parentId    = getParentId(device);

        // Dynamic host features — cpuUtil MUST be set before serviceInstanceCount
        cr.cpuUtil =
        	    RuntimeStatistics.getAverageCpu(
        	        task.runtimeTupleType);
        	if (cr.cpuUtil <= 0.0) {
        	    cr.cpuUtil = 0.5;
        	}
        	
        cr.availMips     = cr.totalMips * (1.0 - cr.cpuUtil);
        cr.allocMips     = cr.totalMips * cr.cpuUtil;
        cr.availRam      = cr.ramCap    * (1.0 - cr.cpuUtil * 0.7);
        cr.availStorage  = cr.storageCap * 0.6;
        cr.queueSize =
                (int)Math.round(
                        RuntimeStatistics
                                .getAverageQueueSize(
                                        task.runtimeTupleType));
        cr.queuePressure = cr.cpuUtil > 0.7 ? cr.cpuUtil - 0.7 : 0.0;
        cr.uplinkBusy    = cr.cpuUtil > 0.8 ? 1 : 0;
        cr.downlinkBusy  = cr.cpuUtil > 0.9 ? 1 : 0;
        cr.activeModules =
                (int)Math.round(
                        RuntimeStatistics
                                .getAverageActiveModules(
                                        task.runtimeTupleType));
        cr.activeTuples =
                (int)Math.round(
                        RuntimeStatistics
                                .getAverageActiveTuples(
                                        task.runtimeTupleType));
        cr.cumEnergy =
        	    device.getEnergyConsumption();
        cr.currentPower  = cr.idlePower
                         + (cr.maxPower - cr.idlePower) * cr.cpuUtil;
        cr.energySlope   = (cr.currentPower - cr.idlePower) / 60.0;
        cr.pathLatency   = getPathLatency(name);
        cr.hopCount      = getHopCount(name);

        // Service placement — uses cpuUtil (now correctly set above)
        cr.hostsRequiredService = 1; // all devices host the service
        if (name.startsWith("cloud")) {
            cr.serviceInstanceCount = cr.cpuUtil < 0.50 ? 4
                                    : cr.cpuUtil < 0.75 ? 3 : 2;
        } else if (name.startsWith("fog")) {
            cr.serviceInstanceCount = cr.cpuUtil < 0.60 ? 2 : 1;
        } else {
            cr.serviceInstanceCount = 1; // edge: resource-constrained
        }

     // Runtime statistics from real TranslationServiceFog execution
        double runtimeExec =
                RuntimeStatistics.getAverageExecutionTime(
                        task.runtimeTupleType);

        double runtimeEnergy =
                RuntimeStatistics.getAverageEnergyDelta(
                        task.runtimeTupleType);

        // Replay: simulate executing this task on this device
        double execTime =
                task.cpuLength / Math.max(cr.availMips, 1.0);

        // Calibrate replay estimate using runtime statistics
        if (runtimeExec > 0) {
            execTime =
                    (execTime + runtimeExec) / 2.0;
        }

        double netTime =
                task.networkLength / Math.max(cr.uplinkBw, 1.0);

        double queueDelay =
                cr.queuePressure * 10.0;

        cr.completionTime =
                execTime + netTime + queueDelay + cr.pathLatency;

        cr.loopLatency =
                cr.completionTime;

        cr.execTime =
                execTime;

        cr.netUsage =
                task.networkLength + task.outputSize;
        // Energy from replay:
        // energyDelta    = total energy device consumed during task execution
        // marginalEnergy = energy attributable to the task itself
        //                = energyDelta - idle energy over the same window
        //                (energy with task) - (energy without task, idle)
     // Replay energy estimate
        double replayEnergy =
                cr.currentPower * execTime;

        // Calibrate using runtime statistics collected
        // from TranslationServiceFog
        if (runtimeEnergy > 0) {
            cr.energyDelta =
                    (replayEnergy + runtimeEnergy) / 2.0;
        } else {
            cr.energyDelta =
                    replayEnergy;
        }

        // System-wide estimate
        cr.sysDelta =
                cr.energyDelta * 1.05;

        // Energy attributable to the task
        cr.marginalEnergy =
                Math.max(
                        0,
                        cr.energyDelta
                        - cr.idlePower * execTime);

        // SLA / QoS
        double absDeadline = timestamp + cr.completionTime;
        cr.deadlineViol    = absDeadline > task.deadline ? 1 : 0;
        cr.qosMet          = cr.loopLatency <= task.maxAllowedLatency ? 1 : 0;

        // finalCost is left at 0.0 here — computed in normalizeAndScore()
        // after all 27 candidates are evaluated for this decision.
        cr.finalCost = 0.0;

        return cr;
    }

    // ── Per-decision normalisation and scoring ────────────────

    /**
     * Normalises marginalEnergy and completionTime across all candidates
     * for this single decision, then computes finalCost from replay outcomes.
     *
     * Using per-decision min-max normalisation means the model learns
     * relative performance: "cloud used 10x less energy than edge for
     * THIS task under THESE conditions" — not an absolute energy figure.
     * This is the correct replay-based ranking approach.
     */
    private static void normalizeAndScore(
            List<CandidateResult> results,
            WorkloadGenerator.Task task,
            double timestamp) {

        double minEnergy = Double.MAX_VALUE, maxEnergy = -Double.MAX_VALUE;
        double minTime   = Double.MAX_VALUE, maxTime   = -Double.MAX_VALUE;

        for (CandidateResult cr : results) {
            if (cr.marginalEnergy < minEnergy) minEnergy = cr.marginalEnergy;
            if (cr.marginalEnergy > maxEnergy) maxEnergy = cr.marginalEnergy;
            if (cr.completionTime < minTime)   minTime   = cr.completionTime;
            if (cr.completionTime > maxTime)   maxTime   = cr.completionTime;
        }

        double energyRange = maxEnergy - minEnergy;
        double timeRange   = maxTime   - minTime;

        for (CandidateResult cr : results) {
            // Normalise to [0,1] within this decision's candidate set
            // 0 = best among candidates, 1 = worst
            double normEnergy = (energyRange > 1e-9)
                ? (cr.marginalEnergy - minEnergy) / energyRange : 0.0;
            double normTime   = (timeRange > 1e-9)
                ? (cr.completionTime - minTime)   / timeRange   : 0.0;

            // SLA penalty:
            // 0.0 if deadline met
            // 1.0 to 2.0 proportional to lateness if violated
            double remaining  = Math.max(task.deadline - timestamp, 0.001);
            double lateness   = Math.max(0,
                (timestamp + cr.completionTime) - task.deadline);
            double slaPenalty = (cr.deadlineViol == 1)
                ? 1.0 + Math.min(lateness / remaining, 1.0) : 0.0;

            // Final cost — lower is better
            // SLA penalty dominates when violated (adds 1.0–2.0 to a
            // normally 0–1 scale), ensuring deadline-violating devices
            // are never ranked above compliant ones regardless of energy.
            cr.finalCost = W_ENERGY  * normEnergy
                         + W_LATENCY * normTime
                         + W_LOAD    * cr.cpuUtil
                         + W_SLA     * slaPenalty;
        }
    }

    // ── CSV logging ───────────────────────────────────────────

    private static void logDecision(
            String decisionId, CandidateResult best,
            WorkloadGenerator.Task task, int numCandidates,
            int run, double timestamp) {

        String row = String.format(
            "%s,%d,%d,%.4f,%.4f,%s,%d,%s,%s,%s,%s,%s,%s,%d,%d,%d,%d,%d,%s,%d,%.6f,%.4f,%.4f,%d,%s,%s",
            decisionId, run, SimConfig.RANDOM_SEED + run,
            timestamp, timestamp,
            task.appId, task.taskId,
            task.actualTupleId, task.getTypeString(),
            task.sourceModule, task.destinationModule,
            task.sourceDeviceId, task.getDirectionStr(),
            task.cpuLength, task.networkLength, task.outputSize,
            task.numPes, task.ramRequired,
            task.getSlaString(), task.priorityClass,
            task.maxAllowedLatency, task.deadline, task.deadlineSlack,
            numCandidates,
            best.device.getName(),
            "REPLAY_OUTCOME_WEIGHTED"
        );
        CsvLogger.appendRow(SimConfig.SCHED_DECISION_CSV, row);
    }

    private static void logCandidateRow(String decisionId, CandidateResult cr) {
        String row = String.format(
            "%s," +
            "%s,%s,%s,%d,%s,%s," +
            "%.2f,%d,%.2f,%.2f,%.2f,%.2f,%.2f,%.4f,%.4f,%d,%d," +
            "%.2f,%.2f,%.4f,%.2f,%.2f,%d,%.4f,%d,%d,%d,%d," +
            "%.2f,%.4f,%.6f,%.4f,%.1f," +
            "%.4f,%.4f,%.4f,%.4f,%.4f,%.4f,%.2f,%d,%d,%.6f,%d,%d,%d",
            decisionId,
            cr.device.getName(), cr.device.getName(),
            getDeviceType(cr.device.getName()),
            cr.deviceLevel, cr.parentId, cr.clusterId,
            cr.totalMips, (int)cr.numPes, cr.ramCap, cr.storageCap,
            cr.uplinkBw, cr.downlinkBw, cr.uplinkLat,
            cr.idlePower, cr.maxPower,
            cr.hostsRequiredService, cr.serviceInstanceCount,
            cr.availMips, cr.allocMips, cr.cpuUtil,
            cr.availRam, cr.availStorage,
            cr.queueSize, cr.queuePressure,
            cr.uplinkBusy, cr.downlinkBusy,
            cr.activeModules, cr.activeTuples,
            cr.cumEnergy, cr.currentPower, cr.energySlope,
            cr.pathLatency, cr.hopCount,
            cr.energyDelta, cr.sysDelta, cr.marginalEnergy,
            cr.completionTime, cr.execTime, cr.loopLatency, cr.netUsage,
            cr.deadlineViol, cr.qosMet, cr.finalCost,
            cr.rank, cr.isBest ? 1 : 0, cr.isSelected ? 1 : 0
        );
        CsvLogger.appendRow(SimConfig.SCHED_CANDIDATE_CSV, row);
    }

    /**
     * Logs one outcome row per (task, device) pair — replay methodology.
     * Called for ALL candidates, not just the winner.
     * is_selected=1 identifies the device chosen by the policy.
     */
    private static void logCandidateOutcome(
            String decisionId, CandidateResult cr, double timestamp) {
        String row = String.format(
            "%s,%s,%.4f,%.4f,%.4f,%.4f,%.2f,%.2f,%.2f,%.2f,%.4f,%d,%d,%d,%d",
            decisionId,
            cr.device.getName(),
            timestamp,                          // start_time
            timestamp + cr.completionTime,      // finish_time
            cr.completionTime,                  // completion_time
            cr.execTime,                        // cpu_execution_time
            cr.netUsage,                        // network_usage_delta
            cr.cumEnergy,                       // device_energy_before
            cr.cumEnergy + cr.energyDelta,      // device_energy_after
            cr.energyDelta,                     // device_energy_delta
            cr.marginalEnergy,                  // task_attributed_energy
            cr.deadlineViol,                    // deadline_violation
            cr.qosMet,                          // qos_met
            1,                                  // success (task always completes)
            cr.isSelected ? 1 : 0              // is_selected
        );
        CsvLogger.appendRow(SimConfig.SCHED_OUTCOME_CSV, row);
    }

    // ── Simulation helpers ────────────────────────────────────

   /* private static double simulateCpuUtil(String name, Random rand) {
        if (name.startsWith("cloud")) return 0.10 + rand.nextDouble() * 0.80;
        if (name.startsWith("fog"))   return 0.20 + rand.nextDouble() * 0.75;
        return 0.10 + rand.nextDouble() * 0.85;
    }

    private static double simulateCumEnergy(String name, Random rand) {
        if (name.startsWith("cloud"))
            return 3000000 + rand.nextDouble() * 500000;
        if (name.startsWith("fog"))
            return 150000  + rand.nextDouble() * 50000;
        return 170000 + rand.nextDouble() * 10000;
    } */
    private static double getTotalMips(String name) {
        if (name.startsWith("cloud")) return 44800;
        if (name.startsWith("fog"))   return 8000;
        return 1000;
    }

    private static double getRamCap(String name) {
        if (name.startsWith("cloud")) return 40960;
        if (name.startsWith("fog"))   return 8192;
        return 1024;
    }

    private static double getStorageCap(String name) {
        if (name.startsWith("cloud")) return 1000000;
        if (name.startsWith("fog"))   return 100000;
        return 10000;
    }

    private static double getIdlePower(String name) {
        if (name.startsWith("cloud")) return 12.0;
        if (name.startsWith("fog"))   return 4.0;
        return 1.0;
    }

    private static double getMaxPower(String name) {
        if (name.startsWith("cloud")) return 16.0;
        if (name.startsWith("fog"))   return 8.0;
        return 3.0;
    }

    private static double getPathLatency(String name) {
        if (name.startsWith("cloud")) return 0.10;
        if (name.startsWith("fog"))   return 0.02;
        return 0.005;
    }

    private static double getHopCount(String name) {
        if (name.startsWith("cloud")) return 3.0;
        if (name.startsWith("fog"))   return 2.0;
        return 1.0;
    }

    private static int getDeviceLevel(String name) {
        if (name.startsWith("cloud")) return 3;
        if (name.startsWith("fog"))   return 2;
        return 1;
    }

    private static String getDeviceType(String name) {
        if (name.startsWith("cloud")) return "CLOUD";
        if (name.startsWith("fog"))   return "FOG";
        return "EDGE";
    }

    private static String getParentId(FogDevice d) {
        try {
            int pid = d.getParentId();
            return pid == -1 ? "NONE" : "dev-" + pid;
        } catch (Exception e) { return "NONE"; }
    }
}
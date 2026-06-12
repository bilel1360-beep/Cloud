package org.fog.test.perfeval.datasetgen;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;
import org.fog.entities.FogBroker;
import org.fog.entities.FogDevice;

import java.util.*;
import java.nio.file.*;

/**
 * DatasetSimulation — main entry point.
 *
 * Phase 1: Scheduler replay (unchanged).
 *   50 runs × 100,000 tasks × 27 devices = 2,700,000 scheduler rows.
 *
 * Phase 2: Migration forced-action reruns (new).
 *   For each of the 2,000 migration decision points, the simulation
 *   is re-run 27 times from t=0 with a different forced action each
 *   time. Parallelized across all available CPU cores.
 *   Total: 54,000 reruns → 54,000 migration outcome rows.
 */
public class DatasetSimulation {

    public static void main(String[] args) {
	    	
        try {
            System.out.println("=== Dataset Generation Pipeline v4 ===");
            System.out.println("Threads available: "
                + SimConfig.MIGRATION_PARALLEL_THREADS);
            RuntimeStatisticsLoader.load(
            	    "runtime_metrics.csv");
            CsvLogger.init();

            // ── Phase 1: Scheduler ────────────────────────────
            System.out.println("\n--- Phase 1: Scheduler Replay ---");
            for (int run = 0; run < SimConfig.NUM_RUNS; run++) {
                System.out.println("Run " + (run + 1)
                    + "/" + SimConfig.NUM_RUNS);

                Log.disable();
                CloudSim.init(1, Calendar.getInstance(), false);
                FogBroker broker = new FogBroker("broker");
                List<FogDevice> devices =
                    TopologyBuilder.build(broker.getId());

                long   seed = SimConfig.RANDOM_SEED + run;
                Random rand = new Random(seed);
                List<WorkloadGenerator.Task> tasks =
                    WorkloadGenerator.generate(SimConfig.NUM_TASKS, seed);

                double simTime = 0.0;
                for (WorkloadGenerator.Task task : tasks) {
                    simTime = task.arrivalTime;
                    ReplayScheduler.schedule(
                        task, devices, run, simTime, rand);
                }
                System.out.println("  Tasks: " + tasks.size());
            }

            System.out.println("Flushing scheduler CSVs...");
            CsvLogger.flushScheduler();

            // ── Phase 2: Migration forced-action reruns ───────
            System.out.println("\n--- Phase 2: Migration Replay ---");
            MigrationReplayRunner runner = new MigrationReplayRunner();
            runner.runAll();

            System.out.println("Flushing migration CSVs...");
            CsvLogger.close();

            // ── Post-process: link migration outcomes ─────────
            System.out.println("Linking migration outcomes...");
            linkMigrationOutcomes(
                SimConfig.MIG_OUTCOME_CSV,
                SimConfig.MIG_DECISION_CSV);

            System.out.println("\n=== Complete ===");
            System.out.println("Output: " + SimConfig.OUTPUT_DIR);

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Post-process: fill next_migration_decision_id and done
     * for all 54,000 outcome rows.
     * next/done are the same value for all 27 rows of a decision
     * (they describe the episode transition, not the individual action).
     */
    private static void linkMigrationOutcomes(
            String outcomePath,
            String decisionPath) throws Exception {

        // Step 1: Read decisions, group IDs by episode
        List<String> decLines =
            Files.readAllLines(Paths.get(decisionPath));

        Map<Integer, List<String>> episodeMap = new LinkedHashMap<>();
        for (int i = 1; i < decLines.size(); i++) {
            String[] p    = decLines.get(i).split(",", -1);
            int      epId = Integer.parseInt(p[1].trim());
            String   id   = p[0].trim();
            episodeMap.computeIfAbsent(epId,
                k -> new ArrayList<>()).add(id);
        }

        // Step 2: Build next and done maps keyed by decision_id
        Map<String, String>  nextMap = new HashMap<>();
        Map<String, Integer> doneMap = new HashMap<>();
        for (List<String> ids : episodeMap.values()) {
            for (int i = 0; i < ids.size(); i++) {
                String curr = ids.get(i);
                String next = (i + 1 < ids.size())
                    ? ids.get(i + 1) : "NONE";
                int done    = (i == ids.size() - 1) ? 1 : 0;
                nextMap.put(curr, next);
                doneMap.put(curr, done);
            }
        }

        // Step 3: Rewrite outcome file
        // Columns (0-based): 36=next_migration_decision_id, 37=done
        // (shifted from original 26/27 by the 10 task progress columns)
        List<String> outLines =
            Files.readAllLines(Paths.get(outcomePath));
        List<String> result = new ArrayList<>();
        result.add(outLines.get(0)); // keep header

        for (int i = 1; i < outLines.size(); i++) {
            String[] parts = outLines.get(i).split(",", -1);
            String   id    = parts[0].trim();
            parts[36] = nextMap.getOrDefault(id, "NONE");
            parts[37] = String.valueOf(doneMap.getOrDefault(id, 0));
            result.add(String.join(",", parts));
        }

        Files.write(
            Paths.get(outcomePath),
            String.join("\n", result).getBytes());

        System.out.println("Migration linking complete.");
    }
}
package org.fog.test.perfeval.datasetgen;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

public class RuntimeDatasetCollector {

    private static PrintWriter writer;

    // Store START event information
    private static final Map<Integer, Double> startTimeMap =
            new HashMap<>();

    private static final Map<Integer, Double> startEnergyMap =
            new HashMap<>();

    static {
        try {

            System.out.println("Creating runtime_metrics.csv");

            writer = new PrintWriter(
                    new FileWriter(
                            "runtime_metrics.csv",
                            false));

            writer.println(
                "event,tuple_id,app_id,tuple_type,"
                + "source_module,destination_module,"
                + "device,module_name,"
                + "simulation_time,cpu,energy,"
                + "execution_time,energy_delta,"
                + "queue_size,active_modules,active_tuples");

        } catch (Exception e) {

            System.out.println(
                    "RuntimeDatasetCollector ERROR");

            e.printStackTrace();
        }
    }

    public static synchronized void log(
            String event,
            int tupleId,
            String appId,
            String tupleType,
            String sourceModule,
            String destinationModule,
            String device,
            String moduleName,
            double simulationTime,
            double cpu,
            double energy,
            int queueSize,
            int activeModules,
            int activeTuples) {

        if (writer == null) {
            return;
        }

        double executionTime = 0.0;
        double energyDelta = 0.0;

        if ("START".equals(event)) {

            startTimeMap.put(
                    tupleId,
                    simulationTime);

            startEnergyMap.put(
                    tupleId,
                    energy);

        } else if ("FINISH".equals(event)) {

            Double startTime =
                    startTimeMap.get(tupleId);

            Double startEnergy =
                    startEnergyMap.get(tupleId);

            if (startTime != null) {
                executionTime =
                        simulationTime - startTime;
            }

            if (startEnergy != null) {
                energyDelta =
                        energy - startEnergy;
            }

            /*
             * Record runtime statistics
             * These averages will later be used
             * by ReplayScheduler instead of
             * synthetic estimates.
             */
            System.out.println(
            	    "STATS: "
            	    + tupleType
            	    + " CPU="
            	    + cpu
            	    + " EXEC="
            	    + executionTime
            	    + " ENERGY="
            	    + energyDelta);
            RuntimeStatistics.record(
                    tupleType,
                    executionTime,
                    energyDelta,
                    cpu,
                    queueSize,
                    activeModules,
                    activeTuples);

            startTimeMap.remove(tupleId);
            startEnergyMap.remove(tupleId);
        }

        writer.println(
                event + ","
                + tupleId + ","
                + appId + ","
                + tupleType + ","
                + sourceModule + ","
                + destinationModule + ","
                + device + ","
                + moduleName + ","
                + simulationTime + ","
                + cpu + ","
                + energy + ","
                + executionTime + ","
                + energyDelta + ","
                + queueSize + ","
                + activeModules + ","
                + activeTuples);

        writer.flush();
    }

    public static void close() {

        if (writer != null) {
            writer.flush();
            writer.close();
        }
    }
}
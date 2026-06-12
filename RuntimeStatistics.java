package org.fog.test.perfeval.datasetgen;

import java.util.*;

public class RuntimeStatistics {
	
    private static final Map<String,List<Double>>
            executionTimes = new HashMap<>();

    private static final Map<String,List<Double>>
            energyDeltas = new HashMap<>();

    private static final Map<String,List<Double>>
            cpuValues = new HashMap<>();

    private static final Map<String,List<Double>>
    		queueSizes = new HashMap<>();

    private static final Map<String,List<Double>>
    		activeModules = new HashMap<>();

    private static final Map<String,List<Double>>
    		activeTuples = new HashMap<>();

    public static synchronized void record(
            String tupleType,
            double executionTime,
            double energyDelta,
            double cpu,
            double queueSize,
            double activeModuleCount,
            double activeTupleCount) {

        executionTimes
                .computeIfAbsent(
                        tupleType,
                        k -> new ArrayList<>())
                .add(executionTime);

        energyDeltas
                .computeIfAbsent(
                        tupleType,
                        k -> new ArrayList<>())
                .add(energyDelta);

        cpuValues
                .computeIfAbsent(
                        tupleType,
                        k -> new ArrayList<>())
                .add(cpu);
        queueSizes
		        .computeIfAbsent(
		                tupleType,
		                k -> new ArrayList<>())
		        .add(queueSize);

		activeModules
		        .computeIfAbsent(
		                tupleType,
		                k -> new ArrayList<>())
		        .add(activeModuleCount);

		activeTuples
		        .computeIfAbsent(
		                tupleType,
		                k -> new ArrayList<>())
		        .add(activeTupleCount);
        
        
    }

    public static double getAverageExecutionTime(
            String tupleType) {

        List<Double> vals =
                executionTimes.get(tupleType);

        if(vals == null || vals.isEmpty())
            return 0.0;

        return vals.stream()
                .mapToDouble(Double::doubleValue)
                .average()
                .orElse(0.0);
    }

    public static double getAverageEnergyDelta(
            String tupleType) {

        List<Double> vals =
                energyDeltas.get(tupleType);

        if(vals == null || vals.isEmpty())
            return 0.0;

        return vals.stream()
                .mapToDouble(Double::doubleValue)
                .average()
                .orElse(0.0);
    }

    public static double getAverageCpu(
            String tupleType) {

        List<Double> vals =
                cpuValues.get(tupleType);

        if(vals == null || vals.isEmpty())
            return 0.0;

        return vals.stream()
                .mapToDouble(Double::doubleValue)
                .average()
                .orElse(0.0);
    }
    public static double getAverageQueueSize(
            String tupleType) {

        List<Double> vals =
                queueSizes.get(tupleType);

        if(vals == null || vals.isEmpty())
            return 0.0;

        return vals.stream()
                .mapToDouble(Double::doubleValue)
                .average()
                .orElse(0.0);
    }
    public static double getAverageActiveModules(
            String tupleType) {

        List<Double> vals =
                activeModules.get(tupleType);

        if(vals == null || vals.isEmpty())
            return 0.0;

        return vals.stream()
                .mapToDouble(Double::doubleValue)
                .average()
                .orElse(0.0);
    }
    public static double getAverageActiveTuples(
            String tupleType) {

        List<Double> vals =
                activeTuples.get(tupleType);

        if(vals == null || vals.isEmpty())
            return 0.0;

        return vals.stream()
                .mapToDouble(Double::doubleValue)
                .average()
                .orElse(0.0);
    }
    public static void printStatistics() {

        System.out.println("\n=== RUNTIME STATISTICS ===");

        for(String tupleType : executionTimes.keySet()) {

        	System.out.println(
        		    tupleType
        		    + " AVG_CPU="
        		    + getAverageCpu(tupleType)
        		    + " AVG_EXEC="
        		    + getAverageExecutionTime(tupleType)
        		    + " AVG_ENERGY="
        		    + getAverageEnergyDelta(tupleType)
        		    + " AVG_QUEUE="
        		    + getAverageQueueSize(tupleType)
        		    + " AVG_MODULES="
        		    + getAverageActiveModules(tupleType)
        		    + " AVG_TUPLES="
        		    + getAverageActiveTuples(tupleType));
        }
    }
}
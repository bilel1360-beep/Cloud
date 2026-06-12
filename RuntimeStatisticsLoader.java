package org.fog.test.perfeval.datasetgen;

import java.io.BufferedReader;
import java.io.FileReader;

public class RuntimeStatisticsLoader {

    public static void load(String csvPath) {

        try (BufferedReader br =
                     new BufferedReader(
                             new FileReader(csvPath))) {

            String line;

            // Skip header
            br.readLine();

            while ((line = br.readLine()) != null) {

                String[] parts =
                        line.split(",", -1);

                if (parts.length < 13)
                    continue;

                String event =
                        parts[0].trim();

                // Only FINISH rows contain
                // execution_time and energy_delta
                if (!"FINISH".equals(event))
                    continue;

                String tupleType =
                        parts[3].trim();

                double executionTime =
                        parseDouble(parts[11]);

                double energyDelta =
                        parseDouble(parts[12]);

                double cpu =
                        parseDouble(parts[9]);

                double queueSize =
                        parseDouble(parts[13]);

                double activeModules =
                        parseDouble(parts[14]);

                double activeTuples =
                        parseDouble(parts[15]);
                
                RuntimeStatistics.record(
                        tupleType,
                        executionTime,
                        energyDelta,
                        cpu,
                        queueSize,
                        activeModules,
                        activeTuples);
            }

            System.out.println(
                    "RuntimeStatistics loaded successfully.");
            RuntimeStatistics.printStatistics();

        } catch (Exception e) {

            System.out.println(
                    "RuntimeStatisticsLoader ERROR");

            e.printStackTrace();
        }
    }

    private static double parseDouble(
            String value) {

        try {
            return Double.parseDouble(
                    value.trim());
        } catch (Exception e) {
            return 0.0;
        }
    }
}
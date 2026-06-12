package org.fog.test.perfeval.datasetgen;

import java.util.*;

public class WorkloadGenerator {

    public enum TaskType { CPU_HEAVY, MEMORY_HEAVY, NETWORK_HEAVY, MIXED }
    public enum SlaClass { HIGH, MEDIUM, LOW }

    public static class Task {
        public int    taskId;
        public String appId;
        public String actualTupleId;
        public TaskType type;
        public String runtimeTupleType;
        public SlaClass slaClass;
        public String sourceModule;
        public String destinationModule;
        public String sourceDeviceId;
        public String tupleDirection;
        public long   cpuLength;
        public long   networkLength;
        public long   outputSize;
        public int    numPes;
        public int    ramRequired;
        public double arrivalTime;
        public double expectedRuntime;
        public double deadline;
        public double deadlineSlack;
        public double maxAllowedLatency;
        public int    priorityClass;

        public String getTypeString()   { return type.name(); }
        public String getSlaString()    { return slaClass.name(); }
        public String getDirectionStr() { return tupleDirection; }
    }

    public static List<Task> generate(int numTasks, long seed) {
        List<Task> tasks    = new ArrayList<>();
        Random     rand     = new Random(seed);
        double     time     = 0.0;

        String[] srcModules = {"sensor","client","preprocessor","aggregator"};
        String[] dstModules = {"processor","classifier","analyzer","actuator"};
        String[] directions = {"UP","DOWN","LATERAL"};

        for (int i = 0; i < numTasks; i++) {
            Task t          = new Task();
            t.taskId        = i;
            t.appId         = "app-" + (i % 4);
            t.actualTupleId = "tuple-" + seed + "-" + i;
            t.type          = TaskType.values()[i % 4];
            switch (t.type) {

            case CPU_HEAVY:
                t.runtimeTupleType = "RAW_DATA";
                break;

            case MEMORY_HEAVY:
                t.runtimeTupleType = "PROCESSED_DATA";
                break;

            case NETWORK_HEAVY:
                t.runtimeTupleType = "M-SENSOR";
                break;

            default:
                t.runtimeTupleType = "ACTION_COMMAND";
        }
            t.sourceModule  = srcModules[i % 4];
            t.destinationModule = dstModules[i % 4];
            t.tupleDirection    = directions[i % 3];
            t.numPes            = 1 + rand.nextInt(4);

            time += (-Math.log(1.0 - rand.nextDouble()))
                  / SimConfig.TASK_ARRIVAL_RATE;
            t.arrivalTime = time;

            switch (t.type) {
                case CPU_HEAVY:
                    // Large CPU, small network — benefits from cloud MIPS
                    t.cpuLength     = 5000  + rand.nextInt(15000);
                    t.networkLength = 100   + rand.nextInt(400);
                    t.outputSize    = 50    + rand.nextInt(200);
                    t.ramRequired   = 128   + rand.nextInt(256);
                    t.slaClass      = SlaClass.MEDIUM;
                    t.priorityClass = 2;
                    break;
                case MEMORY_HEAVY:
                    // Moderate CPU, small network, large RAM — fog preferred
                    t.cpuLength     = 200   + rand.nextInt(500);
                    t.networkLength = 50    + rand.nextInt(150);
                    t.outputSize    = 100   + rand.nextInt(400);
                    t.ramRequired   = 512   + rand.nextInt(1024);
                    t.slaClass      = SlaClass.HIGH;
                    t.priorityClass = 1;
                    break;
                case NETWORK_HEAVY:
                    // Small CPU, LARGE network transfer — edge preferred
                    // BUG FIX: was 50-200 (same as others). Now 5000-15000
                    // to genuinely differentiate this task type.
                    t.cpuLength     = 300   + rand.nextInt(700);
                    t.networkLength = 5000  + rand.nextInt(10000);
                    t.outputSize    = 2000  + rand.nextInt(3000);
                    t.ramRequired   = 64    + rand.nextInt(128);
                    t.slaClass      = SlaClass.LOW;
                    t.priorityClass = 3;
                    break;
                default: // MIXED
                    t.cpuLength     = 2000  + rand.nextInt(5000);
                    t.networkLength = 300   + rand.nextInt(1000);
                    t.outputSize    = 100   + rand.nextInt(400);
                    t.ramRequired   = 256   + rand.nextInt(512);
                    t.slaClass      = SlaClass.MEDIUM;
                    t.priorityClass = 2;
                    break;
            }

            // Expected runtime calibrated to edge-device speed (worst case).
            // REFERENCE_MIPS=1000 (edge), REFERENCE_BANDWIDTH=10 (edge uplink MB/s).
            // This means a cloud device (44800 MIPS) will complete tasks ~45x faster
            // than this estimate — creating a realistic spread of deadline outcomes
            // across device tiers. Without this, deadlines are so loose that every
            // device always meets them and the SLA penalty in the cost function is
            // always zero.
            t.expectedRuntime =
                (double) t.cpuLength     / SimConfig.REFERENCE_MIPS
              + (double) t.networkLength  / SimConfig.REFERENCE_BANDWIDTH
              + 0.05; // 50ms base path latency buffer

            // Deadline by SLA class
            double slack;
            switch (t.slaClass) {
                case HIGH:   slack = SimConfig.SLACK_HIGH;   break;
                case LOW:    slack = SimConfig.SLACK_LOW;    break;
                default:     slack = SimConfig.SLACK_MEDIUM; break;
            }
            t.deadlineSlack     = slack;
            t.deadline          = t.arrivalTime + t.expectedRuntime * slack;
            t.maxAllowedLatency = t.expectedRuntime * slack * 0.8;
            t.sourceDeviceId    = "edge-" + (i % SimConfig.NUM_EDGE_DEVICES);

            tasks.add(t);
        }
        return tasks;
    }
}
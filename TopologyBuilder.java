package org.fog.test.perfeval.datasetgen;

import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.power.PowerHost;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.sdn.overbooking.VmSchedulerTimeSharedOverbookingEnergy;
import org.fog.entities.FogDevice;
import org.fog.entities.FogDeviceCharacteristics;
import org.fog.utils.FogLinearPowerModel;
import org.fog.utils.FogUtils;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class TopologyBuilder {

    public static List<FogDevice> build(int brokerId) throws Exception {
        List<FogDevice> devices = new ArrayList<>();

        // Cloud nodes
        for (int i = 0; i < SimConfig.NUM_CLOUD_NODES; i++) {
            FogDevice cloud = createDevice(
                "cloud-" + i, brokerId,
                44800,  // MIPS (very powerful)
                40960,  // RAM MB
                100,    // uplink bandwidth
                10000,  // downlink bandwidth
                0.0,    // scheduling delay
                0.01,   // cost per MIPS
                16.0,   // max power
                12.0    // idle power
            );
            cloud.setParentId(-1);
            devices.add(cloud);
        }

        // Fog nodes — each connected to cloud-0
        int cloudParentId = devices.get(0).getId();
        for (int i = 0; i < SimConfig.NUM_FOG_NODES; i++) {
            FogDevice fog = createDevice(
                "fog-" + i, brokerId,
                8000,   // MIPS (medium)
                8192,   // RAM MB
                1000,     // uplink
                1000,   // downlink
                1.0,    // scheduling delay
                0.05,   // cost per MIPS
                8.0,    // max power
                4.0     // idle power
            );
            fog.setParentId(cloudParentId);
            fog.setUplinkLatency(10); // 10ms to cloud
            devices.add(fog);
        }

        // Edge devices — spread across fog nodes
        int fogStartIndex = SimConfig.NUM_CLOUD_NODES;
        for (int i = 0; i < SimConfig.NUM_EDGE_DEVICES; i++) {
            int parentFogIndex = fogStartIndex + (i % SimConfig.NUM_FOG_NODES);
            FogDevice edge = createDevice(
                "edge-" + i, brokerId,
                1000,   // MIPS (low power)
                1024,   // RAM MB
                100,      // uplink
                100,    // downlink
                2.0,    // scheduling delay
                0.1,    // cost per MIPS
                3.0,    // max power
                1.0     // idle power
            );
            edge.setParentId(devices.get(parentFogIndex).getId());
            edge.setUplinkLatency(2); // 2ms to fog
            devices.add(edge);
        }

        return devices;
    }

    private static FogDevice createDevice(
            String name, int brokerId,
            long mips, int ram,
            long upBw, long downBw,
            double schedulingInterval,
            double costPerMips,
            double maxPower, double idlePower) throws Exception {

        List<Pe> peList = new ArrayList<>();
        peList.add(new Pe(0, new PeProvisionerSimple(mips)));

        PowerHost host = new PowerHost(
            FogUtils.generateEntityId(),
            new RamProvisionerSimple(ram),
            new BwProvisionerSimple(upBw + downBw),
            1000000,
            peList,
            new VmSchedulerTimeSharedOverbookingEnergy(peList),
            new FogLinearPowerModel(maxPower, idlePower)
        );

        List<Host> hostList = new ArrayList<>();
        hostList.add(host);

        String arch = "x86";
        String os = "Linux";
        String vmm = "Xen";
        double timeZone = 10.0;
        double cost = costPerMips;
        double costPerMem = 0.05;
        double costPerStorage = 0.001;
        double costPerBw = 0.0;

        FogDeviceCharacteristics characteristics = new FogDeviceCharacteristics(
            arch, os, vmm, host, timeZone,
            cost, costPerMem, costPerStorage, costPerBw
        );

        return new FogDevice(
            name, characteristics,
            new org.cloudbus.cloudsim.VmAllocationPolicySimple(hostList),
            new LinkedList<Storage>(),
            schedulingInterval, upBw, downBw, 0, 0.0
        );
    }
}
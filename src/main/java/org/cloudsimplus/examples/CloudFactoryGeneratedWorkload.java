/*
 * CloudSim Plus: A modern, highly-extensible and easier-to-use Framework for
 * Modeling and Simulation of Cloud Computing Infrastructures and Services.
 * http://cloudsimplus.org
 *
 *     Copyright (C) 2015-2021 Universidade da Beira Interior (UBI, Portugal) and
 *     the Instituto Federal de Educação Ciência e Tecnologia do Tocantins (IFTO, Brazil).
 *
 *     This file is part of CloudSim Plus.
 *
 *     CloudSim Plus is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     CloudSim Plus is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with CloudSim Plus. If not, see <http://www.gnu.org/licenses/>.
 */
package org.cloudsimplus.examples;

import org.cloudsimplus.allocationpolicies.VmAllocationPolicy;
import org.cloudsimplus.allocationpolicies.VmAllocationPolicyvCluster;
import org.cloudsimplus.brokers.DatacenterBroker;
import org.cloudsimplus.brokers.DatacenterBrokerSimple;
import org.cloudsimplus.builders.tables.CloudletsTableBuilder;
import org.cloudsimplus.builders.tables.TextTableColumn;
import org.cloudsimplus.cloudlets.Cloudlet;
import org.cloudsimplus.cloudlets.CloudletSimple;
import org.cloudsimplus.core.CloudSimPlus;
import org.cloudsimplus.datacenters.Datacenter;
import org.cloudsimplus.datacenters.DatacenterSimple;
import org.cloudsimplus.hosts.Host;
import org.cloudsimplus.hosts.HostMultiClusters;
import org.cloudsimplus.hosts.HostStateHistoryEntry;
import org.cloudsimplus.resources.Pe;
import org.cloudsimplus.resources.PeSimple;
import org.cloudsimplus.utilizationmodels.UtilizationModel;
import org.cloudsimplus.utilizationmodels.UtilizationModelDynamic;
import org.cloudsimplus.utilizationmodels.UtilizationModelFull;
import org.cloudsimplus.schedulers.cloudlet.CloudletSchedulerTimeShared;
import org.cloudsimplus.schedulers.vm.VmSchedulerTimeShared;
import org.cloudsimplus.vms.Vm;
import org.cloudsimplus.vms.VmOversubscribable;
import org.cloudsimplus.listeners.EventInfo;
import org.cloudsimplus.listeners.EventListener;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.SortedMap;
import java.util.function.Function;
import static java.util.Map.entry;
import java.util.OptionalDouble;
import java.util.stream.DoubleStream;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * An example showing how to submit VMs to the broker with different delays.
 * This way, Cloudlets bound to such delayed VMs just start
 * executing after the respective VM is placed into some Host.
 * Since submitted cloudlets are not explicitly delayed, you can see in results
 * that, at the end, they are delayed due to VM delay.
 *
 * <p>Finally, considering there aren't enough hosts for all VMs, this example shows
 * how to make the broker to destroy idle VMs after a while to open room for new VMs.
 * Check {@link #createBroker()} for details.
 * </p>
 *
 * @author Fabian Mastenbroek
 * @author Manoel Campos da Silva Filho
 * @since CloudSim Plus 7.2.1
 */
public class CloudFactoryGeneratedWorkload {
    private static int HOSTS = 1;

    /**
     * Number of Processor Elements (CPU Cores) of each Host.
     */
    private static int HOST_PES_NUMBER = 64; // 256 cores

    /**
     * Oversubscription levels
     */
    private static Float filtered_oversubscription  = null;

    /**
     * Memory in Megabytes on each host
     */
    private static int HOST_MEMORY = 256*1024; // Go

    /**
     * Storage in Megabytes on each host
     */
    private static final int HOST_STORAGE = 10*1024*1024; // 10 TB
    /**
     * Bandwidth in Megabits/s on each host
     */
    private static final int HOST_BW = 100*1024; // 10 Gb/s

    private static final int SCHEDULING_INTERVAL = 1;

    private List<Host> hostList;
    private List<Vm> vmList;
    private List<Cloudlet> cloudletList;
    private DatacenterBroker broker;
    private Datacenter datacenter;
    private CloudSimPlus simulation;

    private static Map<String, SortedMap<Double, Double>> usageModels;
	private static List<Map<String, String>> vmTemplateList;

    private static double currentTime;

    private static String modelVmFile = "vms.properties";
    private static String modelUsageFile = "models.properties";
    private static Boolean firstFit = false;

    /**
     * Starts the example execution, calling the class constructor\
     * to build and run the simulation.
     *
     * @param args command line parameters
     */
    public static void main(String[] args) {
        if (args.length >= 3) {
            try {
                HOSTS = Integer.parseInt(args[0]);
                HOST_PES_NUMBER = Integer.parseInt(args[1]);
                HOST_MEMORY = Integer.parseInt(args[2])*1024;
                if (args.length >= 4 && !args[3].equals("no"))
                    filtered_oversubscription = Float.parseFloat(args[3]);
                if (args.length >= 5)
                    modelVmFile =  args[4];
                if (args.length >= 6)
                    modelUsageFile = args[5];
                if (args.length >= 7)
                    firstFit = Boolean.parseBoolean(args[6]);
            } catch (NumberFormatException e) {
                System.err.println("Usage : host_number cpu_number mem_gb [filtered_oversubscription] [vmfile] [modelfile] [firstFit True/false]");
                System.exit(1);
            }
        }
        System.out.println("Chosen environmnent: " + HOSTS + " host(s) with " + HOST_PES_NUMBER + " vCPU and " + HOST_MEMORY  + " dram (mb)");
        new CloudFactoryGeneratedWorkload();
    }

    /**
     * Default constructor that builds and starts the simulation.
     */
    private CloudFactoryGeneratedWorkload() {
        /*Enables just some level of log messages.
          Make sure to import org.cloudsimplus.util.Log;*/
        //Log.setLevel(ch.qos.logback.classic.Level.WARN);

        System.out.println("Starting CloudFactory generated scenario ");

        simulation = new CloudSimPlus();

        currentTime = 0;
        simulation.addOnClockTickListener(this::onClockTickListener);

		vmTemplateList = loadCloudFactoryVMs(modelVmFile);
		usageModels = loadCloudFactoryModels(modelUsageFile);
        
        this.hostList = new ArrayList<>();
        this.vmList = new ArrayList<>();
        this.cloudletList = new ArrayList<>();
        this.datacenter = createDatacenter();
        this.broker = createBroker();

        createAndSubmitCloudFactoryVmsAndCloudlets();

        try{
            simulation.start();
        }
        catch(IllegalStateException e){ // our shortcut
            System.out.println("Simulation aborded! No suitable host found"); 
        }
        
        
        printResults();
        System.out.println(getClass().getSimpleName() + " finished!");
    }

    /**
     * Creates a broker that destroys idle VMs after a while.
     * @return
     */
    private DatacenterBroker createBroker() {
        return new DatacenterBrokerSimple(simulation).setVmDestructionDelay(10.0);
    }

    /**
     * Creates a Datacenter with pre-defined configuration.
     *
     * @return the created Datacenter
     */
    private Datacenter createDatacenter() {
        for (int i = 0; i < HOSTS; i++) {
            hostList.add(createHost());
        }
        VmAllocationPolicy allocationPolicy;
        final var dc = new DatacenterSimple(simulation, hostList, new VmAllocationPolicyvCluster(firstFit=firstFit));
        dc.setSchedulingInterval(SCHEDULING_INTERVAL);
        return dc;
    }

    /**
     * Creates a host with pre-defined configuration.
     *
     * @return the created host
     */
    private Host createHost() {
        final var peList = new ArrayList<Pe>();
        final long mips = 1000;
        for(int i = 0; i < HOST_PES_NUMBER; i++){
            peList.add(new PeSimple(mips));
        }
        final var host = new HostMultiClusters(HOST_MEMORY, HOST_BW, HOST_STORAGE, peList);
        host.setStateHistoryEnabled(true);
        host.enableUtilizationStats();
        return host;
    }

    private void printResults() {
        System.out.printf("%nCloudlet results%n");
        new CloudletsTableBuilder(broker.getCloudletFinishedList())
            .addColumn(new TextTableColumn("Host MIPS", "total"), c -> c.getVm().getHost().getTotalMipsCapacity(), 5)
            .addColumn(new TextTableColumn("VM MIPS", "total"), c -> c.getVm().getTotalMipsCapacity(), 8)
            .addColumn(new TextTableColumn("  VM MIPS", "requested"), this::getVmRequestedMips, 9)
            .addColumn(new TextTableColumn("  VM MIPS", "allocated"), this::getVmAllocatedMips, 10)
            .addColumn(new TextTableColumn("  VM oc", "oversub"), c -> ((VmOversubscribable)c.getVm()).getOversubscriptionLevel(), 5)
            .build();
        System.out.printf("%nHost results results%n");
        hostList.forEach(this::printHostStateHistory);
        System.out.printf("%nVMs CPU utilization mean%n");
        for (final var vm : vmList) {
            final double vmCpuUsageMean = vm.getCpuUtilizationStats().getMean()*100;
            System.out.printf("\tVM %d CPU Utilization mean: %6.2f%%%n", vm.getId(), vmCpuUsageMean);
        }
    }

    /**
     * Prints the {@link Host#getStateHistory() state history} for a given Host.
     * Realize that the state history is just collected if that is enabled before
     * starting the simulation by calling {@link Host#setStateHistoryEnabled(boolean)}.
     *
     * @param host
     */
    private void printHostStateHistory(Host host) {
        System.out.println("Host n°" + host.getId());
        List<Double> usage = new ArrayList<Double>();
        for(HostStateHistoryEntry history : host.getStateHistory()){
            if(history.active())
                usage.add(history.percentUsage()*100);
        }
        if(usage.size()>0){
            final OptionalDouble min = usage.stream().mapToDouble(v -> v).min();
            final OptionalDouble max = usage.stream().mapToDouble(v -> v).max();
            final OptionalDouble avg = usage.stream().mapToDouble(v -> v).average();
            System.out.println("Minimum: " + min.orElse(0.0) + " | Average: " + avg.orElse(0.0) + " | Maximum: " + max.orElse(0.0));
        }
        else
            System.out.println("Inactive host");
    }

    private double getVmRequestedMips(Cloudlet c) {
        if(c.getVm().getStateHistory().isEmpty()){
            return 0;
        }
        return c.getVm().getStateHistory().get(c.getVm().getStateHistory().size()-1).getRequestedMips();
    }

    private double getVmAllocatedMips(Cloudlet c) {
        if(c.getVm().getStateHistory().isEmpty()){
            return 0;
        }
        return c.getVm().getStateHistory().get(c.getVm().getStateHistory().size()-1).getAllocatedMips();
    }

    /**
     * Keeps track of simulation clock.
     * Every time the clock changes, this method is called.
     * To enable this method to be called at a defined
     * interval, you need to set the {@link Datacenter#setSchedulingInterval(double) scheduling interval}.
     *
     * @param evt information about the clock tick event
     * @see #SCHEDULING_INTERVAL
     */
    private void onClockTickListener(final EventInfo evt) {
        currentTime = evt.getTime();
    }


    private UtilizationModel setCustomModel(String modelKey){
        final var model = new UtilizationModelDynamic();
        model.setUtilizationUpdateFunction(input -> readUsageBasedOnTime(input, usageModels.get(modelKey)));
        return model;
    }

    /**
     * Read a model usage from a Map format based on current time
     * @return the updated utilization
     * @see #createCloudlet()
     */
    private static double readUsageBasedOnTime(UtilizationModelDynamic utilizationModel, SortedMap<Double, Double> model) {
        double targetKey = currentTime;
        double modelValue = 0;
        for(Double modelKey: model.keySet()) { // Key order is guaranteed with the SortedMap
            modelValue = model.get(modelKey);
            if(targetKey < modelKey){ // we stop at first matching key
                break;
            }
        }
        return modelValue;
    }

    private List<Map<String, String>> loadCloudFactoryVMs(String file){
        List<Map<String, String>> allVMs = new ArrayList<Map<String, String>>();
        try (
            InputStream input = new FileInputStream(file)) {
            Properties vms = new Properties();
            vms.load(input);

            for(Object vm : vms.keySet()){
                allVMs.add(decodeVMLine(vms.getProperty(vm.toString())));
            }
        }catch (IOException e) {e.printStackTrace();}
        return allVMs;
    }

    private Map<String, SortedMap<Double, Double>> loadCloudFactoryModels(String file){
        Map<String, SortedMap<Double, Double>> allModels = new HashMap<String, SortedMap<Double, Double>>();
        try (
            InputStream input = new FileInputStream(file)) {
            Properties models = new Properties();
            models.load(input);

            for(Object model : models.keySet()){
                //System.out.println(model.toString() + ":" + models.getProperty(model.toString()));
                allModels.put(model.toString(), decodeModelLine(models.getProperty(model.toString())));
            }
        }catch (IOException e) {e.printStackTrace();}
        return allModels;
    }

    private SortedMap<Double, Double> decodeModelLine(String line){
        SortedMap<Double, Double> map = new TreeMap<Double, Double>();
        for(String keyVal : line.split(",")){
            String key = keyVal.substring(0,keyVal.indexOf(':'));
            String val = keyVal.substring(keyVal.indexOf(':')+1);
            map.put(Double.parseDouble(key), Double.parseDouble(val)/100);
        }
        return map;
    }

    private Map<String, String> decodeVMLine(String line){
        Map<String, String> map = new HashMap<String, String>();
        for(String keyVal : line.split(",")){
            String key = keyVal.substring(0,keyVal.indexOf(':'));
            String val = keyVal.substring(keyVal.indexOf(':')+1);
            map.put(key, val);
        }
        return map;
    }

    /**
     * Create VM generated from CloudFactory
     */
    private void createAndSubmitCloudFactoryVmsAndCloudlets() {

		for (Map<String, String> template : this.vmTemplateList){

            Float oversubscriptionLevel = new Float(Float.parseFloat(template.get("vmoc")));
            if(filtered_oversubscription != null && !oversubscriptionLevel.equals(filtered_oversubscription)){
                continue;
            }

			VmOversubscribable vm = new VmOversubscribable(Integer.parseInt(template.get("vmid")), Integer.parseInt(template.get("vmmips")), Integer.parseInt(template.get("vmcpu")), oversubscriptionLevel);
            int ramVal = Integer.parseInt(template.get("vmram"));
            if(oversubscriptionLevel > 1 && ramVal>=8129){
                ramVal=8192;
            }
			vm.setRam(ramVal);
            vm.setBw(Integer.parseInt(template.get("vmbw"))).setSize(Integer.parseInt(template.get("vmsize"))).setCloudletScheduler(new CloudletSchedulerTimeShared());
			vm.setSubmissionDelay(Integer.parseInt(template.get("vmsubmission")));
            vm.setShutDownDelay(5.0);
            vm.setLifeTime(Integer.parseInt(template.get("cloudletlifetime")));
			vm.enableUtilizationStats();

			Cloudlet cloudlet = new CloudletSimple(Integer.parseInt(template.get("cloudletid")), Integer.parseInt(template.get("cloudletmips")), Integer.parseInt(template.get("cloudletcpu")));
			cloudlet.setFileSize(Integer.parseInt(template.get("cloudletfilesize"))).setOutputSize(Integer.parseInt(template.get("cloudletoutputsize")));
			cloudlet.setUtilizationModelCpu(setCustomModel(template.get("cloudletmodel")));
			cloudlet.setVm(vm);
			cloudlet.setLifeTime(Integer.parseInt(template.get("cloudletlifetime")));

			vmList.add(vm);
			cloudletList.add(cloudlet);
		}

        this.vmList = vmList;
        this.cloudletList = cloudletList;

        broker.submitVmList(this.vmList);
        broker.submitCloudletList(this.cloudletList);

    }
}

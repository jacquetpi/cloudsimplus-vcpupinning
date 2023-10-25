/*
 * Title: CloudSim Toolkit Description: CloudSim (Cloud Simulation) Toolkit for Modeling and
 * Simulation of Clouds Licence: GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * Copyright (c) 2009-2012, The University of Melbourne, Australia
 */
package org.cloudsimplus.schedulers.vm;

import lombok.Getter;
import lombok.NonNull;

import org.cloudsimplus.resources.Pe;
import org.cloudsimplus.schedulers.MipsShare;
import org.cloudsimplus.vms.*;
import org.cloudsimplus.vms.VmOversubscribable;
import org.cloudsimplus.allocationpolicies.VmAllocationPolicyFirstFit;

import java.util.Iterator;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.util.stream.Collectors;

/**
 * VmSchedulerTimeShared is a Virtual Machine Monitor (VMM), also called Hypervisor,
 * that defines a policy to allocate one or more PEs from a PM to a VM, and allows sharing of PEs
 * by multiple VMs. <b>This class also implements 10% performance degradation due
 * to VM migration. It does not support over-subscription.</b>
 *
 * <p>Each host has to use is own instance of a VmScheduler that will so
 * schedule the allocation of host's PEs for VMs running on it.</p>
 *
 * <p>
 * It does not perform a preemption process in order to move running
 * VMs to the waiting list in order to make room for other already waiting
 * VMs to run. It just imposes there is not waiting VMs,
 * <b>oversimplifying</b> the scheduling, considering that for a given simulation
 * second <i>t</i>, the total processing capacity of the processor cores (in
 * MIPS) is equally divided by the VMs that are using them.
 * </p>
 *
 * <p>In processors enabled with <a href="https://en.wikipedia.org/wiki/Hyper-threading">Hyper-threading technology (HT)</a>,
 * it is possible to run up to 2 processes at the same physical CPU core.
 * However, this scheduler implementation
 * oversimplifies a possible HT feature by allowing several VMs to use a fraction of the MIPS capacity from
 * physical PEs, until that the total capacity of the virtual PE is allocated.
 * Consider that a virtual PE is requiring 1000 MIPS but there is no physical PE
 * with such a capacity. The scheduler will allocate these 1000 MIPS across several physical PEs,
 * for instance, by allocating 500 MIPS from PE 0, 300 from PE 1 and 200 from PE 2, totaling the 1000 MIPS required
 * by the virtual PE.
 * </p>
 *
 * <p>In a real hypervisor in a Host that has Hyper-threading CPU cores, two virtual PEs can be
 * allocated to the same physical PE, but a single virtual PE must be allocated to just one physical PE.</p>
 *
 * @author Rodrigo N. Calheiros
 * @author Anton Beloglazov
 * @author Manoel Campos da Silva Filho
 * @since CloudSim Toolkit 1.0
 */
public class VmSchedulerMultiLevelOversubscription extends VmSchedulerAbstract {

    protected Map<Float, List<VmOversubscribable>> consumerPerOversubscription;
    protected Map<Float, Long> resourceCountPerOversubscription;
    protected List<Pe> peList;
    protected Integer criticalSize;
    protected Float[] oversubscriptionTemplate;

    /**
     * Creates a time-shared VM scheduler.
     *
     */
    public VmSchedulerMultiLevelOversubscription(final List<Pe> peList) {
        this(peList, DEF_VM_MIGRATION_CPU_OVERHEAD);
    }

    /**
     * Creates a time-shared VM scheduler, defining a CPU overhead for VM migration.
     *
     * @param vmMigrationCpuOverhead the percentage of Host's CPU usage increase when a
     * VM is migrating in or out of the Host. The value is in scale from 0 to 1 (where 1 is 100%).
     */
    public VmSchedulerMultiLevelOversubscription(final List<Pe> peList, final double vmMigrationCpuOverhead){
        super(vmMigrationCpuOverhead);
        this.peList = peList;
        this.consumerPerOversubscription = new HashMap<Float, List<VmOversubscribable>>();
        this.resourceCountPerOversubscription = new HashMap<Float, Long>();
        this.oversubscriptionTemplate = new Float[]{1.0f,2.0f,3.0f,4.0f,5.0f,6.0f,7.0f,8.0f,9.0f,10.0f,11.0f,12.0f,13.0f,14.0f,15.0f,16.0f};
        this.criticalSize = 2;
    }

    public void allocateOversubscriptionStack(final Vm vm){
        for(int vcpu = 0; vcpu < vm.getPesNumber(); vcpu++){
            float vcpuOversubscription = this.oversubscriptionTemplate[vcpu];
            createOversubscriptionLevelIfNotExist(vcpuOversubscription);
            consumerPerOversubscription.get(vcpuOversubscription).add((VmOversubscribable)vm);
            resourceCountPerOversubscription.put(vcpuOversubscription, 1 + resourceCountPerOversubscription.get(vcpuOversubscription));
        }
    }

    public void createOversubscriptionLevelIfNotExist(Float oversubscription){
        if (!this.consumerPerOversubscription.containsKey(oversubscription)) {
            this.consumerPerOversubscription.put(oversubscription, new ArrayList<VmOversubscribable>());
            this.resourceCountPerOversubscription.put(oversubscription, new Long(0));
        }
    }

    @Override
    public boolean allocatePesForVmInternal(final Vm vm, final MipsShare requestedMips) {
        boolean success = allocateMipsShareForVmInternal(vm, requestedMips);
        if(success){
            allocateOversubscriptionStack(vm);
            debug(null);
        }
        return success;
    }

    /**
     * Try to allocate the MIPS requested by a VM
     * and update the allocated MIPS share.
     *
     * @param vm the VM
     * @param requestedMips the list of mips share requested by the vm
     * @return true if successful, false otherwise
     */
    private boolean allocateMipsShareForVmInternal(final Vm vm, final MipsShare requestedMips) {
        if (!isSuitableForVm(vm, requestedMips)) {
            return false;
        }

        allocateMipsShareForVm(vm, requestedMips);
        return true;
    }

    /**
     * Performs the allocation of a MIPS List to a given VM.
     * The actual MIPS to be allocated to the VM may be reduced
     * if the VM is in migration, due to migration overhead.
     *
     * @param vm the VM to allocate MIPS to
     * @param requestedMipsReduced the list of MIPS to allocate to the VM,
     * after it being adjusted by the {@link VmSchedulerAbstract#getMipsShareRequestedReduced(Vm, MipsShare)} method.
     * @see VmSchedulerAbstract#getMipsShareRequestedReduced(Vm, MipsShare)
     */
    protected void allocateMipsShareForVm(final Vm vm, final MipsShare requestedMipsReduced) {
        final var mipsShare = getMipsShareToAllocate(vm, requestedMipsReduced);
        ((VmOversubscribable)vm).setAllocatedMips(mipsShare);
    }

    private long getAvailableMipsFromHostPe(final Pe hostPe) {
        return hostPe.getPeProvisioner().getAvailableResource();
    }

    /**
     * The non-emptiness of the list is ensured by the {@link VmScheduler#isSuitableForVm(Vm, MipsShare)} method.
     */
    @Override
    protected boolean isSuitableForVmInternal(final Vm vm, final MipsShare requestedMips) {
        VmOversubscribable vmOversubscribable = (VmOversubscribable) vm;
        final double totalRequestedMips = requestedMips.totalMips();
        // Consult Oversubscription policy
        long possibleNewHostAllocation = getUsedResources(vmOversubscribable);
        //System.out.println("> #Debug id" + vmOversubscribable.getId() + " : " + vmOversubscribable.getOversubscriptionLevel() + " " + requestedMips.pes() + "vcpu" + " " + requestedMips.totalMips() + "mips");
        //System.out.println("> #Debug AvailableMips " + getTotalAvailableMips() + " requested mips" + totalRequestedMips);

        return possibleNewHostAllocation <= getHost().getWorkingPesNumber();
        //return getHost().getWorkingPesNumber() >= requestedMips.pes() && getTotalAvailableMips() >= totalRequestedMips;
    }

    /**
     * Gets the actual MIPS share that will be allocated to VM's PEs,
     * considering the VM migration status.<
     * If the VM is in migration, this will cause overhead, reducing
     * the amount of MIPS allocated to the VM.
     *
     * @param vm the VM requesting allocation of MIPS
     * @param requestedMips the list of MIPS requested for each vPE
     * @return the allocated MIPS share to the VM
     */
    protected MipsShare getMipsShareToAllocate(final Vm vm, final MipsShare requestedMips) {
        return getMipsShareToAllocate(requestedMips, mipsPercentToRequest(vm));
    }

    /**
     * Gets the actual MIPS share that will be allocated to VM's PEs,
     * considering the VM migration status.
     * If the VM is in migration, this will cause overhead, reducing
     * the amount of MIPS allocated to the VM.
     *
     * @param requestedMips the list of MIPS requested for each vPE
     * @param scalingFactor the factor that will be used to reduce the amount of MIPS
     * allocated to each vPE (which is a percentage value between [0 .. 1]) in case the VM is in migration
     * @return the MIPS share allocated to the VM
     */
    protected MipsShare getMipsShareToAllocate(final MipsShare requestedMips, final double scalingFactor) {
        if(scalingFactor == 1){
            return requestedMips;
        }

        return new MipsShare(requestedMips.pes(), requestedMips.mips()*scalingFactor);
    }


    // TODOOOOO
    @Override
    protected long deallocatePesFromVmInternal(final Vm vm, final int pesToRemove) {
        VmOversubscribable vmOversubscribable = (VmOversubscribable) vm;

        long beforePes = getUsedResources();
        for(int vcpu = 0; vcpu < vm.getPesNumber(); vcpu++){
            Float oversubscriptionLevel = this.oversubscriptionTemplate[vcpu];
            consumerPerOversubscription.get(oversubscriptionLevel).removeAll(Collections.singleton(vmOversubscribable));
            resourceCountPerOversubscription.put(oversubscriptionLevel, resourceCountPerOversubscription.get(oversubscriptionLevel) - 1);
        }        
        long afterPes = getUsedResources();

        long removedPes = afterPes - beforePes;
        if(removedPes < 0)
            removedPes = 0;
        //System.out.println("> #VM leaving id" + vmOversubscribable.getId() + " : " + vmOversubscribable.getOversubscriptionLevel() + " " + removedPes + "pes to remove");
        //System.out.println("> #VM host " + getHost().getWorkingPesNumber() + "/" + getHost().getPesNumber());
        return removedPes;
    }

    public long debug(VmOversubscribable additionalVm){
        Long hostPesAllocation = new Long(0);
        for (Float oversubscriptionLevel : consumerPerOversubscription.keySet()) {
            Integer currentSize = consumerPerOversubscription.get(oversubscriptionLevel).size();
            Long hostPesAllocationForOversubscriptionLevel = resourceCountPerOversubscription.get(oversubscriptionLevel);
            Long overallvCPU = resourceCountPerOversubscription.get(oversubscriptionLevel);
            // if((additionalVm != null) && additionalVm.getOversubscriptionLevel().equals(oversubscriptionLevel)){
            //     currentSize+=1;
            //     hostPesAllocationForOversubscriptionLevel+=additionalVm.getPesNumber();
            // }
            // if(currentSize>= this.criticalSize){
            //     hostPesAllocationForOversubscriptionLevel = (long) Math.ceil(hostPesAllocationForOversubscriptionLevel/oversubscriptionLevel);
            // }
            hostPesAllocationForOversubscriptionLevel = (long) Math.ceil(hostPesAllocationForOversubscriptionLevel/oversubscriptionLevel);
            hostPesAllocation += hostPesAllocationForOversubscriptionLevel;
            System.out.println(">>Alloc on " + getHost().getId() + " oc:" +  oversubscriptionLevel + " " + overallvCPU + "/" + getHypothethicalPhysicalAllocationOf(Arrays.asList(oversubscriptionLevel)) + "/" + hostPesAllocationForOversubscriptionLevel + " vm count:" + currentSize);
        }
        System.out.println(">>Alloc on " + getHost().getId() + " overall alloc " + hostPesAllocation + "/" + getHost().getWorkingPesNumber());
        return hostPesAllocation;
    }

    /* getAvailabilityFor(oversubscriptionLevel)
    *  Availability is defined as the number of resources in vcluster available, without having to extend it
    */
    public long getAvailabilityFor(Float oversubscription){
        long allocation = getHypothethicalPhysicalAllocationOf(Arrays.asList(oversubscription));
        long minimalThreshold = (long) Math.ceil(resourceCountPerOversubscription.get(oversubscription)/oversubscription);
        long availability = allocation - minimalThreshold;
        if(availability>=0)
            return availability;
        return 0;
    }

    /* getSizeFor(oversubscriptionLevel)
    *  Allocation size of oversubscriptionLevel
    */
    public long getSizeFor(Float oversubscription){
        return resourceCountPerOversubscription.get(oversubscription); 
    }

    public long getUsedResources(){
        return getUsedResources(null);
    }

    public long getUsedResources(VmOversubscribable additionalVm){      
        if(additionalVm != null){
            for(int vcpu = 0; vcpu < additionalVm.getPesNumber(); vcpu++){
                createOversubscriptionLevelIfNotExist(this.oversubscriptionTemplate[vcpu]);
            }
        }

        List<Float> nonOversubscribedLevels = consumerPerOversubscription.keySet().stream().filter(level -> level <= 1.0f).collect(Collectors.toList());
        List<Float> oversubscribedLevels    = consumerPerOversubscription.keySet().stream().filter(level -> level > 1.0f).collect(Collectors.toList());

        long resourcesNonOversubscribed        = getHypothethicalPhysicalAllocationOf(nonOversubscribedLevels, additionalVm);
        long resourcedOversubscribedMutualised = getHypothethicalPhysicalAllocationOf(oversubscribedLevels, additionalVm);
        long resourcedOversubscribedDedicated  = 0;
        for (Float oversubscribedLevel : oversubscribedLevels)
            resourcedOversubscribedDedicated += getHypothethicalPhysicalAllocationOf(Arrays.asList(oversubscribedLevel), additionalVm);
        
        return resourcesNonOversubscribed + Math.min(resourcedOversubscribedDedicated, resourcedOversubscribedMutualised);
    }

    private long getHypothethicalPhysicalAllocationOf(List<Float> oversubscriptionLevels){
        return getHypothethicalPhysicalAllocationOf(oversubscriptionLevels, null);
    }

    private long getHypothethicalPhysicalAllocationOf(List<Float> oversubscriptionLevels, VmOversubscribable additionalVm){
        if(oversubscriptionLevels.isEmpty())
            return 0L;
        
        float minimalOversubscription = oversubscriptionLevels.get(0);
        long sumAllocation = 0;

        List<VmOversubscribable> uniqueConsumerCount = new ArrayList<VmOversubscribable>();

        for (Float oversubscriptionLevel : oversubscriptionLevels){

            sumAllocation += resourceCountPerOversubscription.get(oversubscriptionLevel);
            
            for(VmOversubscribable vm : consumerPerOversubscription.get(oversubscriptionLevel)){
                if(!uniqueConsumerCount.contains(vm))
                    uniqueConsumerCount.add(vm);
            }

            if(additionalVm != null){
                long additionalAllocation = vmAllocationForOversubscriptionLevel(oversubscriptionLevel, additionalVm);
                if(additionalAllocation>0){
                    sumAllocation += additionalVm.getPesNumber();
                    if(!uniqueConsumerCount.contains(additionalVm))
                        uniqueConsumerCount.add(additionalVm);
                }
            }

            if(oversubscriptionLevel < minimalOversubscription)
                minimalOversubscription = oversubscriptionLevel;
        }

        if(uniqueConsumerCount.size() < this.criticalSize)
            return sumAllocation;
        return (long) Math.ceil(sumAllocation/minimalOversubscription);
    }

    private long vmAllocationForOversubscriptionLevel(Float oversubscriptionLevel, VmOversubscribable vm){
        long allocation = 0;
        for(int vcpu = 0; vcpu < vm.getPesNumber(); vcpu++){
            float vcpuOversubscription = this.oversubscriptionTemplate[vcpu];
            if(vcpuOversubscription == oversubscriptionLevel)
                allocation+=1;
        }
        return allocation;
    }

}

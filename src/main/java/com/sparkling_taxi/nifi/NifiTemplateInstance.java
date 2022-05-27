package com.sparkling_taxi.nifi;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Utility class to get a template running
 */
public class NifiTemplateInstance {
    private final String templateFile;

    private final NifiExecutor executor;

    private String templateId;
    private String processorGroupId;

    private int processorGroupVersion;

    private List<NifiControllerService> controllerServiceIds;

    public NifiTemplateInstance(String file) {
        this.templateFile = file;
        this.executor = new NifiExecutor();
        controllerServiceIds = new ArrayList<>();
    }

    public NifiExecutor getExecutor() {
        return executor;
    }

    public int getProcessorGroupVersion() {
        return processorGroupVersion;
    }

    public void incrementProcessGroupVersion() {
        processorGroupVersion++;
    }

    // TODO: dopo se c'è tempo
    public boolean areAllControllerServicesRunning() {
        return controllerServiceIds.stream().allMatch(executor::isControllerServiceRunning);
    }

    public boolean isRunning() {
        return false;
    }

//    private Optional<String> uploadAndInstantiateTemplate(String template) {
//        Optional<String> templateId = executor.uploadTemplate(template);
//        // if it all goes well
//        if (templateId.isPresent()) {
//            System.out.println("templateId = " + templateId.get());
//            // instantiate a processGroup from the template
//            Optional<String> s = executor.instantiateTemplate(templateId.get());
//            if (s.isPresent()) {
//                System.out.println(s.get());
//                return true;
//            }
//        }
//        return false;
//    }

    /**
     * This method:
     * - upload a  template
     * - instantiate the template
     *
     * @return true if all goes well
     */
    public boolean uploadAndInstantiateTemplate() {
        Optional<String> templateId = executor.uploadTemplate(templateFile);
        // if it all goes well
        if (templateId.isPresent()) {
            this.templateId = templateId.get();
            System.out.println("templateId = " + this.templateId);
            // instantiate a processGroup from the template
            Optional<String> s = executor.instantiateTemplate(templateId.get());
            if (s.isPresent()) {
                System.out.println("processorGroupId = " + s.get());
                processorGroupId = s.get();
                controllerServiceIds = executor.getControllerServices();
                return true;
            }
        }
        return false;
    }

    /**
     * This method:
     * - activate controller services
     * - run all processors
     */
    public boolean runAll() {
        runAllControllerServices();
        boolean running = executor.setRunStatusOfProcessorGroup(processorGroupId, processorGroupVersion, "RUNNING");
        incrementProcessGroupVersion();
        return running;
    }

    /**
     * This method:
     * - stop controller services
     * - stop all processors
     * - empties all queues
     */
    public boolean stopAll() {
        boolean stoppedServices = stopAllControllerServices();
        boolean stoppedProcessGroups = executor.setRunStatusOfProcessorGroup(processorGroupId, processorGroupVersion, "STOPPED");
        incrementProcessGroupVersion();
        boolean emptied = executor.emptyQueues(processorGroupId);
        return stoppedServices && stoppedProcessGroups && emptied;
    }

    /**
     * This method
     * - remove processor group instance
     * - remove controller services instances
     * - delete template from Nifi
     *
     * @return true if all goes well
     */
    public boolean removeAll() {

        boolean templateDeleted = executor.deleteTemplate(templateId);

        // remove the process group of the template
        List<String> theProcessGroup = executor.getProcessorGroups();
        System.out.println("removed process groups: " + theProcessGroup.size());
        boolean emptied = theProcessGroup.stream().allMatch(executor::emptyQueues);
        // remove all processGroups
        boolean allGroupsDeleted = theProcessGroup.stream().allMatch(executor::removeProcessGroup);
        // remove all controllers services
        boolean allServicesDeleted = controllerServiceIds.stream().allMatch(executor::removeControllerService);
        System.out.println("allServicesDeleted = " + allServicesDeleted);
        return templateDeleted && emptied && allGroupsDeleted && allServicesDeleted;
    }

    public List<NifiControllerService> getControllerServices() {
        return controllerServiceIds;
    }

    public boolean runAllControllerServices() {
        boolean ss = true;
        for (NifiControllerService id : controllerServiceIds) {
            ss = ss && executor.runControllerService(id);
        }
        return ss;
    }

    public boolean stopAllControllerServices() {
        boolean ss = true;
        for (NifiControllerService id : controllerServiceIds) {
            ss = ss && executor.stopControllerService(id);
        }
        return ss;
    }

    public String getProcessorGroup() {
        return processorGroupId;
    }
}

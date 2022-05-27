package com.sparkling_taxi.nifi;

import org.junit.jupiter.api.Test;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * These tests requires the NiFi Docker container to be running
 */
public class NifiTest {

    private final NifiExecutor executor = new NifiExecutor();
    private static final String EXAMPLE_TEMPLATE = ".nifi/templates/preprocessing_query1.xml";

    @Test
    public void getRootProcessorGroupTest() {
        String root = executor.getRootProcessGroup();
        assertNotNull(root);
        System.out.println("IDs of processor groups in root: " + root);
    }

    @Test
    public void instantiateTemplateTest() {
        // upload a template
        NifiTemplateInstance n = new NifiTemplateInstance(EXAMPLE_TEMPLATE);
        boolean b = n.uploadAndInstantiateTemplate();
        assertTrue(b, "TEST: Failed to instantiate the template " + EXAMPLE_TEMPLATE);
        // Delete the template, groups and controller services to make the test repeatable
        boolean d = n.removeAll();
        assertTrue(d, "TEST: failed to delete template " + EXAMPLE_TEMPLATE);
    }

    /**
     * Initially there are no processor Groups and check all are empty
     */
    @Test
    public void listProcessorGroupsTest() {
        List<String> processorGroups = executor.getProcessorGroups();
        int prevSize = processorGroups.size();

        // calls the previous test to get at least one template...
        instantiateTemplateTest();
        List<String> processorGroupsNew = executor.getProcessorGroups();
        assertEquals(prevSize + 1, processorGroupsNew.size());
        System.out.println(processorGroupsNew);

        // checks Processors inside the processor Group:
        assertEquals(6, executor.getProcessors(processorGroupsNew.get(0)).size());

        // removes all the templates
        for (String processorGroup : processorGroupsNew) {
            executor.removeProcessGroup(processorGroup);
        }
    }

    @Test
    public void controllerServicesTest() {
        NifiTemplateInstance n = new NifiTemplateInstance(EXAMPLE_TEMPLATE);
        assertTrue(n.uploadAndInstantiateTemplate());
        List<NifiControllerService> controllerServices = n.getControllerServices();
        assertFalse(controllerServices.isEmpty());

        // assertFalse(n.areAllControllerServicesRunning());

        assertTrue(n.runAllControllerServices(), "Not all running...");

        // assertTrue(n.areAllControllerServicesRunning());
        n.stopAllControllerServices();

        // removes template, process group in the flow and controller services
        assertTrue(n.removeAll());
    }

    @Test
    public void runProcessorGroup() throws InterruptedException {
        NifiTemplateInstance n = new NifiTemplateInstance(EXAMPLE_TEMPLATE);
        assertTrue(n.uploadAndInstantiateTemplate());

        assertTrue(n.runAll());

        Thread.sleep(2000);

        assertTrue(n.stopAll());

        Thread.sleep(12000); //TODO: attendere che gli InvokeHttp finiscano di eseguire...

        assertTrue(n.removeAll());
    }

    @Test
    public void clearAllControllerServices(){
        List<NifiControllerService> controllerServices = executor.getControllerServices();
        int size = controllerServices.size();
        int done = 0;
        for (NifiControllerService controllerService : controllerServices) {
            executor.stopControllerService(controllerService);
            executor.removeControllerService(controllerService);
            done++;
        }
        assertEquals(done, size);
    }

    public static void main(String[] args) {
        NifiExecutor nifiExecutor = new NifiExecutor();
        List<String> processorGroups = nifiExecutor.getProcessorGroups();
        String s = processorGroups.get(0);
        nifiExecutor.emptyQueues(s);
    }
}

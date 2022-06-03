package com.sparkling_taxi.nifi;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * These tests requires the NiFi Docker container to be running
 */
public class NifiTest {

    public static final String TEST_NIFI_API_URL = "http://localhost:8181/nifi-api/";
    private final NifiExecutor executor = new NifiExecutor(TEST_NIFI_API_URL);
    private static final String EXAMPLE_TEMPLATE = ".nifi/templates/simple_preprocessing_query1.xml";

    @Test
    public void getRootProcessorGroupTest() {
        String root = executor.getRootProcessGroup();
        assertNotNull(root);
        System.out.println("IDs of processor groups in root: " + root);
    }

    @Test
    public void instantiateTemplateTest() {
        // upload a template
        NifiTemplateInstance n = new NifiTemplateInstance(EXAMPLE_TEMPLATE, TEST_NIFI_API_URL);
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
        NifiTemplateInstance n = new NifiTemplateInstance(EXAMPLE_TEMPLATE, TEST_NIFI_API_URL);
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
    public void runProcessorGroup() {
        NifiTemplateInstance n = new NifiTemplateInstance(EXAMPLE_TEMPLATE, TEST_NIFI_API_URL);
        assertTrue(n.uploadAndInstantiateTemplate(), "Failed to instantiate");
        assertTrue(n.runAll(), "Failed to run");
        assertTrue(n.stopAll(), "Failed to stop");
        assertTrue(n.removeAll(), "Failed to remove");
    }

    @Test
    public void clearAllControllerServices() {
        List<String> processorGroups = executor.getProcessorGroups();
        for (String processorGroup : processorGroups) {
            List<NifiControllerService> controllerServices = executor.getControllerServices(processorGroup);
            int size = controllerServices.size();
            int done = 0;
            for (NifiControllerService controllerService : controllerServices) {
                executor.stopControllerService(controllerService);
                executor.removeControllerService(controllerService);
                done++;
            }
            assertEquals(done, size);
        }
    }

    @Test
    public void numberProcessorsRunningTest(){
        NifiTemplateInstance nifi = new NifiTemplateInstance(EXAMPLE_TEMPLATE, TEST_NIFI_API_URL);
        int n = nifi.numberProcessRunning();
        assertEquals(0, n, "There are more than 0 processors running at the start!!!");

        nifi.uploadAndInstantiateTemplate();
        nifi.runAll();
        int n1 = nifi.numberProcessRunning();
        assertEquals(5, n1, "Not all processors are running...");

        nifi.stopAll();
        nifi.removeAll();
    }
}

package topologies;

import topologyBuilder.TopologyCreator;


public class EventDetectionKeyBasedWithSleep {
    public static void main(String[] args) throws Exception {
        TopologyCreator topologyCreator = new TopologyCreator();
        topologyCreator.submitTopologyWithCassandraKeyBasedWithSleep();
    }
}

package topologies;

import topologyBuilder.TopologyCreator;


public class EventDetectionKeyBased {
    public static void main(String[] args) throws Exception {
        TopologyCreator topologyCreator = new TopologyCreator();
        topologyCreator.submitTopologyWithCassandraKeyBased();
    }
}

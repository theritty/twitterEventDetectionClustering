package topologies;

import topologyBuilder.TopologyCreator;

public class EventDetectionHybrid {
    public static void main(String[] args) throws Exception {
        TopologyCreator topologyCreator = new TopologyCreator();
        topologyCreator.submitTopologyWithCassandraHybrid();
    }
}
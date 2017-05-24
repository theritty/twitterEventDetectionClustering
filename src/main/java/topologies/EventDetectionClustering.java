package topologies;

import topologyBuilder.TopologyCreator;

public class EventDetectionClustering {
    public static void main(String[] args) throws Exception {
        TopologyCreator topologyCreator = new TopologyCreator();
        topologyCreator.submitTopologyWithCassandraClustering();
    }
}
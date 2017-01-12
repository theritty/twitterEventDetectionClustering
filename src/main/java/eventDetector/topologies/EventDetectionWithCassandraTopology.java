package eventDetector.topologies;

import topologyBuilder.TopologyCreator;

public class EventDetectionWithCassandraTopology {
    public static void main(String[] args) throws Exception {
        TopologyCreator topologyCreator = new TopologyCreator();
        topologyCreator.submitTopologyWithCassandra();
    }
}
package eventDetector.topologies;

import topologyBuilder.TopologyCreator;

/**
 * Created by ozlemcerensahin on 10/05/2017.
 */
public class EventDetectionKeyBased {
    public static void main(String[] args) throws Exception {
        TopologyCreator topologyCreator = new TopologyCreator();
        topologyCreator.submitTopologyWithCassandraKeyBased();
    }
}

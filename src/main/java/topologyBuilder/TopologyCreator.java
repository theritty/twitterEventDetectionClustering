package topologyBuilder;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.shade.org.json.simple.JSONValue;
import org.apache.storm.thrift.transport.TTransportException;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;


public class TopologyCreator {
    private static LocalCluster localCluster;
    private Config config;
    private Properties properties;

    public TopologyCreator() throws IOException {
        TopologyHelper topologyHelper = new TopologyHelper();
        properties = topologyHelper.loadProperties( "config.properties" );
        config = topologyHelper.copyPropertiesToStormConfig(properties);
    }



    public void submitTopologyWithCassandraKeyBased()
    {
        try
        {
            loadTopologyPropertiesAndSubmitKeyBased( properties, config, BoltBuilder.prepareBoltsForCassandraSpoutKeyBased(properties) );
        }
        catch ( TTransportException | InvalidTopologyException | AuthorizationException | AlreadyAliveException | InterruptedException e )
        {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    protected synchronized void loadTopologyPropertiesAndSubmitKeyBased(Properties properties, Config config, StormTopology stormTopology )
            throws InvalidTopologyException, AuthorizationException, AlreadyAliveException, InterruptedException, TTransportException {

        String stormExecutionMode = properties.getProperty("storm.execution.mode");
        String topologyName = properties.getProperty("storm.topology.name");
        String nimbusHosts = properties.getProperty("nimbus.host");
        int nimbusPort = Integer.parseInt(properties.getProperty("nimbus.port"));
        String stormZookeeperServers = properties.getProperty("storm.zookeeper.serverss");
        String stormPorts = properties.getProperty("storm.ports");
        int numWorkers = Integer.parseInt(properties.getProperty("num.workers"));

//        config.setDebug( true );

        switch (stormExecutionMode) {
            case ("cluster"):
                List<String> nimbusSeeds = TopologyHelper.splitString(nimbusHosts);
                config.put(Config.NIMBUS_SEEDS, nimbusSeeds);
                config.setDebug(true);
                config.put(Config.STORM_ZOOKEEPER_SERVERS, stormZookeeperServers);
                config.put(Config.SUPERVISOR_SLOTS_PORTS, stormPorts);
                config.setNumAckers(numWorkers);
                config.setNumWorkers(numWorkers);
                Map storm_conf = Utils.readStormConfig();
                storm_conf.put("nimbus.seeds", nimbusSeeds);
                List<String> servers = TopologyHelper.splitString(stormZookeeperServers);
                storm_conf.put("storm.zookeeper.servers", servers);
                List<Integer> ports = TopologyHelper.splitInteger(stormPorts);
                storm_conf.put("supervisor.slots.ports", ports);
                storm_conf.put("topology.workers", numWorkers);
                storm_conf.put("topology.acker.executors", numWorkers);
//                String workingDir = System.getProperty("user.dir");
                String inputJar = "/Users/ozlemcerensahin/Desktop/workspace/twitterEventDetectionClustering/target/storm-twitter-1.0-SNAPSHOT-jar-with-dependencies.jar";
                NimbusClient nimbus = new NimbusClient(storm_conf, nimbusHosts, nimbusPort);
                String uploadedJarLocation = StormSubmitter.submitJar(storm_conf, inputJar);

                try {
                    String jsonConf = JSONValue.toJSONString(storm_conf);
                    nimbus.getClient().submitTopology(topologyName,
                            uploadedJarLocation, jsonConf, stormTopology);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                wait(144000000);
                nimbus.close();
                localCluster.killTopology(topologyName);
                localCluster.shutdown();
                break;

            case ("local"):
                if (localCluster == null) localCluster = new LocalCluster();
                Config conf = new Config();
                conf.setDebug(true);
//                conf.put(Config.TOPOLOGY_SLEEP_SPOUT_WAIT_STRATEGY_TIME_MS, 6);
                localCluster.submitTopology(topologyName, conf, stormTopology);

                while (true) {
                    Thread.sleep(1000000);
                }

//                long sleep_day=10;
//                long sleep_hour=50;
//                long sleep_minute=50;
//                long sleep_seconds=0;
//                Utils.sleep(sleep_day*24*60*60*1000 + sleep_hour*60*60*1000 + sleep_minute*60*1000 + sleep_seconds*1000);
//
//                System.out.println("Shutting down");
//                localCluster.killTopology(topologyName);
//                localCluster.shutdown();


        }


    }


    public void submitTopologyWithCassandraClustering()
    {
        try
        {
            loadTopologyPropertiesAndSubmitClustering( properties, config, BoltBuilder.prepareBoltsForCassandraSpoutClustering(properties) );
        }
        catch (Exception e )
        {
            e.printStackTrace();
        }
    }


    protected synchronized void loadTopologyPropertiesAndSubmitClustering(Properties properties, Config config, StormTopology stormTopology )
            throws  Exception {

        String stormExecutionMode = properties.getProperty( "storm.execution.mode" );
        String topologyName = properties.getProperty( "storm.topology.name" );
        String nimbusHosts = properties.getProperty( "nimbus.host" );
        int nimbusPort = Integer.parseInt( properties.getProperty( "nimbus.port" ) );
        String stormZookeeperServers = properties.getProperty( "storm.zookeeper.serverss" );
        String stormPorts = properties.getProperty( "storm.ports" );
        int numWorkers = Integer.parseInt(properties.getProperty( "num.workers" ));

        switch ( stormExecutionMode )
        {
            case ( "cluster" ):
                List<String> nimbusSeeds = TopologyHelper.splitString( nimbusHosts );
                config.put( Config.NIMBUS_SEEDS, nimbusSeeds );
                config.setDebug(true);
                config.put( Config.STORM_ZOOKEEPER_SERVERS, stormZookeeperServers );
                config.put( Config.SUPERVISOR_SLOTS_PORTS, stormPorts );
                config.setNumAckers(numWorkers);
                config.setNumWorkers(numWorkers);
                Map storm_conf = Utils.readStormConfig();
                storm_conf.put("nimbus.seeds", nimbusSeeds);
                List<String> servers = TopologyHelper.splitString(stormZookeeperServers);
                storm_conf.put("storm.zookeeper.servers", servers);
                List<Integer> ports = TopologyHelper.splitInteger(stormPorts);
                storm_conf.put("supervisor.slots.ports",ports);
                storm_conf.put("topology.workers", numWorkers);
                storm_conf.put("topology.acker.executors", numWorkers);
//                String workingDir = System.getProperty("user.dir");
                String inputJar = "/Users/ozlemcerensahin/Desktop/workspace/twitterEventDetectionClustering/target/storm-twitter-1.0-SNAPSHOT-jar-with-dependencies.jar";
                NimbusClient nimbus = new NimbusClient(storm_conf, nimbusHosts, nimbusPort );
                String uploadedJarLocation = StormSubmitter.submitJar(storm_conf,inputJar );

                try {
                    String jsonConf = JSONValue.toJSONString( storm_conf );
                    nimbus.getClient().submitTopology( topologyName,
                            uploadedJarLocation, jsonConf, stormTopology );
                } catch (Exception e) {
                    e.printStackTrace();
                }

                wait(144000000);
                nimbus.close();
                localCluster.killTopology(topologyName);
                localCluster.shutdown();
                break;

            case ( "local" ):
                if(localCluster==null) localCluster = new LocalCluster();
                Config conf = new Config();
                conf.setDebug(true);
                localCluster.submitTopology( topologyName, conf, stormTopology );

                //wait 40 hours
                wait(144000000);

                System.out.println("Shutting down");
                localCluster.killTopology(topologyName);
                localCluster.shutdown();


        }
    }
}

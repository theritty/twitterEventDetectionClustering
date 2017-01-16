package topologyBuilder;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import org.apache.storm.shade.org.json.simple.JSONValue;
import org.apache.thrift7.TException;
import org.apache.thrift7.transport.TTransportException;

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


    public void submitTopologyWithCassandra()
    {
        try
        {
            loadTopologyPropertiesAndSubmit( properties, config, BoltBuilder.prepareBoltsForCassandraSpout(properties) );
        }
        catch ( TTransportException | InvalidTopologyException | AuthorizationException | AlreadyAliveException | InterruptedException e )
        {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    protected synchronized void loadTopologyPropertiesAndSubmit(Properties properties, Config config, StormTopology stormTopology )
            throws InvalidTopologyException, AuthorizationException, AlreadyAliveException, InterruptedException, TTransportException {

        String stormExecutionMode = properties.getProperty( "storm.execution.mode" );
        String topologyName = properties.getProperty( "storm.topology.name" );
        String nimbusHost = properties.getProperty( "nimbus.host" );
        int nimbusPort = Integer.parseInt( properties.getProperty( "nimbus.port" ) );
        String stormZookeeperServers = properties.getProperty( "storm.zookeeper.serverss" );
        String stormPorts = properties.getProperty( "storm.ports" );

//        config.setDebug( true );

        switch ( stormExecutionMode )
        {
            case ( "cluster" ):
                config.put( Config.NIMBUS_HOST, nimbusHost );
                config.put( Config.STORM_ZOOKEEPER_SERVERS, stormZookeeperServers );
                config.put( Config.SUPERVISOR_SLOTS_PORTS, stormPorts );
                config.setNumAckers( 3 );
                //config.setNumWorkers( 3 );
                Map storm_conf = Utils.readStormConfig();
                storm_conf.put("nimbus.host", nimbusHost);
                List<String> servers = TopologyHelper.splitString( stormZookeeperServers );
                storm_conf.put( "storm.zookeeper.servers", servers  );
                List<Integer> ports = TopologyHelper.splitInteger( stormPorts );
                storm_conf.put( "supervisor.slots.ports",ports  );
                String workingDir = System.getProperty("user.dir");
                String inputJar = workingDir + "/target/Demo-jar-with-dependencies.jar";
                NimbusClient nimbus = new NimbusClient(storm_conf, nimbusHost, nimbusPort );
                String uploadedJarLocation = StormSubmitter.submitJar(storm_conf,inputJar );
                try {
                    String jsonConf = JSONValue.toJSONString( storm_conf );
                    nimbus.getClient().submitTopology( topologyName,
                            uploadedJarLocation, jsonConf, stormTopology );
                } catch ( TException e ) {
                    e.printStackTrace();
                }
                wait(144000000);
                break;

            case ( "local" ):
                if(localCluster==null) localCluster = new LocalCluster();
                Config conf = new Config();
                conf.setDebug(false);
                localCluster.submitTopology( topologyName, conf, stormTopology );

                //wait 40 hours
                wait(144000000);

                System.out.println("Shutting down");
                localCluster.killTopology(topologyName);
                localCluster.shutdown();


        }
    }
}

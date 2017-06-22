package eventDetectionHybrid;

import cassandraConnector.CassandraDaoHybrid;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.UUIDs;
import algorithms.*;
import drawing.*;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import topologyBuilder.Constants;
import topologyBuilder.TopologyHelper;

import java.util.*;

class Cluster {
    String country;
    UUID id;
    HashMap<String, Double> cosinevector;
    int currentnumtweets;
    long lastround;
    int prevnumtweets;

    public Cluster(String country, UUID id, HashMap<String, Double> cosinevector, int currentnumtweets, long lastround, int prevnumtweets) {
        this.country = country;
        this.id = id;
        this.cosinevector = cosinevector;
        this.currentnumtweets = currentnumtweets;
        this.lastround = lastround;
        this.prevnumtweets = prevnumtweets;
    }
}

public class EventDetectorBoltHybrid extends BaseRichBolt {

    private OutputCollector collector;
    private int componentId;
    private String fileNum;
    private Date lastDate = new Date();

    private CassandraDaoHybrid cassandraDao;

    private CosineSimilarity cosineSimilarity = new CosineSimilarity();


    public EventDetectorBoltHybrid(String filenum, CassandraDaoHybrid cassandraDao)
    {
        this.fileNum = filenum + "/";
        this.cassandraDao = cassandraDao;
    }
    @Override
    public void prepare(Map config, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        this.componentId = context.getThisTaskId()-1;
        TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", "eventdet : " + componentId  );
    }


    @Override
    public void execute(Tuple tuple) {
        long round = tuple.getLongByField("round");
        String country = tuple.getStringByField("country");
        ArrayList<HashMap<String, Double>> clusters = (ArrayList<HashMap<String, Double>>) tuple.getValueByField("clusters");


        TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", country + " event detection " + round + " num of clusters: " + clusters.size());
        if(round==0L)  return;

        Date nowDate = new Date();
        try {
            for(int i=0; i< clusters.size();) {
                TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt",  "HoHoHoHo" + clusters.get(i).get("numTweets"));
                if (clusters.get(i).get("numTweets") < 50.0)
                    clusters.remove(i);
                else i++;
            }

            System.out.println(country + " final " + componentId + " " + clusters.size());

            TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", new Date() + " Event Detector " + componentId + " evaluates clusters for "  + round + " " + country + " num of clusters: " + clusters.size());

            int updateCount = 0;
            int deleteCount = 0;
            ResultSet resultSet ;
            try {
                resultSet = cassandraDao.getClusters(country);
                Iterator<Row> iterator = resultSet.iterator();
                while (iterator.hasNext()) {
                    boolean updated = false;
                    Row row = iterator.next();
                    Cluster cNew = new Cluster(row.getString("country"), row.getUUID("id"), (HashMap<String, Double>) row.getMap("cosinevector", String.class, Double.class), row.getInt("currentnumtweets"), row.getLong("lastround"), row.getInt("prevnumtweets"));

                    if(clusters.size()<=0) break;
                    double maxSim = 0;
                    for(int j=0;j<clusters.size();) {
                        double similarity = cosineSimilarity.cosineSimilarityFromMap(cNew.cosinevector, clusters.get(j));

                        if(similarity>maxSim) maxSim = similarity;
                        if(similarity>0.5) {
                            cNew = updateCluster(cNew, clusters.get(j));
                            TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", "Update clusters between cass and local!!!!!!!");
                            clusters.remove(j);
                            updated = true;
                        }
                        else j++;
                    }

                    if(updated) {
                        updateCount++;
                        Iterator<Map.Entry<String, Double>> it = cNew.cosinevector.entrySet().iterator();
                        while(it.hasNext()) {
                            Map.Entry<String, Double> entry = it.next();
                            double value = entry.getValue();
                            if(value < 0.01) it.remove();
                        }

                        if( ((double) cNew.currentnumtweets / (double) (cNew.currentnumtweets + cNew.prevnumtweets) > 0.5) ) {
                            System.out.println("updated event again: " + cNew.id);
                            List<Object> values_event = new ArrayList<>();
                            values_event.add(round);
                            values_event.add(cNew.id);
                            values_event.add(cNew.country);
                            values_event.add(cNew.cosinevector);
                            values_event.add((double) cNew.currentnumtweets / (double) (cNew.currentnumtweets + cNew.prevnumtweets));
                            values_event.add(cNew.currentnumtweets + cNew.prevnumtweets );
                            cassandraDao.insertIntoEvents(values_event.toArray());
                            TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", new Date() + "Updated Event Here !!!! :)))) ");

                        }
                        else
                            System.out.println("event rate " + (double) cNew.currentnumtweets / (double) (cNew.currentnumtweets + cNew.prevnumtweets) );

                        List<Object> values2 = new ArrayList<>();
                        values2.add(cNew.id);
                        values2.add(country);
                        values2.add(cNew.cosinevector);
                        values2.add(cNew.currentnumtweets + cNew.prevnumtweets);
                        values2.add(0);
                        values2.add(round);
                        cassandraDao.insertIntoClusters(values2.toArray());
                    }
                    else if(round - cNew.lastround >= 2) {
                        cassandraDao.deleteFromClusters(country, cNew.id);
                        deleteCount++;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }


            System.out.println("start add");
            for(int i=0; i<clusters.size();i++) {
                Iterator<Map.Entry<String, Double>> it = clusters.get(i).entrySet().iterator();
                while(it.hasNext()) {
                    Map.Entry<String, Double> entry = it.next();
                    double value = entry.getValue();
                    if(value < 0.05) {
                        it.remove();
                    }
                }

                addNewCluster(round, clusters.get(i), country);
            }

            System.out.println( country + " Update count " + updateCount + " delete count " + deleteCount + " new count " + clusters.size() );

            TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", new Date() + " Event Detector " + componentId + " finished evaluating clusters for "  + round + " " + country);

            List<Object> values = new ArrayList<>();
            values.add(round);
            values.add(componentId);
            values.add(true);
            cassandraDao.insertIntoProcessed(values.toArray());

        } catch (Exception e) {
            e.printStackTrace();
        }
        lastDate = new Date();
        System.out.println("update done");


        TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", "round " + round + " put excel " + componentId);
        ExcelWriter.putData(componentId, nowDate, lastDate, round,cassandraDao);
        collector.ack(tuple);
        clusters.clear();
    }


    public void addNewCluster(long round, HashMap<String, Double> newCluster, String country)  {
        try {
            if(!newCluster.containsKey("numTweets"))
                System.out.println("no key");
            int numTweets = newCluster.get("numTweets").intValue();
            newCluster.remove("numTweets");
            UUID clusterid = UUIDs.timeBased();
            List<Object> values = new ArrayList<>();
            values.add(clusterid);
            values.add(country);
            values.add(newCluster);
            values.add(0);
            values.add(numTweets);
            values.add(round);
            cassandraDao.insertIntoClusters(values.toArray());

            newCluster.put("numTweets", (double) numTweets);

            if(numTweets > 50) {
                List<Object> values_event = new ArrayList<>();
                values_event.add(round);
                values_event.add(clusterid);
                values_event.add(country);
                values_event.add(newCluster);
                values_event.add(1.0);
                values_event.add(numTweets);
                cassandraDao.insertIntoEvents(values_event.toArray());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Cluster updateCluster(Cluster c, HashMap<String, Double> cosinevectorLocal) {
        HashMap<String, Double> cosinevectorCluster = c.cosinevector;
        double numTweetsLocal   = cosinevectorLocal.get("numTweets");
        double numTweetsCluster = c.prevnumtweets;
        cosinevectorLocal.remove("numTweets");

        Iterator<Map.Entry<String, Double>> it = cosinevectorCluster.entrySet().iterator();
        while(it.hasNext()) {
            Map.Entry<String, Double> entry = it.next();
            String key = entry.getKey();
            double value = entry.getValue();

            double valueLocal = 0;
            if(cosinevectorLocal.containsKey(key))
                valueLocal = cosinevectorLocal.get(key);

            double newValue = (value * numTweetsCluster + valueLocal * numTweetsLocal) / (numTweetsLocal + numTweetsCluster);
            cosinevectorCluster.put(key, newValue);
            cosinevectorLocal.remove(key);
        }

        Iterator<Map.Entry<String, Double>> it2 = cosinevectorLocal.entrySet().iterator();
        while(it2.hasNext()) {
            Map.Entry<String, Double> entry = it2.next();
            String key = entry.getKey();
            double value = entry.getValue();
            double newValue = (value * numTweetsLocal) / (numTweetsLocal + numTweetsCluster);
            cosinevectorCluster.put(key, newValue);
        }

        c.cosinevector = cosinevectorCluster;
        c.currentnumtweets = c.currentnumtweets + (int) numTweetsLocal;

        return c;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
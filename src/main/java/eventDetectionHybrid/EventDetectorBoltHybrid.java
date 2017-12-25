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
import eventDetectionWithClustering.Cluster;
import topologyBuilder.Constants;
import topologyBuilder.TopologyHelper;

import java.util.*;

public class EventDetectorBoltHybrid extends BaseRichBolt {

    private OutputCollector collector;
    private int componentId;
    private String fileNum;
    private Date lastDate = new Date();

    private CassandraDaoHybrid cassandraDao;


    //
    private int updateCntCond = 50;
    private double updateCntPer = 0.01;
    private double similarityThreshold = 0.5;
    private int totCntThre = 100;
    private double newCntPer = 0.05;
    private ArrayList<Cluster> clusters = new ArrayList<>();

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
        System.out.println( "eventdet : " + componentId  );
    }

    public void updateCluster(Cluster cluster1, Cluster cluster2) {
        double numTweets1 = cluster1.currentnumtweets;
        double numTweets2 = cluster2.currentnumtweets;

        Iterator<Map.Entry<String, Double>> it = cluster1.cosinevector.entrySet().iterator();
        while(it.hasNext()) {
            Map.Entry<String, Double> entry = it.next();
            String key = entry.getKey();
            if(cluster2.cosinevector.containsKey(key)){
                double value1 = entry.getValue();
                double value2 = cluster2.cosinevector.get(key);
                double newValue = (value1 * numTweets1 + value2 * numTweets2) / (numTweets1+numTweets2);
                if(numTweets1+numTweets2>updateCntCond && newValue<updateCntPer) {
                    it.remove();
                }
                else {
                    cluster1.cosinevector.put(key, newValue);
                }
                cluster2.cosinevector.remove(key);
            }
        }

        Iterator<Map.Entry<String, Double>> it2 = cluster2.cosinevector.entrySet().iterator();
        while(it2.hasNext()) {
            Map.Entry<String, Double> entry = it2.next();
            String key = entry.getKey();
            double value = entry.getValue();
            double newValue = (value * numTweets2) / (numTweets1+numTweets2);
            if(numTweets1+numTweets2<updateCntCond || newValue>updateCntPer) {
                cluster1.cosinevector.put(key, newValue);
            }
        }
    }

    public void mergeClusters() {
        for(int i=0;i<clusters.size()-1;i++) {
            for(int j=i+1; j< clusters.size();){
                double similarity = cosineSimilarity.cosineSimilarityFromMap(clusters.get(j).cosinevector, clusters.get(i).cosinevector);
                if(similarity>similarityThreshold) {
                    updateCluster(clusters.get(i), clusters.get(j));
                    clusters.get(i).currentnumtweets += clusters.get(j).currentnumtweets;
                    clusters.get(i).tweetList.addAll(clusters.get(j).tweetList);
                    clusters.remove(j);

                }
                else j++;
            }
        }
    }

    @Override
    public void execute(Tuple tuple) {
        long round = tuple.getLongByField("round");
        String country = tuple.getStringByField("country");
        clusters = (ArrayList<Cluster>) tuple.getValueByField("clusters");


        TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", country + " event detection " + round + " num of clusters: " + clusters.size());
        if(round==0L)  return;

        Date nowDate = new Date();
        try {

            mergeClusters();

            for(int i=0; i< clusters.size();) {
                TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt",  "HoHoHoHo" + clusters.get(i).currentnumtweets);
                if (clusters.get(i).currentnumtweets < 50.0)
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
                        double similarity = cosineSimilarity.cosineSimilarityFromMap(cNew.cosinevector, clusters.get(j).cosinevector);

                        if(similarity>maxSim) maxSim = similarity;
                        if(similarity>similarityThreshold) {
                            cNew = updateCluster(cNew, clusters.get(j), round);
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
                            if(value < 0.05) it.remove();
                        }

                        if( ((double) cNew.currentnumtweets / (double) (cNew.currentnumtweets + cNew.prevnumtweets) > similarityThreshold) ) {
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
                Iterator<Map.Entry<String, Double>> it = clusters.get(i).cosinevector.entrySet().iterator();
                while(it.hasNext()) {
                    Map.Entry<String, Double> entry = it.next();
                    double value = entry.getValue();
                    if(value < newCntPer) {
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


    public void addNewCluster(long round, Cluster newCluster, String country)  {
        try {
            System.out.println("New");

            int numTweets = newCluster.currentnumtweets;
            UUID clusterid = UUIDs.timeBased();
            List<Object> values = new ArrayList<>();
            values.add(clusterid);
            values.add(country);
            values.add(newCluster.cosinevector);
            values.add(0);
            values.add(numTweets);
            values.add(round);
            cassandraDao.insertIntoClusters(values.toArray());

            if(numTweets > totCntThre) {
                List<Object> values_event = new ArrayList<>();
                values_event.add(round);
                values_event.add(clusterid);
                values_event.add(country);
                values_event.add(newCluster.cosinevector);
                values_event.add(1.0);
                values_event.add(numTweets);
                cassandraDao.insertIntoEvents(values_event.toArray());
            }

            for(long tweetId: newCluster.tweetList) {
                List<Object> values_event = new ArrayList<>();
                values_event.add(round);
                values_event.add(clusterid);
                values_event.add(tweetId);
                cassandraDao.insertIntoTweetsAndCluster(values_event.toArray());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Cluster updateCluster(Cluster c, Cluster cosinevectorLocal, long round) throws Exception {
        HashMap<String, Double> cosinevectorCluster = c.cosinevector;
        double numTweetsLocal   = cosinevectorLocal.currentnumtweets;
        double numTweetsCluster = c.prevnumtweets;
        System.out.println("update");

        Iterator<Map.Entry<String, Double>> it = cosinevectorCluster.entrySet().iterator();
        while(it.hasNext()) {
            Map.Entry<String, Double> entry = it.next();
            String key = entry.getKey();
            double value = entry.getValue();

            double valueLocal = 0;
            if(cosinevectorLocal.cosinevector.containsKey(key))
                valueLocal = cosinevectorLocal.cosinevector.get(key);

            double newValue = (value * numTweetsCluster + valueLocal * numTweetsLocal) / (numTweetsLocal + numTweetsCluster);
            cosinevectorCluster.put(key, newValue);
            if(newValue < updateCntPer) cosinevectorCluster.remove(key);
        }

        Iterator<Map.Entry<String, Double>> it2 = cosinevectorLocal.cosinevector.entrySet().iterator();
        while(it2.hasNext()) {
            Map.Entry<String, Double> entry = it2.next();
            String key = entry.getKey();
            double value = entry.getValue();
            double newValue = (value * numTweetsLocal) / (numTweetsLocal + numTweetsCluster);
            cosinevectorCluster.put(key, newValue);
            if(newValue < newCntPer) cosinevectorCluster.remove(key);
        }

        c.cosinevector = cosinevectorCluster;
        c.tweetList.addAll(cosinevectorLocal.tweetList);
        c.currentnumtweets = c.currentnumtweets + (int) numTweetsLocal;


        for(long tweetId: cosinevectorLocal.tweetList) {
            List<Object> values_event = new ArrayList<>();
            values_event.add(round);
            values_event.add(c.id);
            values_event.add(tweetId);
            cassandraDao.insertIntoTweetsAndCluster(values_event.toArray());
        }

        return c;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
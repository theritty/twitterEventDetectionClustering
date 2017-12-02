package eventDetectionWithClustering;

import backtype.storm.task.OutputCollector;
import cassandraConnector.CassandraDao;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.UUIDs;
import algorithms.*;
import drawing.*;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import topologyBuilder.Constants;
import topologyBuilder.TopologyHelper;

import java.io.IOException;
import java.util.*;

public class EventDetectorBoltClustering extends BaseRichBolt {

    private OutputCollector collector;
    private int componentId;
    private String fileNum;
    private Date lastDate = new Date();
    private String country;
    private int count ;
    private int taskNum = 0;

    private CassandraDao cassandraDao;

    private CosineSimilarity cosineSimilarity = new CosineSimilarity();
    private ArrayList<Cluster> clusters = new ArrayList<>();


    public EventDetectorBoltClustering(String filenum, CassandraDao cassandraDao, String country, int taskNum)
    {
        this.fileNum = filenum + "/";
        this.cassandraDao = cassandraDao;
        this.country = country;
        this.count = 0;
        this.taskNum = taskNum;
    }
    @Override
    public void prepare(Map config, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        this.componentId = context.getThisTaskId()-1;
        TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", "eventdet : " + componentId + " " + country );
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
                if(numTweets1+numTweets2>50 && newValue<0.01) {
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
            if(numTweets1+numTweets2<50 || newValue>0.01) {
                cluster1.cosinevector.put(key, newValue);
            }
        }
    }

    public void mergeClusters() {
        for(int i=0;i<clusters.size()-1;i++) {
            for(int j=i+1; j< clusters.size();){
                double similarity = cosineSimilarity.cosineSimilarityFromMap(clusters.get(j).cosinevector, clusters.get(i).cosinevector);
                if(similarity>0.5) {
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
        ArrayList<Cluster> clustersx = (ArrayList<Cluster>) tuple.getValueByField("clusters");
        clusters.addAll(clustersx);


        if(++count < taskNum) {
            TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", "Event detector " + componentId + " gets entry. " + count + ". Not finished " + round + " " + country);
            return ;
        }

        TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", country + " event detection " + round);
        if(round==0L) {
            try {
                ExcelWriter.createTimeChart(cassandraDao);
            } catch (IOException e) {
                e.printStackTrace();
            }
            collector.ack(tuple);
            return;
        }
        Date nowDate = new Date();
        try {
            TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", country +  " " + round + " before " + clusters.size());
            System.out.println(country + " before " + componentId + " " + clusters.size());

//            for(HashMap<String, Double> clustersdd : clusters) {
//                if(String.valueOf((long) (double) clustersdd.get("clusterid")).startsWith("3")) {
//                    System.out.println("nooooo " + clustersdd.get("clusterid"));
//                }
//            }


            mergeClusters();
            TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", country +  " " + round + " after " + clusters.size());
            System.out.println(country + " after " + componentId + " " + clusters.size());
//
//            for(HashMap<String, Double> clustersdd : clusters) {
//                if(String.valueOf((long) (double) clustersdd.get("clusterid")).startsWith("3")) {
//                    System.out.println("nooooo2 " + clustersdd.get("clusterid"));
//                }
//            }

            for(int i=0; i< clusters.size();) {
                if (clusters.get(i).currentnumtweets < 30.0)
                    clusters.remove(i);
                else i++;
            }

//            for(HashMap<String, Double> clustersdd : clusters) {
//                if(String.valueOf((long) (double) clustersdd.get("clusterid")).startsWith("3")) {
//                    System.out.println("nooooo2 " + clustersdd.get("clusterid"));
//                }
//            }

            TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", new Date() + " Event Detector " + componentId + " evaluates clusters for "  + round + " " + country);

            int updateCount = 0;
            int deleteCount = 0;
            ResultSet resultSetx ;
            try {
                resultSetx = cassandraDao.getClusters(country);
                Iterator<Row> iteratorx = resultSetx.iterator();
                while (iteratorx.hasNext()) {

//
//                    for(HashMap<String, Double> clustersdd : clusters) {
//                        if(String.valueOf((long) (double) clustersdd.get("clusterid")).startsWith("3")) {
//                            System.out.println("nooooo3 " + clustersdd.get("clusterid"));
//                        }
//                    }

                    boolean updated = false;
                    Row row = iteratorx.next();
                    Cluster cNew = new Cluster(row.getString("country"), row.getUUID("id"), (HashMap<String, Double>) row.getMap("cosinevector", String.class, Double.class), row.getInt("currentnumtweets"), row.getLong("lastround"), row.getInt("prevnumtweets"));

                    if(clusters.size()<=0) break;
                    double maxSim = 0;
                    for(int j=0;j<clusters.size();) {
                        double similarity = cosineSimilarity.cosineSimilarityFromMap(cNew.cosinevector, clusters.get(j).cosinevector);

                        if(similarity>maxSim) maxSim = similarity;
                        if(similarity>0.5) {
                            cNew = updateCluster(cNew, clusters.get(j), round);
                            System.out.println("Update clusters between cass and local!!!!!!!");
                            clusters.remove(j);
                            updated = true;
                        }
                        else j++;
                    }
                    System.out.println("max sim " + maxSim);

                    if(updated) {
                        updateCount++;
                        Iterator<Map.Entry<String, Double>> it = cNew.cosinevector.entrySet().iterator();
                        while(it.hasNext()) {
                            Map.Entry<String, Double> entry = it.next();
                            double value = entry.getValue();
                            if(value < 0.01) {
                                it.remove();
                            }
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


            for(int i=0; i<clusters.size();i++) {
                Iterator<Map.Entry<String, Double>> it = clusters.get(i).cosinevector.entrySet().iterator();
                while(it.hasNext()) {
                    Map.Entry<String, Double> entry = it.next();
                    double value = entry.getValue();
                    if(value < 0.05) {
                        it.remove();
                    }
                }
                addNewCluster(round, clusters.get(i));
            }

            System.out.println( country + " Update count " + updateCount + " delete count " + deleteCount + " new count " + clusters.size() );

            TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", new Date() + " Event Detector " + componentId + " finished evaluating clusters for "  + round + " " + country);

            List<Object> values3 = new ArrayList<>();
            values3.add(round);
            values3.add(componentId);
            values3.add(0L);
            values3.add(0L);
            values3.add(true);
            values3.add(country);
            cassandraDao.insertIntoProcessed(values3.toArray());

        } catch (Exception e) {
            e.printStackTrace();
            count=0;
        }
        lastDate = new Date();
        count=0;
        System.out.println("update done");


        TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", "round " + round + " put excel " + componentId);
        ExcelWriter.putData(componentId, nowDate, lastDate, round,cassandraDao);
        collector.ack(tuple);
        clusters.clear();


    }


    public void addNewCluster(long round, Cluster newCluster)  {
//        "(id, country, cosinevector, prevnumtweets, currentnumtweets, lastround)"
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

            if(numTweets > 100) {
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
            cosinevectorLocal.cosinevector.remove(key);
        }

        Iterator<Map.Entry<String, Double>> it2 = cosinevectorLocal.cosinevector.entrySet().iterator();
        while(it2.hasNext()) {
            Map.Entry<String, Double> entry = it2.next();
            String key = entry.getKey();
            double value = entry.getValue();
            double newValue = (value * numTweetsLocal) / (numTweetsLocal + numTweetsCluster);
            cosinevectorCluster.put(key, newValue);
        }

//        System.out.println("CHECK:::  " + cosinevectorLocal.get("clusterid") + " String " +  String.valueOf( (long) (double) cosinevectorLocal.get("clusterid")));
//        cassandraDao.updateClusterTweets(round, String.valueOf( (long) (double) cosinevectorLocal.get("clusterid")), String.valueOf((long) (double) cosinevectorCluster.get("clusterid")));

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
        declarer.declare(new Fields( "round", "country"));
    }
}
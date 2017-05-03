package eventDetector.bolts;

import com.datastax.driver.core.utils.UUIDs;
import eventDetector.algorithms.CosineSimilarity;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import cassandraConnector.CassandraDao;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import eventDetector.drawing.ExcelWriter;
import topologyBuilder.Constants;
import topologyBuilder.TopologyHelper;

import java.io.IOException;
import java.util.*;

public class EventDetectorBolt extends BaseRichBolt {

    private OutputCollector collector;
    private long currentRound = 0;
    private long ignoredCount = 0;
    private int componentId;
    private String fileNum;
    private Date lastDate = new Date();
    private Date startDate = new Date();
    private String country;
    private int count ;
    private int taskNum = 0;

    private CassandraDao cassandraDao;

    private CosineSimilarity cosineSimilarity = new CosineSimilarity();
    private ArrayList<HashMap<String, Double>> clusters = new ArrayList<>();


    public EventDetectorBolt(String filenum, CassandraDao cassandraDao, String country, int taskNum)
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


    public void updateCluster(HashMap<String, Double> cluster1, HashMap<String, Double> cluster2) {
        double numTweets1 = cluster1.get("numTweets");
        double numTweets2 = cluster2.get("numTweets");

        Iterator<Map.Entry<String, Double>> it = cluster1.entrySet().iterator();
        while(it.hasNext()) {
            Map.Entry<String, Double> entry = it.next();
            String key = entry.getKey();
            if(cluster2.containsKey(key)){
                double value1 = entry.getValue();
                double value2 = cluster2.get(key);
                double newValue = (value1 * numTweets1 + value2 * numTweets2) / (numTweets1+numTweets2);
                cluster1.put(key, newValue);
                cluster1.put("numTweets", numTweets1+numTweets2);
                cluster2.remove(key);
            }
        }
        Iterator<Map.Entry<String, Double>> it2 = cluster2.entrySet().iterator();
        while(it2.hasNext()) {
            Map.Entry<String, Double> entry = it2.next();
            String key = entry.getKey();
            double value = entry.getValue();
            double newValue = (value * numTweets2) / (numTweets1+numTweets2);
            cluster1.put(key, newValue);
//            cluster2.remove(key);
        }
    }

    public void mergeClusters() {
        for(int i=0;i<clusters.size()-1;i++) {
            for(int j=i+1; j< clusters.size();){
                double similarity = cosineSimilarity.cosineSimilarityFromMap(clusters.get(j), clusters.get(i));
                if(similarity>0.4) {
                    updateCluster(clusters.get(i), clusters.get(j));
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
        ArrayList<HashMap<String, Double>> clustersx = (ArrayList<HashMap<String, Double>>) tuple.getValueByField("clusters");
        System.out.println("Taking " + clustersx.size());
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


        for(HashMap<String, Double> m : clusters)
            if(m.get("numTweets")>1.0)
                System.out.println(m.get("numTweets") + "   <<<<<<3333333333333");

        System.out.println("before " + componentId + " " + clusters.size());
        mergeClusters();
        System.out.println("after " + componentId + " " + clusters.size());


        for(HashMap<String, Double> m : clusters)
            if(m.get("numTweets")>1.0)
                System.out.println(m.get("numTweets") + "   <<<<<<3333333333333");

//        TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", new Date() + " Event Detector " + componentId + " evaluates clusters for "  + round + " " + country);
//        ResultSet resultSetx ;
//        try {
//            resultSetx = cassandraDao.getClusters(country);
//            Iterator<Row> iteratorx = resultSetx.iterator();
//            while (iteratorx.hasNext()) {
//                Row row = iteratorx.next();
//                HashMap<String, Double> cosinevector = (HashMap<String, Double>) row.getMap("cosinevector", String.class, Double.class);
//
//                if(clusters.size()<=0) break;
//                for(int i=0;i<clusters.size();) {
//                    double similarity = cosineSimilarity.cosineSimilarityFromMap(cosinevector, clusters.get(i));
//
//                    if(similarity>0.5) {
//                        updateCluster(row, cosinevector);
//                        clusters.remove(i);
//                    }
//                    else i++;
//                }
//
//            }
//            for(int i=0; i<clusters.size();) {
//                addNewCluster(round, clusters.get(i));
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        ResultSet resultSet ;
//        try {
//            resultSet = cassandraDao.getClusters(country);
//            Iterator<Row> iterator = resultSet.iterator();
//            while (iterator.hasNext()) {
//                Row row = iterator.next();
//                UUID clusterid = row.getUUID("id");
//                int prevnumtweets = row.getInt("prevnumtweets");
//                int currentnumtweets = row.getInt("currentnumtweets");
//                int numtweets = prevnumtweets+currentnumtweets;
//                long lastround = row.getLong("lastround");
//
//
//                //Total number of tweets is below the threshold. Remove cluster.
////                if(currentnumtweets<50) {
//////                    cassandraDao.deleteFromClusters(country, clusterid);
//////                    continue;
////                }
////                else {
//                    HashMap<String, Double> cosinevector = (HashMap<String, Double>) row.getMap("cosinevector", String.class, Double.class);
//                    Iterator<Map.Entry<String, Double>> it = cosinevector.entrySet().iterator();
//                    while(it.hasNext()) {
//                        Map.Entry<String, Double> entry = it.next();
//                        double value = entry.getValue();
//                        if(value < 0.01) it.remove();
//                    }
//
//                    List<Object> values2 = new ArrayList<>();
//                    values2.add(clusterid);
//                    values2.add(country);
//                    values2.add(cosinevector);
//                    values2.add(numtweets);
//                    values2.add(0);
//                    values2.add(round);
//                    cassandraDao.insertIntoClusters(values2.toArray());
//
//                    if( ((double) currentnumtweets)/((double) numtweets) > 0.5){
//                        //add as event
//                        List<Object> values_event = new ArrayList<>();
//                        values_event.add(round);
//                        values_event.add(clusterid);
//                        values_event.add(country);
//                        values_event.add(cosinevector);
//                        values_event.add(((double) currentnumtweets)/((double) numtweets) );
//                        values_event.add(numtweets );
//                        cassandraDao.insertIntoEvents(values_event.toArray());
//                    }
////                }
//            }
//
//
//            TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", new Date() + " Event Detector " + componentId + " finished evaluating clusters for "  + round + " " + country);
//
//            List<Object> values3 = new ArrayList<>();
//            values3.add(round);
//            values3.add(componentId);
//            values3.add(0L);
//            values3.add(0L);
//            values3.add(true);
//            values3.add(country);
//            cassandraDao.insertIntoProcessed(values3.toArray());
//
//        } catch (Exception e) {
//            e.printStackTrace();
//            count=0;
//        }
//        lastDate = new Date();
//        count=0;


        TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", "round " + round + " put excel " + componentId);
        ExcelWriter.putData(componentId, nowDate, lastDate, currentRound,cassandraDao);
        collector.ack(tuple);
        clusters.clear();


    }

    public void addNewCluster(long round, HashMap<String, Double> newCluster) throws Exception {
//        "(id, country, cosinevector, prevnumtweets, currentnumtweets, lastround)"

        if(!newCluster.containsKey("numTweets"))
            System.out.println("no key");
        double numTweets = newCluster.get("numTweets");
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

    }

    public void updateCluster(Row row, HashMap<String, Double> cosinevectorLocal) throws Exception {
        HashMap<String, Double> cosinevectorCluster = (HashMap<String, Double>) row.getMap("cosinevector", String.class, Double.class);
        double numTweetsLocal   = cosinevectorLocal.get("numTweets");
        double numTweetsCluster = row.getInt("prevnumtweets");
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
//            if(newValue < 0.001) {
//                it.remove();
//            }
//            else {
            cosinevectorCluster.put(key, newValue);
            cosinevectorLocal.remove(key);
//            }
        }

        Iterator<Map.Entry<String, Double>> it2 = cosinevectorLocal.entrySet().iterator();
        while(it2.hasNext()) {
            Map.Entry<String, Double> entry = it2.next();
            String key = entry.getKey();
            double value = entry.getValue();
            double newValue = (value * numTweetsLocal) / (numTweetsLocal + numTweetsCluster);
            cosinevectorCluster.put(key, newValue);
            cosinevectorLocal.remove(key);
        }

        List<Object> values = new ArrayList<>();
        values.add(row.getUUID("id"));
        values.add(country);
        values.add(cosinevectorCluster);
        values.add(row.getInt("prevnumtweets"));
        values.add(row.getInt("currentnumtweets")+numTweetsLocal);
        values.add(row.getLong("lastround"));
        cassandraDao.insertIntoClusters(values.toArray());

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields( "round", "country"));
    }

}
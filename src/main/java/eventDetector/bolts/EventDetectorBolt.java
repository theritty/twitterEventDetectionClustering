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

public class EventDetectorBolt extends BaseRichBolt {

    private OutputCollector collector;
    private long currentRound = 0;
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
                if(numTweets1+numTweets2>60 && newValue<0.06) {
                    it.remove();
                }
                else {
                    cluster1.put(key, newValue);
                    cluster1.put("numTweets", numTweets1+numTweets2);
                }
                cluster2.remove(key);
            }
        }
        Iterator<Map.Entry<String, Double>> it2 = cluster2.entrySet().iterator();
        while(it2.hasNext()) {
            Map.Entry<String, Double> entry = it2.next();
            String key = entry.getKey();
            double value = entry.getValue();
            double newValue = (value * numTweets2) / (numTweets1+numTweets2);
            if(numTweets1+numTweets2<60 || newValue>0.06) {
                cluster1.put(key, newValue);
            }
        }
    }

    public void mergeClusters() {
        for(int i=0;i<clusters.size()-1;i++) {
            for(int j=i+1; j< clusters.size();){
                double similarity = cosineSimilarity.cosineSimilarityFromMap(clusters.get(j), clusters.get(i));
                if(similarity>0.5) {
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
            System.out.println(country + " before " + componentId + " " + clusters.size());
            mergeClusters();
            System.out.println(country + " after " + componentId + " " + clusters.size());

            for(int i=0; i< clusters.size();) {
                if (clusters.get(i).get("numTweets") < 30.0)
                    clusters.remove(i);
                else i++;
            }

            System.out.println(country + " final " + componentId + " " + clusters.size());

            ArrayList<Cluster> cassClusters = new ArrayList<>();
            TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", new Date() + " Event Detector " + componentId + " evaluates clusters for "  + round + " " + country);
            ResultSet resultSetx ;
            try {
                resultSetx = cassandraDao.getClusters(country);
                Iterator<Row> iteratorx = resultSetx.iterator();
                while (iteratorx.hasNext()) {
                    Row row = iteratorx.next();
                    Cluster c = new Cluster(row.getString("country"), row.getUUID("id"), (HashMap<String, Double>) row.getMap("cosinevector", String.class, Double.class), row.getInt("currentnumtweets"), row.getLong("lastround"), row.getInt("prevnumtweets"));
                    cassClusters.add(c);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

            System.out.println("Cass clusters size " + cassClusters.size());

            int updateCount = 0;
            int deleteCount = 0;
            int newCount = 0;
            for(int i=0; i< cassClusters.size();i++) {
                boolean updated = false;
                Cluster cNew = cassClusters.get(i);
                if(clusters.size()<=0) break;
                double maxSim = 0;
                for(int j=0;j<clusters.size();) {
                    double similarity = cosineSimilarity.cosineSimilarityFromMap(cassClusters.get(i).cosinevector, clusters.get(j));

                    if(similarity>maxSim) maxSim = similarity;
                    if(similarity>0.6) {
                        cNew = updateCluster(cNew, clusters.get(j));
                        System.out.println("Uodate clusters between cass and local!!!!!!!");
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
                        if(value < 0.06) {
                            it.remove();
                        }
                    }

                    if( (double) cNew.currentnumtweets / (double) (cNew.currentnumtweets + cNew.prevnumtweets) > 0.5) {
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

            System.out.println("start add");
            newCount = clusters.size();
            for(int i=0; i<clusters.size();i++) {
                Iterator<Map.Entry<String, Double>> it = clusters.get(i).entrySet().iterator();
                while(it.hasNext()) {
                    Map.Entry<String, Double> entry = it.next();
                    double value = entry.getValue();
                    if(value < 0.05) {
                        it.remove();
                    }
                }

                addNewCluster(round, clusters.get(i));
            }

            System.out.println( country + " Update count " + updateCount + " delete count " + deleteCount + " new count " + newCount );

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


    public void addNewCluster(long round, HashMap<String, Double> newCluster)  {
//        "(id, country, cosinevector, prevnumtweets, currentnumtweets, lastround)"
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

            if(numTweets > 100) {
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

//        List<Object> values = new ArrayList<>();
//        values.add(row.getUUID("id"));
//        values.add(country);
//        values.add(cosinevectorCluster);
//        values.add(row.getInt("prevnumtweets"));
//        values.add(row.getInt("currentnumtweets")+numTweetsLocal);
//        values.add(row.getLong("lastround"));
//        cassandraDao.insertIntoClusters(values.toArray());

        return c;

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields( "round", "country"));
    }

}
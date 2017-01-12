package eventDetector.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cassandraConnector.CassandraDao;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import eventDetector.algorithms.CosineSimilarity;
import eventDetector.drawing.ExcelWriter;
import topologyBuilder.Constants;
import topologyBuilder.TopologyHelper;

import java.util.*;

public class WordCountBolt extends BaseRichBolt {

  private OutputCollector collector;
  private HashMap<String, Long> countsForRounds = null;
  private long currentRound = 0;
  private int threshold;
  private long ignoredCount = 0;
  private int componentId;
  private String fileNum;
  private Date lastDate = new Date();
  private Date startDate = new Date();

  private CassandraDao cassandraDao;


  public WordCountBolt(int threshold, String filenum, CassandraDao cassandraDao)
  {
    this.threshold = threshold;
    this.fileNum = filenum + "/";
    this.cassandraDao = cassandraDao;
  }
  @Override
  public void prepare(Map config, TopologyContext context,
                      OutputCollector collector) {
    this.collector = collector;
    this.countsForRounds = new HashMap<>();
    this.componentId = context.getThisTaskId()-1;
    System.out.println("wc : " + componentId );
  }

  @Override
  public void execute(Tuple tuple) {
    ArrayList<Integer> word = (ArrayList<Integer>) tuple.getValueByField("word");
    long round = tuple.getLongByField("round");
    long tweetid = tuple.getLongByField("tweetid");

    if("dummyBLOCKdone".equals(word))
       this.collector.emit(new Values(word, round, false, tuple.getValueByField("dates"), tuple.getSourceStreamId()));


    TopologyHelper.writeToFile("/Users/ozlemcerensahin/Desktop/workhistory.txt", new Date() + " Word count " + componentId + " working "  + round);
    if(round > currentRound)
    {
      TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + currentRound + ".txt",
              "Word count "+ componentId + " end for round " + currentRound + " at " + lastDate);

      TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + currentRound + ".txt",
              "Word count "+ componentId + " time taken for round" + currentRound + " is " +
                      (lastDate.getTime()-startDate.getTime())/1000);
      if ( currentRound!=0)
        ExcelWriter.putData(componentId,startDate,lastDate, "wc",tuple.getSourceStreamId(), currentRound);

      startDate = new Date();
      TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + round + ".txt",
              "Word count "+ componentId + " starting for round " + round + " at " + startDate );

      countsForRounds.clear();
      currentRound = round;
    }
    else if(round < currentRound) {
      ignoredCount++;
      if(ignoredCount%1000==0)
        TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + "ignoreCount.txt",
              "Word count ignored count " + componentId + ": " + ignoredCount );
      return;
    }

    ResultSet resultSet ;
    try {
      resultSet = cassandraDao.getCustersByRound(round);
      Iterator<Row> iterator = resultSet.iterator();

      if(!iterator.hasNext()) {
        addNewCluster(round, word, tweetid);
      }
      else {
        boolean similarclusterfound = false;
        while (iterator.hasNext()) {
          Row row = iterator.next();
          ArrayList<Integer> cosinevector = (ArrayList<Integer>) row.getList("cosinevector", Integer.class);
          if (cosinevector == null) continue;

          double similarity = CosineSimilarity.cosineSimilarity(cosinevector, word);
          if(similarity > 0.2) {
            similarclusterfound = true;
            List<Object> values = new ArrayList<>();
            values.add(row.getUUID("id"));
            values.add(tweetid);
            cassandraDao.insertIntoClusterAndTweets(values.toArray());

            System.out.println("Similar found. Cluster id: " + row.getUUID("id") + ", tweet id: " + tweetid);
            break;
          }
        }
        if(!similarclusterfound) {
          addNewCluster(round, word, tweetid);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  public void addNewCluster(long round, List<Integer> tweet, long tweetid) throws Exception {
    UUID clusterid = new UUID(4242L, 4242L);
    List<Object> values = new ArrayList<>();
    values.add(clusterid);
    values.add(round);
    values.add(tweet);
    cassandraDao.insertIntoClusters(values.toArray());
    values = new ArrayList<>();
    values.add(clusterid);
    values.add(tweetid);
    cassandraDao.insertIntoClusterAndTweets(values.toArray());
  }
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("key", "round", "blockEnd", "rounds", "country"));
  }

}
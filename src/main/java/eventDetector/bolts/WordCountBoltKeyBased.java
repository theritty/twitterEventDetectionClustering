package eventDetector.bolts;

import cassandraConnector.CassandraDaoKeyBased;
import eventDetector.drawing.ExcelWriterKeyBased;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import topologyBuilder.Constants;
import topologyBuilder.TopologyHelper;

import java.util.*;

public class WordCountBoltKeyBased extends BaseRichBolt {

    private OutputCollector collector;
    private HashMap<String, Long> countsForRounds = null;
    private long currentRound = 0;
    private int threshold;
    private long ignoredCount = 0;
    private int componentId;
    private String fileNum;
    private Date lastDate = new Date();
    private Date startDate = new Date();
    private CassandraDaoKeyBased cassandraDao;
    private int numDetector;
    private int firstDetectorId;
    private int detectorTask;
    private long lastRoundEnd = 0;


    public WordCountBoltKeyBased(int threshold, String filenum, String country, CassandraDaoKeyBased cassandraDao, int numDetector, int firstDetectorNum)
    {
        this.threshold = threshold;
        this.fileNum = filenum + "/";
        this.cassandraDao = cassandraDao;
        this.numDetector = numDetector;
        this.firstDetectorId = firstDetectorNum;
        this.detectorTask = firstDetectorNum;
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
        String word = tuple.getStringByField("word");
        long round = tuple.getLongByField("round");
        boolean blockend = tuple.getBooleanByField("blockEnd");

        TopologyHelper.writeToFile(Constants.WORKHISTORY_FILE + fileNum+ "workhistory.txt", new Date() + " Word count " + componentId + " working "  + round);
        if(blockend)
        {
            blockEndProcess(round);
            collector.ack(tuple);

            countsForRounds.clear();
            lastRoundEnd = round;
            currentRound = round;

            return;
        }
        else if(round < currentRound) {
            ignoredCount++;
            if(ignoredCount%1000==0)
                TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + "ignoreCount.txt",
                        "Word count ignored count " + componentId + ": " + ignoredCount );
            collector.ack(tuple);
            return;
        }

        Long count = countsForRounds.get(word);
        if (count == null) count = 0L;
        if(count>=threshold) {
            collector.ack(tuple);
            return;
        }
        else {
            processNewWord(word, ++count, round);
        }

        lastDate = new Date();
        collector.ack(tuple);
    }


    private void blockEndProcess(long round) {
        System.out.println("Receive blockend for " + round + ", bolt id " + componentId);
        try {
            List<Object> values = new ArrayList<>();
            values.add(round);
            values.add(componentId);
            values.add(true);
            cassandraDao.insertIntoProcessed(values.toArray());

            if(lastRoundEnd<round)
                for(int i=firstDetectorId;i<firstDetectorId+numDetector;i++)
                    this.collector.emitDirect(i, new Values("BLOCKEND", round, true));

        } catch (Exception e) {
            e.printStackTrace();
        }

        TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + currentRound + ".txt",
                "Word count "+ componentId + " end for round " + currentRound + " at " + lastDate);

        double diff = (lastDate.getTime()-startDate.getTime())/1000;
        if(diff==0.0) diff=1.0;
        TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + currentRound + ".txt",
                "Word count "+ componentId + " time taken for round" + currentRound + " is " + diff );
        if ( currentRound!=0)
            ExcelWriterKeyBased.putData(componentId,startDate,lastDate, currentRound);

        startDate = new Date();
        TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + round + ".txt",
                "Word count "+ componentId + " starting for round " + round + " at " + startDate );


    }

    private void processNewWord(String word, long count, long round) {

        if(count==threshold) {
            this.collector.emitDirect(detectorTask++, new Values(word, round, false));
            if(detectorTask==firstDetectorId+numDetector)
                detectorTask=firstDetectorId;
        }

        countsForRounds.put(word, count);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "round", "blockEnd"));
    }

}
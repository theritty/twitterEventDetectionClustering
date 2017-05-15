package eventDetector.bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import eventDetector.drawing.ExcelWriterKeyBased;
import topologyBuilder.Constants;
import topologyBuilder.TopologyHelper;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class WordCountBoltKeyBasedWithSleep extends BaseRichBolt {

    private OutputCollector collector;
    private HashMap<String, Long> countsForRounds = null;
    private long currentRound = 0;
    private int threshold;
    private long ignoredCount = 0;
    private int componentId;
    private String fileNum;
    private Date lastDate = new Date();
    private Date startDate = new Date();


    public WordCountBoltKeyBasedWithSleep(int threshold, String filenum)
    {
        this.threshold = threshold;
        this.fileNum = filenum + "/";
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

        if("dummyBLOCKdone".equals(word))
            this.collector.emit(new Values(word, round, false, tuple.getValueByField("dates"), tuple.getSourceStreamId()));

        TopologyHelper.writeToFile(Constants.WORKHISTORY_FILE + fileNum+ "workhistory.txt", new Date() + " Word count " + componentId + " working "  + round);
        if(round > currentRound)
        {
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
        Long count = countsForRounds.get(word);

        if (count == null) count = 1L;
        else count++;

        countsForRounds.put(word, count);

        if (count == threshold) {
            this.collector.emit(new Values(word, round, false, tuple.getValueByField("dates"), tuple.getSourceStreamId()));
        }
        lastDate = new Date();

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "round", "blockEnd", "rounds", "country"));
    }

}
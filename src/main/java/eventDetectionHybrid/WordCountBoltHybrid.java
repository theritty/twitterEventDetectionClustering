package eventDetectionHybrid;

import cassandraConnector.CassandraDaoHybrid;
import drawing.*;
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

public class WordCountBoltHybrid extends BaseRichBolt {

    private OutputCollector collector;
    private HashMap<String, Long> countsForRounds = null;
    private int threshold;
    private int componentId;
    private String fileNum;
    private CassandraDaoHybrid cassandraDao;
    private long lastRoundEnd = 0;


    public WordCountBoltHybrid(int threshold, String filenum, CassandraDaoHybrid cassandraDao)
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
        TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", "wc : " + componentId  );
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        long round = tuple.getLongByField("round");
        boolean blockend = tuple.getBooleanByField("blockEnd");

        TopologyHelper.writeToFile(Constants.WORKHISTORY_FILE + fileNum+ "workhistory.txt", new Date() + " Word count " + componentId + " working "  + round);
        Date nowDate = new Date();
        if(blockend)
        {
            markComponentAsFinishedInCassaandra(round);
            collector.ack(tuple);

            countsForRounds.clear();
            lastRoundEnd = round;
            ExcelWriter.putData(componentId,nowDate,new Date(), round, cassandraDao);
            return;
        }

        Long count = countsForRounds.get(word);
        if (count == null) count = 0L;
        if(count>=threshold) {
            collector.ack(tuple);
            ExcelWriter.putData(componentId,nowDate,new Date(), round, cassandraDao);
            return;
        }
        else {
            processNewWord(word, ++count, round);
        }

        collector.ack(tuple);
        ExcelWriter.putData(componentId,nowDate,new Date(), round, cassandraDao);
    }

    private void markComponentAsFinishedInCassaandra(long round) {
        System.out.println("Receive blockend for " + round + ", bolt id " + componentId);
        try {
            List<Object> values = new ArrayList<>();
            values.add(round);
            values.add(componentId);
            values.add(true);
            cassandraDao.insertIntoProcessed(values.toArray());

            if(lastRoundEnd<round) {
                TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", "wc blockEnd : " + componentId  );
                this.collector.emit(new Values("BLOCKEND", round, true, componentId));
            }
            else
                TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", "wc lastround<round: " + componentId  );


        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    private void processNewWord(String word, long count, long round) {

        if(count==threshold) {
            this.collector.emit(new Values(word, round, false, componentId));
        }

        countsForRounds.put(word, count);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "round", "blockEnd", "compId"));
    }

}
package eventDetectionHybrid;

import cassandraConnector.CassandraDaoHybrid;
import cassandraConnector.CassandraDaoKeyBased;
import algorithms.*;
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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class EventCandidateFinderHybrid extends BaseRichBolt {

    private OutputCollector collector;
    private String filePath;
    private double tfidfEventRate;
    private CassandraDaoHybrid cassandraDao;
    private int componentId;
    private String fileNum;
    private ArrayList<String> words;
    private int compareSize;
    private int numWordCountBolts;
    private ArrayList<Integer> numWordCountBoltsForRound;
    private String country;
    private long finalRound = 0;

    public EventCandidateFinderHybrid(CassandraDaoHybrid cassandraDao, String filePath, String fileNum, double tfidfEventRate, int compareSize, String country, int numWordCountBolts )
    {
        this.tfidfEventRate = tfidfEventRate;
        this.filePath = filePath + fileNum;
        this.cassandraDao = cassandraDao;
        this.fileNum = fileNum + "/";
        this.words = new ArrayList<>();
        this.compareSize = compareSize;
        this.country = country;
        this.numWordCountBolts = numWordCountBolts;
        this.numWordCountBoltsForRound = new ArrayList<>();
    }

    @Override
    public void prepare(Map config, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;

        this.componentId = context.getThisTaskId()-1;
        TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", "detector: " + componentId );
    }

    @Override
    public void execute(Tuple tuple) {
        String wrd = tuple.getStringByField("key");
        long round = tuple.getLongByField("round");
        boolean blockend = tuple.getBooleanByField("blockEnd");
        int comingCompId = tuple.getIntegerByField("compId");

        Date nowDate = new Date();
        if(!blockend) {
            words.add(wrd);
            collector.ack(tuple);

            ExcelWriter.putData(componentId,nowDate,new Date(), round, cassandraDao);
            return;
        }

        if(!numWordCountBoltsForRound.contains(comingCompId) && finalRound<round) numWordCountBoltsForRound.add(comingCompId);
        System.out.println("Detector " + componentId + " num: " + numWordCountBoltsForRound + " " + numWordCountBolts);
        if(numWordCountBolts == numWordCountBoltsForRound.size()) {

            for (String key : words) {
                isEventCandidate(key, round);
            }

            markComponentAsFinishedInCassaandra(round);
            endOfRoundOperations(round);

            collector.ack(tuple);
            words.clear();
            numWordCountBoltsForRound.clear();
            this.collector.emit(new Values("blockEnd", round, true, componentId));
        }
        ExcelWriter.putData(componentId,nowDate,new Date(), round, cassandraDao);

    }

    private void endOfRoundOperations(long round) {
        TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + round + ".txt",
                "Detector bolt " + componentId + " end of round " + round + " at " + new Date());
        finalRound = round;
    }

    private void markComponentAsFinishedInCassaandra(long round) {

        try {
            List<Object> values = new ArrayList<>();
            values.add(round);
            values.add(componentId-1);
            values.add(true);
            cassandraDao.insertIntoProcessed(values.toArray());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void isEventCandidate(String key, long round) {
        ArrayList<Double> tfidfs = new ArrayList<>();
        ArrayList<Long> rounds = new ArrayList<>();
        for (int i = compareSize-1; i >= 0; i--)
            rounds.add(round - i);

        for (long roundNum : rounds) {
            TFIDFCalculatorWithCassandra calculator = new TFIDFCalculatorWithCassandra();
            tfidfs.add(calculator.tfIdf(cassandraDao, rounds, key, roundNum, country));
        }
        if(tfidfs.size()<2) return;

        boolean allzero = true;
        for (double tfidf : tfidfs) {
            if (tfidf != 0.0) {
                allzero = false;
                break;
            }
        }

        try {
            if (!allzero) {
                TopologyHelper.writeToFile(filePath + "/tfidf-" + Long.toString(round) + "-" + country + ".txt",
                        "Key: " + key + ". Tf-idf values: " + tfidfs.toString());

                if (tfidfs.get(tfidfs.size() - 2) == 0) {
                    if (tfidfs.get(tfidfs.size() - 1) / 0.0001 > tfidfEventRate) {
                        cassandraDao.insertIntoEvents(round, country, key, tfidfs.get(tfidfs.size()-1) / tfidfs.get(tfidfs.size()-2));
                        this.collector.emit(new Values(key, round, false, componentId));
                    }
                } else if (tfidfs.get(tfidfs.size() - 1) / tfidfs.get(tfidfs.size() - 2) > tfidfEventRate) {
                    cassandraDao.insertIntoEvents(round, country, key, tfidfs.get(tfidfs.size()-1) / tfidfs.get(tfidfs.size()-2));
                    this.collector.emit(new Values(key, round, false, componentId));
                }

            } else {
                TopologyHelper.writeToFile(filePath + "/tfidf-" + Long.toString(round) + "-allzero-" + country + ".txt",
                        "Key: " + key);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields( "key", "round", "blockEnd", "compId"));
    }
}

package eventDetector.bolts;

import cassandraConnector.CassandraDaoKeyBased;
import eventDetector.algorithms.TFIDFCalculatorWithCassandraKeyBased;
import eventDetector.drawing.ExcelWriterClustering;
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

public class EventDetectorWithCassandraBoltKeyBased extends BaseRichBolt {

    private OutputCollector collector;
    private String filePath;
    private double tfidfEventRate;
    private CassandraDaoKeyBased cassandraDao;
    private int componentId;
    private String fileNum;
    private ArrayList<String> words;
    private int compareSize;
    private int numWordCountBolts;
    private ArrayList<Integer> numWordCountBoltsForRound;
    private String country;
    private long finalRound = 0;

    public EventDetectorWithCassandraBoltKeyBased(CassandraDaoKeyBased cassandraDao, String filePath, String fileNum, double tfidfEventRate, int compareSize, String country, int numWordCountBolts )
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
        System.out.println("detector: " + componentId );
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

            ExcelWriterClustering.putData(componentId,nowDate,new Date(), round, cassandraDao);
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
        }
        ExcelWriterClustering.putData(componentId,nowDate,new Date(), round, cassandraDao);

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
            TFIDFCalculatorWithCassandraKeyBased calculator = new TFIDFCalculatorWithCassandraKeyBased();
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

        if (!allzero) {
            TopologyHelper.writeToFile(filePath + "/tfidf-" + Long.toString(round) + "-" + country + ".txt",
                    "Key: " + key + ". Tf-idf values: " + tfidfs.toString());

            if (tfidfs.get(tfidfs.size() - 2) == 0) {
                if (tfidfs.get(tfidfs.size() - 1) / 0.0001 > tfidfEventRate) {
                    this.collector.emit(new Values(key, tfidfs, round, country));
                }
            } else if (tfidfs.get(tfidfs.size() - 1) / tfidfs.get(tfidfs.size() - 2) > tfidfEventRate) {
                this.collector.emit(new Values(key, tfidfs, round, country));
            }

        } else {
            TopologyHelper.writeToFile(filePath + "/tfidf-" + Long.toString(round) + "-allzero-" + country + ".txt",
                    "Key: " + key);
        }
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields( "key", "tfidfs", "round", "country"));
    }
}

package eventDetectionKeybasedWithSleep;

import cassandraConnector.CassandraDaoKeyBased;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import algorithms.*;
import drawing.*;
import topologyBuilder.Constants;
import topologyBuilder.TopologyHelper;

import java.util.*;

    public class EventDetectorWithCassandraBoltKeyBasedWithSleep extends BaseRichBolt {

        private OutputCollector collector;
        private String filePath;
        private double tfidfEventRate;
        private CassandraDaoKeyBased cassandraDao;
        private int componentId;
        private long currentRound = 0;
        private String fileNum;
        private Date lastDate = new Date();
        private Date startDate = new Date();
        private HashMap<Long, Long> ignores;
        private long ignoredCount = 0L;

        public EventDetectorWithCassandraBoltKeyBasedWithSleep(CassandraDaoKeyBased cassandraDao, String filePath, String fileNum, double tfidfEventRate )
        {
            this.tfidfEventRate = tfidfEventRate;
            this.filePath = filePath + fileNum;
            this.cassandraDao = cassandraDao;
            this.fileNum = fileNum + "/";
            this.ignores = new HashMap<>();
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

            ArrayList<Long> rounds = (ArrayList<Long>)tuple.getValueByField("rounds");
            String key = tuple.getStringByField("key");
            String country = (String) tuple.getValueByField( "country" );
            long round = tuple.getLongByField("round");

            if("dummyBLOCKdone".equals(key))
                this.collector.emit(new Values(key, new ArrayList<Double>(), round, country));

            TopologyHelper.writeToFile(Constants.WORKHISTORY_FILE + fileNum+ "workhistory.txt", new Date() +  " Detector " + componentId + " working " + round);

            ArrayList<Double> tfidfs = new ArrayList<>();
            if(currentRound < round) {
                TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + currentRound + ".txt",
                        "Detector bolt " + componentId + " end of round " + currentRound + " at " + lastDate);

                double diff = (lastDate.getTime()-startDate.getTime())/1000;
                if(diff==0.0) diff=1.0;
                TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + currentRound + ".txt",
                        "Word count "+ componentId + " time taken for round" + currentRound + " is " + diff);
//                if ( currentRound!=0)
//                    ExcelWriter.putData(componentId,startDate,lastDate, currentRound, cassandraDao);

                startDate = new Date();
                TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + round + ".txt",
                        "Detector bolt " + componentId + " start of round " + round + " at " + new Date());
                currentRound = round;
            }

            if(round < currentRound)
            {
                ignores.putIfAbsent(round, 0L);
                ignores.put(round,ignores.get(round)+1);

                ignoredCount++;
                TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + "ignoreCount.txt",
                        "Detector bolt Ignoring " + key + " from round " + round +
                                " while evaluating round " + currentRound + ". total ignore count: " + ignoredCount);

                for(long r:ignores.keySet())
                    TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + "ignoreCount.txt",
                            "Detector bolt Ignored count " + componentId + " : " + ignoredCount + " round " + r +
                                    " ignore count: " + ignores.get(r));

                TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + "ignoreCount.txt",
                        "---------------------------------------------------------------------------------");
                return;
            }

            Date nowDate = new Date();

            for (long roundNum: rounds)
            {
                TFIDFCalculatorWithCassandra calculator = new TFIDFCalculatorWithCassandra();
                tfidfs.add(calculator.tfIdf(cassandraDao, rounds,key,roundNum,country));
            }
            boolean allzero=true;
            for(double tfidf: tfidfs)
            {
                if(tfidf != 0.0)
                {
                    allzero=false;
                    break;
                }
            }

            if(!allzero) {
//            System.out.println("Tf idf calculated for " + key + " at round " + round+ " country " + country);
                TopologyHelper.writeToFile(filePath + "/tfidf-" + Long.toString(round) + "-" + country + ".txt",
                        "Key: " + key + ". Tf-idf values: " + tfidfs.toString());
                if(tfidfs.size()<2 || tfidfs.get(tfidfs.size()-2) == 0)
                {
                    if(tfidfs.get(tfidfs.size()-1)/0.0001>tfidfEventRate)
                    {
                        this.collector.emit(new Values(key, tfidfs, round, country));
                    }
                }
                else if(tfidfs.get(tfidfs.size()-1)/tfidfs.get(tfidfs.size()-2)>tfidfEventRate)
                {
                    this.collector.emit(new Values(key, tfidfs, round, country));
                }
            }
            else
            {
//            System.out.println("Tf idf all zero for " + key + " at round " + round+ " country " + country);
                TopologyHelper.writeToFile(filePath + "/tfidf-" + Long.toString(round)+"-allzero-" + country + ".txt",
                        "Key: " + key );
            }
            lastDate = new Date();
            ExcelWriter.putData(componentId,nowDate,lastDate, currentRound, cassandraDao);

        }




        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer)
        {
            declarer.declare(new Fields( "key", "tfidfs", "round", "country"));
        }
    }

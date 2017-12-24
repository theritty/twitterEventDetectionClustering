package eventDetectionKeybasedWithSleep;

import cassandraConnector.CassandraDaoKeyBased;
import drawing.*;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import algorithms.*;
import org.jfree.data.category.DefaultCategoryDataset;
import topologyBuilder.Constants;
import topologyBuilder.TopologyHelper;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class EventCompareBoltKeyBasedWithSleep extends BaseRichBolt {
    private String drawFilePath;
    private CassandraDaoKeyBased cassandraDao;
    private int componentId;
    private String fileNum;
    private long currentRound = 0;

    HashMap<Long, ArrayList<HashMap<String, Object>>> wordList;

    public EventCompareBoltKeyBasedWithSleep(CassandraDaoKeyBased cassandraDao, String fileNum)
    {
        this.drawFilePath = Constants.IMAGES_FILE_PATH + fileNum +"/";
        wordList = new HashMap<>();
        this.cassandraDao = cassandraDao;
        this.fileNum = fileNum +"/";
    }

    @Override
    public void prepare(Map config, TopologyContext context,
                        OutputCollector collector) {
        this.componentId = context.getThisTaskId()-1;
        TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", "compare: " + componentId );
    }

    @Override
    public void execute(Tuple tuple) {

        ArrayList<Double> tfidfs = (ArrayList<Double>) tuple.getValueByField("tfidfs");
        String key = tuple.getStringByField("key");
        long round = tuple.getLongByField("round");
        String country = tuple.getStringByField("country");

        if("dummyBLOCKdone".equals(key)) return;

        TopologyHelper.writeToFile(Constants.WORKHISTORY_FILE + fileNum+ "workhistory.txt", new Date() + " Compare " + componentId + " working " + round);

        Date nowDate = new Date();

        if(tfidfs.size()<2) return;

        System.out.println(new Date() + ": Event found => " + key + " at round " + round  + " for " + country + " ");
        if(tfidfs.get(tfidfs.size()-2)==0) tfidfs.set(tfidfs.size()-2, 0.0001);
        try {
            cassandraDao.insertIntoEvents(round, country, key, tfidfs.get(tfidfs.size()-1) / tfidfs.get(tfidfs.size()-2));

            DefaultCategoryDataset countsList = getCountListFromCass(round, key, country);
            LineChart.drawLineChart(countsList,key,round,country, drawFilePath);
        } catch (Exception e) {
            e.printStackTrace();
        }

        ExcelWriter.putData(componentId,nowDate,new Date(), currentRound, cassandraDao);

    }

    protected DefaultCategoryDataset getCountListFromCass(long round, String key, String country) throws Exception {
        long roundPast = round-9;

        DefaultCategoryDataset countsList = new DefaultCategoryDataset( );
        DateFormat df = new SimpleDateFormat("dd.MM.yyyy HH:mm");
        while(roundPast<=round) {
            ResultSet resultSet = cassandraDao.getFromCounts(roundPast, key, country);
            Iterator<Row> iterator = resultSet.iterator();
            if (iterator.hasNext()) {
                Row row = iterator.next();
                countsList.addValue(row.getLong("count"), "counts", df.format(new Date(new Long(roundPast) * 6*60*1000)));
            }
            else {
                CountCalculatorKeyBased c = new CountCalculatorKeyBased();
                long ct = c.addNewEntryToCassCounts(cassandraDao, roundPast, key, country).get("count");
                countsList.addValue(ct, "counts", df.format(new Date(new Long(roundPast) * 6 * 60 * 1000)));
            }
            roundPast+=2;
        }
        return countsList;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
    }
}
package eventDetector.algorithms;

import cassandraConnector.CassandraDao;
import cassandraConnector.CassandraDaoKeyBased;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class CountCalculatorKeyBased {

    public HashMap<String, Long> addNewEntryToCassCounts(CassandraDaoKeyBased cassandraDao, long round, String word, String country)
    {
        long count=0L, allcount=0L;
        HashMap<String, Long> counts = new HashMap<>();
        ResultSet resultSet2 ;
        try {
            resultSet2 = cassandraDao.getTweetsByRoundAndCountry(round, country);
            Iterator<Row> iterator2 = resultSet2.iterator();

            while(iterator2.hasNext())
            {
                Row row = iterator2.next();
                String tweet = row.getString("tweet");
                if(tweet == null || !row.getString("country").equals(country)) continue;
                String[] splittedList = tweet.split(" ");
                for(String s : splittedList) {
                    allcount++;
                    if ((s != null || s.length() > 0) && s.equals(word) ) {
                        count++;
                    }
                }
            }
            counts.put("count", count);
            counts.put("allcount", allcount);
            insertValuesToCass(cassandraDao, round, word, country, count, allcount);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return counts;
    }

    public HashMap<String, Double> getCountOfWord(CassandraDaoKeyBased cassandraDao, String word, long round, String country) {
        double count, allcount;
        HashMap<String, Double> hm = null;
        try {
            ResultSet resultSet =cassandraDao.getFromCounts(round, word, country);

            Iterator<Row> iterator = resultSet.iterator();
            if (!iterator.hasNext()) {
                HashMap<String, Long> tmp = addNewEntryToCassCounts(cassandraDao, round, word, country);
                count = tmp.get("count");
                allcount = tmp.get("allcount");
            }
            else{
                Row row = iterator.next();
                if(row.getLong("count")<0 || row.getLong("totalnumofwords")<0 )
                {
                    HashMap<String, Long> tmp = addNewEntryToCassCounts(cassandraDao, round, word, country);
                    count = tmp.get("count");
                    allcount = tmp.get("allcount");
                }
                else {
                    count = row.getLong("count");
                    allcount = row.getLong("totalnumofwords");
                }
            }

            hm = new HashMap<>();
            hm.put("count", count);
            hm.put("totalnumofwords", allcount);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return hm;
    }

    private void insertValuesToCass(CassandraDaoKeyBased cassandraDao, long round, String word, String country, long count, long allcount)
    {
        try {
            List<Object> values = new ArrayList<>();
            values.add(round);
            values.add(word);
            values.add(country);
            values.add(count);
            values.add(allcount);
            cassandraDao.insertIntoCounts(values.toArray());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

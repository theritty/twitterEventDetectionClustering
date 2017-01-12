package eventDetector.algorithms;

import cassandraConnector.CassandraDao;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import java.util.*;

public class MatchEvents {
  public static HashMap<String,ArrayList<Integer>> prepareEventVectors(ArrayList<String> events, long round) throws Exception {
    CassandraDao cassandraDao = new CassandraDao("tweets", "counts8", "events8");
    HashMap<String,ArrayList<Integer>> eventGroups = new HashMap<>();

    for(String event:events) {
      ArrayList<Integer> tmp = new ArrayList<>();
      eventGroups.put(event,tmp);
    }

    ResultSet resultSet = cassandraDao.getTweetsByRound(round);
    Iterator<Row> iterator = resultSet.iterator();
    while(iterator.hasNext())
    {
      Row row = iterator.next();
      for(String eve:events) {
        ArrayList<Integer> tmp = eventGroups.get(eve);
        if (row.getString("tweet").contains(eve))  tmp.add(1);
        else tmp.add(0);
        eventGroups.put(eve,tmp);
      }
    }
    return eventGroups;

  }

  public static ArrayList<ArrayList<String>> groupEvents(ArrayList<String> events, long round, String country) throws Exception {

    HashMap<String,ArrayList<Integer>> eventGroups=prepareEventVectors(events,round);
    ArrayList<ArrayList<String>> grouping=new ArrayList<>();


    for (int i=0;i<events.size()-1;i++)
    {
      boolean foundoverall=false;
      String events1=events.get(i);
      int place=0;

      ArrayList<String> tmp = new ArrayList<>();
      for(ArrayList<String> list : grouping)
      {
        boolean found = false;
        for(String ev:list)
        {
          if(ev.equals(events1)) { found=true;break; }
        }
        if(found){ foundoverall=true; break; }
        place++;
      }
      if(!foundoverall) { tmp.add(events1); grouping.add(tmp); }

      for (int j=i+1;j<events.size();j++)
      {
        String events2=events.get(j);
        double sim = CosineSimilarity.cosineSimilarity(eventGroups.get(events1), eventGroups.get(events2));
        if(sim>0.1)  grouping.get(place).add(events2);
      }
    }

    for(ArrayList<String> list : grouping) {
      for (int i=0;i<list.size()-1;i++) {
        for(int j=i+1;j<list.size();j++){
          if(list.get(i).equals(list.get(j))) list.remove(j--);
        }
      }
    }
    return grouping;
  }

  public static void main(String[] args) throws Exception {

  }
}
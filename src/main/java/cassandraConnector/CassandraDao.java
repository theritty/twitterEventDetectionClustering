package cassandraConnector;

import com.datastax.driver.core.*;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import topologyBuilder.Constants;
import topologyBuilder.TopologyHelper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CassandraDao implements Serializable
{
    private transient PreparedStatement statement_cluster;
    private transient PreparedStatement statement_cluster_delete;
    private transient PreparedStatement statement_cluster_get_with_lastround;
    private transient PreparedStatement statement_processtimes;
    private transient PreparedStatement statement_processtimes_get;
    private transient PreparedStatement statement_event;
    private transient PreparedStatement statement_event_get;
    private transient PreparedStatement statement_event_wordbased;
    private transient PreparedStatement statement_cluster_get;
    private transient PreparedStatement statement_cluster_get_by_id;
    private transient PreparedStatement statement_tweet_get;
    private transient PreparedStatement statement_tweet_getbyid;
    private transient PreparedStatement statement_round_get;
    private transient PreparedStatement statement_processedTweets_get;
    private transient PreparedStatement statement_processedTweets_getCountry;
    private transient PreparedStatement statement_processedTweets_getAll;
    private transient PreparedStatement statement_processedTweets;
    private transient PreparedStatement statement_tweetsandcluster;
    private transient PreparedStatement statement_clustertweets;
    private transient BoundStatement boundStatement_tweets_get;
    private transient BoundStatement boundStatement_tweets_getbyid;
    private transient BoundStatement boundStatement_processedTweets_get;
    private transient BoundStatement boundStatement_processedTweets_getCountry;
    private transient BoundStatement boundStatement_processedTweets_getAll;
    private transient BoundStatement boundStatement_processedTweets;
    private transient BoundStatement boundStatement_rounds_get;
    private transient BoundStatement boundStatement_cluster;
    private transient BoundStatement boundStatement_cluster_delete;
    private transient BoundStatement boundStatement_cluster_get_with_lastround;
    private transient BoundStatement boundStatement_event;
    private transient BoundStatement boundStatement_event_get;
    private transient BoundStatement boundStatement_processtimes;
    private transient BoundStatement boundStatement_processtimes_get;
    private transient BoundStatement boundStatement_event_wordBased;
    private transient BoundStatement boundStatement_cluster_get;
    private transient BoundStatement boundStatement_cluster_get_by_id;
    private transient BoundStatement boundStatement_tweetsandcluster;
    private transient BoundStatement boundStatement_clustertweets;


    private static String CLUSTER_FIELDS =   "(id, country, cosinevector, prevnumtweets, currentnumtweets, lastround)";
    private static String CLUSTER_VALUES = "(?, ?, ?, ?, ?, ?)";

    private static String PROCESSTIMES_FIELDS =   "(row,column,id)";
    private static String PROCESSTIMES_VALUES = "(?, ?, ?)";

    private static String EVENT_FIELDS =   "(round, clusterid, country, cosinevector, incrementrate, numtweet)";
    private static String EVENT_VALUES = "(?, ?, ?, ?, ?, ?)";

//    private static String EVENTS_WORDBASED_FIELDS = "(round, country, word, incrementpercent)";
//    private static String EVENTS_VALUES = "(?, ?, ?, ?)";

    private static String PROCESSED_FIELDS =   "(round, boltid, spoutSent, boltProcessed, finished, country)";
    private static String PROCESSED_VALUES = "(?, ?, ?, ?, ?, ?)";

    private static String TWEETSANDCLUSTER_FIELDS =   "(round, clusterid, tweetid)";
    private static String TWEETSANDCLUSTER_VALUES = "(?, ?, ?)";

    private String tweetsTable;
    private String processTimesTable;
    private String clusterTable;
    private String eventTable;
    private String eventWordBasedTable;
    private String processedTweetsTable;
    private String tweetsandclusterTable;

    public CassandraDao(String tweetsTable, String clusterTable, String eventTable, String eventWordBasedTable, String processedTweetsTable, String processTimesTable, String tweetsandclusterTable) throws Exception {
        this.tweetsTable = tweetsTable;
        this.clusterTable = clusterTable;
        this.eventTable = eventTable;
        this.eventWordBasedTable = eventWordBasedTable;
        this.processedTweetsTable = processedTweetsTable;
        this.processTimesTable = processTimesTable;
        this.tweetsandclusterTable = tweetsandclusterTable;

        prepareAll();
    }

    private void prepareAll()
    {
        if(statement_tweetsandcluster==null) {
            statement_tweetsandcluster = CassandraConnection.connect().prepare(
                    "INSERT INTO " + tweetsandclusterTable + " " + TWEETSANDCLUSTER_FIELDS
                            + " VALUES " + TWEETSANDCLUSTER_VALUES + ";");
        }
        if(statement_event==null) {
            statement_event = CassandraConnection.connect().prepare(
                    "INSERT INTO " + eventTable + " " + EVENT_FIELDS
                            + " VALUES " + EVENT_VALUES + ";");
        }
        if(statement_processtimes==null) {
            statement_processtimes = CassandraConnection.connect().prepare(
                    "INSERT INTO " + processTimesTable + " " + PROCESSTIMES_FIELDS
                            + " VALUES " + PROCESSTIMES_VALUES + ";");
        }
        if(statement_processedTweets==null) {
            statement_processedTweets = CassandraConnection.connect().prepare(
                    "INSERT INTO " + processedTweetsTable + " " + PROCESSED_FIELDS
                            + " VALUES " + PROCESSED_VALUES + ";");
        }
        if(statement_cluster==null) {
            statement_cluster = CassandraConnection.connect().prepare(
                    "INSERT INTO " + clusterTable + " " + CLUSTER_FIELDS
                            + " VALUES " + CLUSTER_VALUES + ";");
        }

        if(statement_cluster_delete==null) {
            statement_cluster_delete = CassandraConnection.connect().prepare(
                    "DELETE FROM " + clusterTable + " WHERE country=? AND id=?;");
        }

//        if(statement_clusterinfo==null) {
//            statement_clusterinfo = CassandraConnection.connect().prepare(
//                    "INSERT INTO " + clusterinfoTable + " " + CLUSTERINFO_FIELDS
//                            + " VALUES " + CLUSTERINFO_VALUES + ";");
//        }

        if(statement_event_wordbased==null) {
            statement_event_wordbased = CassandraConnection.connect().prepare(
                    "SELECT * FROM " + eventWordBasedTable );
        }

        if(statement_processtimes_get==null) {
            statement_processtimes_get = CassandraConnection.connect().prepare(
                    "SELECT * FROM " + processTimesTable );
        }

        if(statement_processedTweets_get==null) {
            statement_processedTweets_get = CassandraConnection.connect().prepare(
                    "SELECT * FROM " + processedTweetsTable + " WHERE round=? AND boltid=?;" );
        }

        if(statement_processedTweets_getCountry==null) {
            statement_processedTweets_getCountry = CassandraConnection.connect().prepare(
                    "SELECT * FROM " + processedTweetsTable + " WHERE round=? AND country=? ALLOW FILTERING;" );
        }

        if(statement_processedTweets_getAll==null) {
            statement_processedTweets_getAll = CassandraConnection.connect().prepare(
                    "SELECT * FROM " + processedTweetsTable + " WHERE round=? ;" );
        }

        if(statement_tweet_get==null) {
            statement_tweet_get = CassandraConnection.connect().prepare(
                    "SELECT * FROM " + tweetsTable + " WHERE round=?;");
        }

        if(statement_tweet_getbyid==null) {
            statement_tweet_getbyid = CassandraConnection.connect().prepare(
                    "SELECT * FROM " + tweetsTable + " WHERE round=? AND country=? AND id=? ALLOW FILTERING;");
        }

        if(statement_event_get==null) {
            statement_event_get = CassandraConnection.connect().prepare(
                    "SELECT * FROM " + eventTable + " WHERE country=? ALLOW FILTERING;");
        }
        if(statement_round_get==null) {
            statement_round_get = CassandraConnection.connect().prepare(
                    "SELECT DISTINCT round FROM " + tweetsTable + ";");
        }
        if(statement_cluster_get==null) {
            statement_cluster_get = CassandraConnection.connect().prepare(
                    "SELECT * FROM " + clusterTable + " WHERE country=?;");
        }
        if(statement_cluster_get_with_lastround==null) {
            statement_cluster_get_with_lastround = CassandraConnection.connect().prepare(
                    "SELECT * FROM " + clusterTable + " WHERE lastround<? ALLOW FILTERING;");
        }

        if(statement_cluster_get_by_id ==null) {
            statement_cluster_get_by_id = CassandraConnection.connect().prepare(
                    "SELECT * FROM " + clusterTable + " WHERE country=? AND id=?;");
        }
        if(statement_clustertweets ==null) {
            statement_clustertweets = CassandraConnection.connect().prepare(
//                    "SELECT * FROM " + tweetsandclusterTable + " WHERE round=? AND clusterid=? ALLOW FILTERING;");
                    "SELECT * FROM " + tweetsandclusterTable + " WHERE round=?;");
        }


        if(boundStatement_tweetsandcluster == null)
            boundStatement_tweetsandcluster = new BoundStatement(statement_tweetsandcluster);
        if(boundStatement_tweets_get == null)
            boundStatement_tweets_get = new BoundStatement(statement_tweet_get);
        if(boundStatement_tweets_getbyid == null)
            boundStatement_tweets_getbyid = new BoundStatement(statement_tweet_getbyid);
        if(boundStatement_processtimes_get == null)
            boundStatement_processtimes_get = new BoundStatement(statement_processtimes_get);
        if(boundStatement_processtimes == null)
            boundStatement_processtimes = new BoundStatement(statement_processtimes);
        if(boundStatement_processedTweets_get == null)
            boundStatement_processedTweets_get = new BoundStatement(statement_processedTweets_get);
        if(boundStatement_processedTweets_getCountry == null)
            boundStatement_processedTweets_getCountry = new BoundStatement(statement_processedTweets_getCountry);
        if(boundStatement_processedTweets_getAll == null)
            boundStatement_processedTweets_getAll = new BoundStatement(statement_processedTweets_getAll);
        if(boundStatement_processedTweets == null)
            boundStatement_processedTweets = new BoundStatement(statement_processedTweets);
        if(boundStatement_rounds_get == null)
            boundStatement_rounds_get = new BoundStatement(statement_round_get);
        if(boundStatement_cluster_get == null)
            boundStatement_cluster_get = new BoundStatement(statement_cluster_get);
        if(boundStatement_cluster_get_with_lastround == null)
            boundStatement_cluster_get_with_lastround = new BoundStatement(statement_cluster_get_with_lastround);
        if(boundStatement_cluster_get_by_id == null)
            boundStatement_cluster_get_by_id = new BoundStatement(statement_cluster_get_by_id);
        if(boundStatement_cluster == null)
            boundStatement_cluster = new BoundStatement(statement_cluster);
        if(boundStatement_cluster_delete == null)
            boundStatement_cluster_delete = new BoundStatement(statement_cluster_delete);
        if(boundStatement_event == null)
            boundStatement_event = new BoundStatement(statement_event);
        if(boundStatement_event_get == null)
            boundStatement_event_get = new BoundStatement(statement_event_get);
        if(boundStatement_event_wordBased == null)
            boundStatement_event_wordBased = new BoundStatement(statement_event_wordbased);
        if(boundStatement_clustertweets == null)
            boundStatement_clustertweets = new BoundStatement(statement_clustertweets);
    }

    public void insertIntoTweetsAndCluster( Object[] values ) throws Exception
    {
        prepareAll();
        CassandraConnection.connect().execute(boundStatement_tweetsandcluster.bind(values));
    }

    public void insertIntoProcessed( Object[] values ) throws Exception
    {
        prepareAll();
        CassandraConnection.connect().execute(boundStatement_processedTweets.bind(values));
    }
    public void insertIntoClusters( Object[] values ) throws Exception
    {
        prepareAll();
        CassandraConnection.connect().executeAsync(boundStatement_cluster.bind(values));
    }
    public void insertIntoProcessTimes( Object[] values ) throws Exception
    {
        prepareAll();
        CassandraConnection.connect().executeAsync(boundStatement_processtimes.bind(values));
    }
    public void deleteFromClusters( Object... values ) throws Exception
    {
        prepareAll();
        CassandraConnection.connect().execute(boundStatement_cluster_delete.bind(values));
    }

    public void insertIntoEvents( Object[] values ) throws Exception
    {
        prepareAll();
        CassandraConnection.connect().executeAsync(boundStatement_event.bind(values));
    }

    public ResultSet getTweetsByRound( Object... values ) throws Exception
    {
        prepareAll();
        ResultSet resultSet = CassandraConnection.connect().execute(boundStatement_tweets_get.bind(values));

        return resultSet;
    }
    public ResultSet getTweetsById( Object... values ) throws Exception
    {
        prepareAll();
        ResultSet resultSet = CassandraConnection.connect().execute(boundStatement_tweets_getbyid.bind(values));

        return resultSet;
    }
    public ResultSet getProcessTimes(  ) throws Exception
    {
        prepareAll();
        ResultSet resultSet = CassandraConnection.connect().execute(boundStatement_processtimes_get.bind());

        return resultSet;
    }

    public ResultSet getProcessed( Object... values ) throws Exception
    {
        prepareAll();
        ResultSet resultSet = CassandraConnection.connect().execute(boundStatement_processedTweets_get.bind(values));

        return resultSet;
    }

    public ResultSet getAllProcessed( Object... values ) throws Exception
    {
        prepareAll();
        ResultSet resultSet = CassandraConnection.connect().execute(boundStatement_processedTweets_getAll.bind(values));

        return resultSet;
    }

    public ResultSet getClusters( Object... values ) throws Exception
    {
        prepareAll();
        ResultSet resultSet = CassandraConnection.connect().execute(boundStatement_cluster_get.bind(values));

        return resultSet;
    }

    public ResultSet getEvents( Object... values ) throws Exception
    {
        prepareAll();
        ResultSet resultSet = CassandraConnection.connect().execute(boundStatement_event_get.bind(values));

        return resultSet;
    }

    public ResultSet getEventsWordBased( ) throws Exception
    {
        prepareAll();
        ResultSet resultSet = CassandraConnection.connect().execute(boundStatement_event_wordBased.bind());

        return resultSet;
    }


    public ResultSet getRounds() throws Exception
    {
        prepareAll();
        ResultSet resultSet = CassandraConnection.connect().execute(boundStatement_rounds_get.bind());

        return resultSet;
    }


    public ResultSet getClusterTweets( Object... values ) throws Exception {
        prepareAll();
        ResultSet resultSet = CassandraConnection.connect().execute(boundStatement_clustertweets.bind(values));

        return resultSet;

    }


    public void updateClusterTweets(long round, String oldClusterId, String newClusterId) throws Exception
    {
        ResultSet resultSet = getClusterTweets(round);
        Iterator<Row> iterator = resultSet.iterator();
        TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH  + "experiment-4methods-clustering-weekData-db-eval/sout.txt", "Old cluster id: " + oldClusterId + ", new clusterid: " + newClusterId );

        while (iterator.hasNext()) {
            Row row = iterator.next();
            if (row.getString("clusterid").equals(oldClusterId)) {
                List<Object> values_event = new ArrayList<>();
                values_event.add(round);
                values_event.add(newClusterId);
                values_event.add(row.getLong("tweetid"));
                insertIntoTweetsAndCluster(values_event.toArray());
            }
        }
    }

    static FutureCallback<ResultSet> callback =  new FutureCallback<ResultSet>() {
        @Override public void onSuccess(ResultSet result) {
        }

        @Override public void onFailure(Throwable t) {
            System.err.println("Error while reading Cassandra version: " + t.getMessage());
        }
    };

    public void checkError(ResultSetFuture future)
    {
        Futures.addCallback(future, callback, MoreExecutors.directExecutor());
    }

}


package cassandraConnector;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;

import java.io.Serializable;

public class CassandraDao implements Serializable
{
    private transient PreparedStatement statement_cluster;
    private transient PreparedStatement statement_cluster_delete;
    private transient PreparedStatement statement_cluster_get_with_lastround;
    private transient PreparedStatement statement_event;
    private transient PreparedStatement statement_event_get;
    private transient PreparedStatement statement_event_wordbased;
    private transient PreparedStatement statement_clusterinfo;
    private transient PreparedStatement statement_clusterandtweet;
    private transient PreparedStatement statement_cluster_get;
    private transient PreparedStatement statement_cluster_get_by_id;
    private transient PreparedStatement statement_clusterinfo_get;
    private transient PreparedStatement statement_clusterinfo_id_get;
    private transient PreparedStatement statement_clusterandtweet_get;
    private transient PreparedStatement statement_tweet_get;
    private transient PreparedStatement statement_round_get;
    private transient BoundStatement boundStatement_tweets_get;
    private transient BoundStatement boundStatement_rounds_get;
    private transient BoundStatement boundStatement_cluster;
    private transient BoundStatement boundStatement_cluster_delete;
    private transient BoundStatement boundStatement_cluster_get_with_lastround;
    private transient BoundStatement boundStatement_event;
    private transient BoundStatement boundStatement_event_get;
    private transient BoundStatement boundStatement_event_wordBased;
    private transient BoundStatement boundStatement_clusterinfo;
    private transient BoundStatement boundStatement_clusterandtweets;
    private transient BoundStatement boundStatement_cluster_get;
    private transient BoundStatement boundStatement_cluster_get_by_id;
    private transient BoundStatement boundStatement_clusterinfo_get;
    private transient BoundStatement boundStatement_clusterinfo_id_get;
    private transient BoundStatement boundStatement_clusterandtweets_get;

    private static String CLUSTER_FIELDS =   "(id, country, cosinevector, numberoftweets, lastround)";
    private static String CLUSTER_VALUES = "(?, ?, ?, ?, ?)";

    private static String EVENT_FIELDS =   "(round, clusterid, country, cosinevector, incrementrate, numtweet)";
    private static String EVENT_VALUES = "(?, ?, ?, ?, ?, ?)";

    private static String EVENTS_WORDBASED_FIELDS = "(round, country, word, incrementpercent)";
    private static String EVENTS_VALUES = "(?, ?, ?, ?)";
    
    private static String CLUSTERINFO_FIELDS =   "(round, id, country, numberoftweets)";
    private static String CLUSTERINFO_VALUES = "(?, ?, ?, ?)";

    private static String CLUSTERANDTWEETS_FIELDS =   "(clusterid, tweetid)";
    private static String CLUSTERANDTWEETS_VALUES = "(?, ?)";

    private String tweetsTable;
    private String clusterTable;
    private String eventTable;
    private String eventWordBasedTable;
    private String clusterinfoTable;
    private String clusterandtweetTable;

    public CassandraDao(String tweetsTable, String clusterTable, String clusterinfoTable, String clusterandtweetTable, String eventTable, String eventWordBasedTable) throws Exception {
        this.tweetsTable = tweetsTable;
        this.clusterTable = clusterTable;
        this.clusterinfoTable = clusterinfoTable;
        this.clusterandtweetTable = clusterandtweetTable;
        this.eventTable = eventTable;
        this.eventWordBasedTable = eventWordBasedTable;

        prepareAll();
    }

    private void prepareAll()
    {
        if(statement_event==null) {
            statement_event = CassandraConnection.connect().prepare(
                    "INSERT INTO " + eventTable + " " + EVENT_FIELDS
                            + " VALUES " + EVENT_VALUES + ";");
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

        if(statement_clusterinfo==null) {
            statement_clusterinfo = CassandraConnection.connect().prepare(
                    "INSERT INTO " + clusterinfoTable + " " + CLUSTERINFO_FIELDS
                            + " VALUES " + CLUSTERINFO_VALUES + ";");
        }

        if(statement_clusterandtweet==null) {
            statement_clusterandtweet = CassandraConnection.connect().prepare(
                    "INSERT INTO " + clusterandtweetTable + " " + CLUSTERANDTWEETS_FIELDS
                            + " VALUES " + CLUSTERANDTWEETS_VALUES + ";");
        }

        if(statement_event_wordbased==null) {
            statement_event_wordbased = CassandraConnection.connect().prepare(
                    "SELECT * FROM " + eventWordBasedTable );
        }

        if(statement_tweet_get==null) {
            statement_tweet_get = CassandraConnection.connect().prepare(
                    "SELECT * FROM " + tweetsTable + " WHERE round=?;");
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
        if(statement_clusterinfo_get==null) {
            statement_clusterinfo_get = CassandraConnection.connect().prepare(
                    "SELECT * FROM " + clusterinfoTable + " WHERE round=? AND country=?;");
        }
        if(statement_clusterinfo_id_get==null) {
            statement_clusterinfo_id_get = CassandraConnection.connect().prepare(
                    "SELECT * FROM " + clusterinfoTable + " WHERE round=? AND country=? AND id=?;");
        }
        if(statement_clusterandtweet_get==null) {
            statement_clusterandtweet_get = CassandraConnection.connect().prepare(
                    "SELECT * FROM " + clusterandtweetTable + " WHERE clusterid=?;");
        }

        if(boundStatement_tweets_get == null)
            boundStatement_tweets_get = new BoundStatement(statement_tweet_get);
        if(boundStatement_rounds_get == null)
            boundStatement_rounds_get = new BoundStatement(statement_round_get);
        if(boundStatement_cluster_get == null)
            boundStatement_cluster_get = new BoundStatement(statement_cluster_get);
        if(boundStatement_cluster_get_with_lastround == null)
            boundStatement_cluster_get_with_lastround = new BoundStatement(statement_cluster_get_with_lastround);
        if(boundStatement_cluster_get_by_id == null)
            boundStatement_cluster_get_by_id = new BoundStatement(statement_cluster_get_by_id);
        if(boundStatement_clusterinfo_get == null)
            boundStatement_clusterinfo_get = new BoundStatement(statement_clusterinfo_get);
        if(boundStatement_clusterinfo_id_get == null)
            boundStatement_clusterinfo_id_get = new BoundStatement(statement_clusterinfo_id_get);
        if(boundStatement_clusterandtweets_get == null)
            boundStatement_clusterandtweets_get = new BoundStatement(statement_clusterandtweet_get);
        if(boundStatement_cluster == null)
            boundStatement_cluster = new BoundStatement(statement_cluster);
        if(boundStatement_cluster_delete == null)
            boundStatement_cluster_delete = new BoundStatement(statement_cluster_delete);
        if(boundStatement_clusterinfo == null)
            boundStatement_clusterinfo = new BoundStatement(statement_clusterinfo);
        if(boundStatement_clusterandtweets == null)
            boundStatement_clusterandtweets = new BoundStatement(statement_clusterandtweet);
        if(boundStatement_event == null)
            boundStatement_event = new BoundStatement(statement_event);
        if(boundStatement_event_get == null)
            boundStatement_event_get = new BoundStatement(statement_event_get);
        if(boundStatement_event_wordBased == null)
            boundStatement_event_wordBased = new BoundStatement(statement_event_wordbased);
    }

    public void insertIntoClusters( Object[] values ) throws Exception
    {
        prepareAll();
        CassandraConnection.connect().executeAsync(boundStatement_cluster.bind(values));
    }
    public void deleteFromClusters( Object... values ) throws Exception
    {
        prepareAll();
        CassandraConnection.connect().executeAsync(boundStatement_cluster_delete.bind(values));
    }

    public void insertIntoEvents( Object[] values ) throws Exception
    {
        prepareAll();
        CassandraConnection.connect().executeAsync(boundStatement_event.bind(values));
    }

    public void insertIntoClusterinfo( Object[] values ) throws Exception
    {
        prepareAll();
        CassandraConnection.connect().executeAsync(boundStatement_clusterinfo.bind(values));
    }

    public void insertIntoClusterAndTweets( Object[] values ) throws Exception
    {
        prepareAll();
        CassandraConnection.connect().executeAsync(boundStatement_clusterandtweets.bind(values));
    }

    public ResultSet getTweetsByRound( Object... values ) throws Exception
    {
        prepareAll();
        ResultSet resultSet = CassandraConnection.connect().execute(boundStatement_tweets_get.bind(values));

        return resultSet;
    }

    public ResultSet getClusterinfoByRound( Object... values ) throws Exception
    {
        prepareAll();
        ResultSet resultSet = CassandraConnection.connect().execute(boundStatement_clusterinfo_get.bind(values));

        return resultSet;
    }

    public ResultSet getClusterByLastRound( Object... values ) throws Exception
    {
        prepareAll();
        ResultSet resultSet = CassandraConnection.connect().execute(boundStatement_cluster_get_with_lastround.bind(values));

        return resultSet;
    }

    public ResultSet getClusterinfoByRoundAndId( Object... values ) throws Exception
    {
        prepareAll();
        ResultSet resultSet = CassandraConnection.connect().execute(boundStatement_clusterinfo_id_get.bind(values));

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

    public ResultSet getClustersById(Object... values) throws Exception
    {
        prepareAll();
        ResultSet resultSet = CassandraConnection.connect().execute(boundStatement_cluster_get_by_id.bind(values));

        return resultSet;
    }

    public ResultSet getTweetsOfCluster( Object... values ) throws Exception
    {
        prepareAll();
        ResultSet resultSet = CassandraConnection.connect().execute(boundStatement_clusterandtweets_get.bind(values));

        return resultSet;
    }

    public ResultSet getRounds() throws Exception
    {
        prepareAll();
        ResultSet resultSet = CassandraConnection.connect().execute(boundStatement_rounds_get.bind());

        return resultSet;
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


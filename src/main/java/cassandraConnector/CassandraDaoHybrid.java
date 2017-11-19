package cassandraConnector;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;

import java.io.Serializable;

public class CassandraDaoHybrid implements Serializable
{
    private transient PreparedStatement statement_tweets;
    private transient PreparedStatement statement_counts;
    private transient PreparedStatement statement_events;
    private transient PreparedStatement statement_events_keybased;
    private transient PreparedStatement statement_where;
    private transient PreparedStatement statement_tweet_get;
    private transient PreparedStatement statement_tweet_by_country_get;
    private transient PreparedStatement statement_round_get;
    private transient PreparedStatement statement_events_get;
    private transient PreparedStatement statement_processed2Tweets_get;
    private transient PreparedStatement statement_processed2Tweets_getAll;
    private transient PreparedStatement statement_processed2Tweets;
    private transient PreparedStatement statement_processtimes;
    private transient PreparedStatement statement_processtimes_get;
    private transient PreparedStatement statement_cluster_get;
    private transient PreparedStatement statement_cluster_get_by_id;
    private transient PreparedStatement statement_cluster;
    private transient PreparedStatement statement_cluster_delete;
    private transient PreparedStatement statement_cluster_get_with_lastround;
    private transient PreparedStatement statement_tweetsandcluster;
    private transient PreparedStatement statement_clustertweets;

    private transient BoundStatement boundStatement_processtimes;
    private transient BoundStatement boundStatement_processtimes_get;
    private transient BoundStatement boundStatement_processed2Tweets_get;
    private transient BoundStatement boundStatement_processed2Tweets_getAll;
    private transient BoundStatement boundStatement_processed2Tweets;
    private transient BoundStatement boundStatement_events;
    private transient BoundStatement boundStatement_events_keybased;
    private transient BoundStatement boundStatement_events_get;
    private transient BoundStatement boundStatement_tweets;
    private transient BoundStatement boundStatement_tweets_by_country_get;
    private transient BoundStatement boundStatement_tweets_get;
    private transient BoundStatement boundStatement_rounds_get;
    private transient BoundStatement boundStatement_counts;
    private transient BoundStatement boundStatement_where;
    private transient BoundStatement boundStatement_cluster;
    private transient BoundStatement boundStatement_cluster_delete;
    private transient BoundStatement boundStatement_cluster_get_with_lastround;
    private transient BoundStatement boundStatement_cluster_get;
    private transient BoundStatement boundStatement_cluster_get_by_id;
    private transient BoundStatement boundStatement_tweetsandcluster;
    private transient BoundStatement boundStatement_clustertweets;


    private static String TWEETS_FIELDS =   "(id, tweet, userid, tweettime, retweetcount, round, country, " +
            "class_politics, class_music,class_sports)";
    private static String TWEETS_VALUES = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static String EVENTS_FIELDS =   "(round, clusterid, country, cosinevector, incrementrate, numtweet)";
    private static String EVENTS_VALUES = "(?, ?, ?, ?, ?, ?)";


    private static String EVENTS_KEYBASED_FIELDS = "(round, country, word, incrementpercent)";
    private static String EVENTS_KEYBASED_VALUES = "(?, ?, ?, ?)";


    private static String COUNTS_FIELDS = "(round, word, country, count, totalnumofwords)";
    private static String COUNTS_VALUES = "(?, ?, ?, ?, ?)";

    private static String PROCESSTIMES_FIELDS =   "(row,column,id)";
    private static String PROCESSTIMES_VALUES = "(?, ?, ?)";

    private static String PROCESSED_FIELDS =   "(round, boltid, finished)";
    private static String PROCESSED_VALUES = "(?, ?, ?)";

    private static String CLUSTER_FIELDS =   "(id, country, cosinevector, prevnumtweets, currentnumtweets, lastround)";
    private static String CLUSTER_VALUES = "(?, ?, ?, ?, ?, ?)";

    private static String TWEETSANDCLUSTER_FIELDS =   "(clusterid, tweetid)";
    private static String TWEETSANDCLUSTER_VALUES = "(?, ?)";


    private String tweetsTable;
    private String countsTable;
    private String eventsTable;
    private String eventsKeybasedTable;
    private String processedTable;
    private String processTimesTable;
    private String clusterTable;
    private String tweetsandclusterTable;

    public CassandraDaoHybrid(String tweetsTable, String countsTable, String eventsTable, String processedTable, String processTimesTable, String clusterTable, String eventsKeybasedTable, String tweetsandclusterTable) throws Exception {
        this.tweetsTable = tweetsTable;
        this.countsTable = countsTable;
        this.eventsTable = eventsTable;
        this.processedTable = processedTable;
        this.processTimesTable = processTimesTable;
        this.clusterTable = clusterTable;
        this.eventsKeybasedTable = eventsKeybasedTable;
        this.tweetsandclusterTable = tweetsandclusterTable;

        prepareAll();
    }

    private void prepareAll()
    {


        if(statement_processtimes_get==null) {
            statement_processtimes_get = CassandraConnection.connect().prepare(
                    "SELECT * FROM " + processTimesTable);
        }
        if(statement_processtimes==null) {
            statement_processtimes = CassandraConnection.connect().prepare(
                    "INSERT INTO " + processTimesTable + " " + PROCESSTIMES_FIELDS
                            + " VALUES " + PROCESSTIMES_VALUES + ";");
        }

        if(statement_events_keybased==null) {
            statement_events_keybased = CassandraConnection.connect().prepare(
                    "INSERT INTO " + eventsKeybasedTable + " " + EVENTS_KEYBASED_FIELDS
                            + " VALUES " + EVENTS_KEYBASED_VALUES + ";");
        }
        if(statement_tweets==null) {
            statement_tweets = CassandraConnection.connect().prepare(
                    "INSERT INTO " + tweetsTable + " " + TWEETS_FIELDS
                            + " VALUES " + TWEETS_VALUES + ";");
        }
        if(statement_processed2Tweets ==null) {
            statement_processed2Tweets = CassandraConnection.connect().prepare(
                    "INSERT INTO " + processedTable + " " + PROCESSED_FIELDS
                            + " VALUES " + PROCESSED_VALUES + ";");
        }
        if(statement_processed2Tweets_get ==null) {
            statement_processed2Tweets_get = CassandraConnection.connect().prepare(
                    "SELECT * FROM " + processedTable + " WHERE round=? AND boltid=?;" );
        }

        if(statement_processed2Tweets_getAll ==null) {
            statement_processed2Tweets_getAll = CassandraConnection.connect().prepare(
                    "SELECT * FROM " + processedTable + " WHERE round=? ;" );
        }
        if(statement_counts==null) {
            statement_counts = CassandraConnection.connect().prepare(
                    "INSERT INTO " + countsTable + " " + COUNTS_FIELDS
                            + " VALUES " + COUNTS_VALUES + ";");
        }
        if(statement_events==null) {
            statement_events = CassandraConnection.connect().prepare(
                    "INSERT INTO " + eventsTable + " " + EVENTS_FIELDS
                            + " VALUES " + EVENTS_VALUES + ";");
        }
        if(statement_events_get==null) {
            statement_events_get = CassandraConnection.connect().prepare(
                    "SELECT * FROM " + eventsTable + " WHERE country=? ALLOW FILTERING;");
        }

        if(statement_where==null) {
            statement_where = CassandraConnection.connect().prepare(
                    "SELECT * FROM " + countsTable + " WHERE round=? AND word=? AND country=?;");
        }
        if(statement_tweet_get==null) {
            statement_tweet_get = CassandraConnection.connect().prepare(
                    "SELECT * FROM " + tweetsTable + " WHERE round=?;");
        }
        if(statement_tweet_by_country_get==null) {
            statement_tweet_by_country_get = CassandraConnection.connect().prepare(
                    "SELECT * FROM " + tweetsTable + " WHERE round=? AND country=?;");
        }
        if(statement_round_get==null) {
            statement_round_get = CassandraConnection.connect().prepare(
                    "SELECT DISTINCT round FROM " + tweetsTable + ";");
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


        if(statement_tweetsandcluster==null) {
            statement_tweetsandcluster = CassandraConnection.connect().prepare(
                    "INSERT INTO " + tweetsandclusterTable + " " + TWEETSANDCLUSTER_FIELDS
                            + " VALUES " + TWEETSANDCLUSTER_VALUES + ";");
        }
//        if(statement_clustertweets ==null) {
//            statement_clustertweets = CassandraConnection.connect().prepare(
//                    "SELECT * FROM " + tweetsandclusterTable + " WHERE clusterid=?;");
//        }
        if(statement_clustertweets ==null) {
            statement_clustertweets = CassandraConnection.connect().prepare(
                    "UPDATE " +  tweetsandclusterTable + " SET clusterid = ? where clusterid = ?;");
        }
        if(boundStatement_tweets == null)
            boundStatement_tweets = new BoundStatement(statement_tweets);
        if(boundStatement_events == null)
            boundStatement_events = new BoundStatement(statement_events);
        if(boundStatement_counts == null)
            boundStatement_counts = new BoundStatement(statement_counts);
        if(boundStatement_where == null)
            boundStatement_where = new BoundStatement(statement_where);
        if(boundStatement_events_get == null)
            boundStatement_events_get = new BoundStatement(statement_events_get);
        if(boundStatement_events_keybased == null)
            boundStatement_events_keybased = new BoundStatement(statement_events_keybased);
        if(boundStatement_tweets_get == null)
            boundStatement_tweets_get = new BoundStatement(statement_tweet_get);
        if(boundStatement_tweets_by_country_get == null)
            boundStatement_tweets_by_country_get = new BoundStatement(statement_tweet_by_country_get);
        if(boundStatement_rounds_get == null)
            boundStatement_rounds_get = new BoundStatement(statement_round_get);
        if(boundStatement_processed2Tweets_get == null)
            boundStatement_processed2Tweets_get = new BoundStatement(statement_processed2Tweets_get);
        if(boundStatement_processed2Tweets_getAll == null)
            boundStatement_processed2Tweets_getAll = new BoundStatement(statement_processed2Tweets_getAll);
        if(boundStatement_processed2Tweets == null)
            boundStatement_processed2Tweets = new BoundStatement(statement_processed2Tweets);
        if(boundStatement_processtimes_get == null)
        boundStatement_processtimes_get = new BoundStatement(statement_processtimes_get);
        if(boundStatement_processtimes == null)
            boundStatement_processtimes = new BoundStatement(statement_processtimes);
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
        if(boundStatement_tweetsandcluster == null)
            boundStatement_tweetsandcluster = new BoundStatement(statement_tweetsandcluster);
        if(boundStatement_clustertweets == null)
            boundStatement_clustertweets = new BoundStatement(statement_clustertweets);

    }


    public ResultSet getFromCounts( Object... values ) throws Exception
    {
        prepareAll();
        ResultSet resultSet = CassandraConnection.connect().execute(boundStatement_where.bind(values));

        return resultSet;
    }

    public void insertIntoEventsKeybased( Object... values ) throws Exception
    {
        prepareAll();
        CassandraConnection.connect().executeAsync(boundStatement_events_keybased.bind(values));
    }

    public void insertIntoProcessTimes( Object[] values ) throws Exception
    {
        prepareAll();
        CassandraConnection.connect().executeAsync(boundStatement_processtimes.bind(values));
    }

    public void insertIntoProcessed( Object[] values ) throws Exception
    {
        prepareAll();
        CassandraConnection.connect().execute(boundStatement_processed2Tweets.bind(values));
    }

    public ResultSet getAllProcessed( Object... values ) throws Exception
    {
        prepareAll();
        ResultSet resultSet = CassandraConnection.connect().execute(boundStatement_processed2Tweets_getAll.bind(values));

        return resultSet;
    }

    public void insertIntoEvents( Object... values ) throws Exception
    {
        prepareAll();
        CassandraConnection.connect().executeAsync(boundStatement_events.bind(values));
    }

    public ResultSet getTweetsByRound( Object... values ) throws Exception
    {
        prepareAll();
        ResultSet resultSet = CassandraConnection.connect().execute(boundStatement_tweets_get.bind(values));

        return resultSet;
    }

    public ResultSet getTweetsByRoundAndCountry( Object... values ) throws Exception
    {
        prepareAll();
        ResultSet resultSet = CassandraConnection.connect().execute(boundStatement_tweets_by_country_get.bind(values));
        return resultSet;
    }

    public ResultSet getRounds() throws Exception
    {
        prepareAll();
        ResultSet resultSet = CassandraConnection.connect().execute(boundStatement_rounds_get.bind());

        return resultSet;
    }

    public void insertIntoClusters( Object[] values ) throws Exception
    {
        prepareAll();
        CassandraConnection.connect().executeAsync(boundStatement_cluster.bind(values));
    }
    public void deleteFromClusters( Object... values ) throws Exception
    {
        prepareAll();
        CassandraConnection.connect().execute(boundStatement_cluster_delete.bind(values));
    }
    public ResultSet getClusters( Object... values ) throws Exception
    {
        prepareAll();
        ResultSet resultSet = CassandraConnection.connect().execute(boundStatement_cluster_get.bind(values));

        return resultSet;
    }

    public void insertIntoCounts( Object[] values ) throws Exception
    {
        prepareAll();
        ResultSetFuture rsf = CassandraConnection.connect().executeAsync(boundStatement_counts.bind(values));
        checkError(rsf);
    }

    public void insertIntoTweetsAndCluster( Object[] values ) throws Exception
    {
        prepareAll();
        CassandraConnection.connect().execute(boundStatement_tweetsandcluster.bind(values));
    }



    public void updateClusterTweets( Object[] values ) throws Exception
    {
        prepareAll();
        CassandraConnection.connect().execute(boundStatement_clustertweets.bind(values));

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


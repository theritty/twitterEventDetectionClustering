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
    private transient PreparedStatement statement_clusterandtweet;
    private transient PreparedStatement statement_cluster_get;
    private transient PreparedStatement statement_clusterandtweet_get;
    private transient PreparedStatement statement_tweet_get;
    private transient PreparedStatement statement_round_get;
    private transient BoundStatement boundStatement_tweets_get;
    private transient BoundStatement boundStatement_rounds_get;
    private transient BoundStatement boundStatement_cluster;
    private transient BoundStatement boundStatement_clusterandtweets;
    private transient BoundStatement boundStatement_cluster_get;
    private transient BoundStatement boundStatement_clusterandtweets_get;

    private static String CLUSTER_FIELDS =   "(id, round, cosinevector)";
    private static String CLUSTER_VALUES = "(?, ?, ?)";

    private static String CLUSTERANDTWEETS_FIELDS =   "(clusterid, tweetid)";
    private static String CLUSTERANDTWEETS_VALUES = "(?, ?)";

    private String tweetsTable;
    private String clusterTable;
    private String clusterandtweetTable;

    public CassandraDao(String tweetsTable, String clusterTable, String clusterandtweetTable) throws Exception {
        this.tweetsTable = tweetsTable;
        this.clusterTable = clusterTable;
        this.clusterandtweetTable = clusterandtweetTable;

        prepareAll();
    }

    private void prepareAll()
    {
        if(statement_cluster==null) {
            statement_cluster = CassandraConnection.connect().prepare(
                    "INSERT INTO " + clusterTable + " " + CLUSTER_FIELDS
                            + " VALUES " + CLUSTER_VALUES + ";");
        }
        if(statement_clusterandtweet==null) {
            statement_clusterandtweet = CassandraConnection.connect().prepare(
                    "INSERT INTO " + clusterandtweetTable + " " + CLUSTERANDTWEETS_FIELDS
                            + " VALUES " + CLUSTERANDTWEETS_VALUES + ";");
        }

        if(statement_tweet_get==null) {
            statement_tweet_get = CassandraConnection.connect().prepare(
                    "SELECT * FROM " + tweetsTable + " WHERE round=?;");
        }
        if(statement_round_get==null) {
            statement_round_get = CassandraConnection.connect().prepare(
                    "SELECT DISTINCT round FROM " + tweetsTable + ";");
        }
        if(statement_cluster_get==null) {
            statement_cluster_get = CassandraConnection.connect().prepare(
                    "SELECT * FROM " + clusterTable + " WHERE round=?;");
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
        if(boundStatement_clusterandtweets_get == null)
            boundStatement_clusterandtweets_get = new BoundStatement(statement_clusterandtweet_get);
        if(boundStatement_cluster == null)
            boundStatement_cluster = new BoundStatement(statement_cluster);
        if(boundStatement_clusterandtweets == null)
            boundStatement_clusterandtweets = new BoundStatement(statement_clusterandtweet);
    }

    public void insertIntoClusters( Object[] values ) throws Exception
    {
        prepareAll();
        CassandraConnection.connect().executeAsync(boundStatement_cluster.bind(values));
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

    public ResultSet getCustersByRound( Object... values ) throws Exception
    {
        prepareAll();
        ResultSet resultSet = CassandraConnection.connect().execute(boundStatement_cluster_get.bind(values));

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


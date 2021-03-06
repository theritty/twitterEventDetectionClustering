# twitterEventDetectionClustering


HOW TO RUN CLUSTER
------------------------
http://www.apache.org/dyn/closer.lua/storm/apache-storm-1.0.3/apache-storm-1.0.3.zip
unzip
edit storm.yaml

-----
storm.zookeeper.servers:
    - "127.0.0.1"
nimbus.seeds: ["127.0.0.1"]
storm.local.dir: "/Users/ozlemcerensahin/opt/apache-storm-1.0.3/datadir/storm"
supervisor.slots.ports:
    - 6700
    - 6701
    - 6702
    - 6703

------

http://www.apache.org/dyn/closer.cgi/zookeeper/
unzip
edit zoo.cfg
------
tickTime=2000
initLimit=10
syncLimit=5
dataDir=/Users/ozlemcerensahin/opt/zookeeper-3.4.9/datadir/zookeeper
clientPort=2181
------


start zk: ./zkServer.sh start
start storm::
    ./storm nimbus
    ./storm supervisor
    ./storm ui

submit jar:
    ./storm jar /Users/ozlemcerensahin/Desktop/workspace/twitterEventDetectionClustering/target/eventdetection-1.0-jar-with-dependencies.jar topologies.EventDetectionClustering
    ./storm jar /Users/ozlemcerensahin/Desktop/workspace/twitterEventDetectionClustering/target/eventdetection-1.0-jar-with-dependencies.jar topologies.EventDetectionKeyBased
    ./storm jar /Users/ozlemcerensahin/Desktop/workspace/twitterEventDetectionClustering/target/eventdetection-1.0-jar-with-dependencies.jar topologies.EventDetectionKeyBasedWithSleep
    ./storm jar /Users/ozlemcerensahin/Desktop/workspace/twitterEventDetectionClustering/target/eventdetection-1.0-jar-with-dependencies.jar topologies.EventDetectionHybrid


    ./storm jar /home/ceren/workspace/twitterEventDetectionClustering/target/eventdetection-1.0-jar-with-dependencies.jar topologies.EventDetectionClustering
    ./storm jar /home/ceren/workspace/twitterEventDetectionClustering/target/eventdetection-1.0-jar-with-dependencies.jar topologies.EventDetectionKeyBased
    ./storm jar /home/ceren/workspace/twitterEventDetectionClustering/target/eventdetection-1.0-jar-with-dependencies.jar topologies.EventDetectionKeyBasedWithSleep
    ./storm jar /home/ceren/workspace/twitterEventDetectionClustering/target/eventdetection-1.0-jar-with-dependencies.jar topologies.EventDetectionHybrid





TRUNCATE eventcluster_daily ;TRUNCATE events_daily ;TRUNCATE cluster_daily ;TRUNCATE clusterandtweet_daily ;TRUNCATE processedtweets ;TRUNCATE processtimes ;
TRUNCATE eventcluster4 ;TRUNCATE events4 ;TRUNCATE cluster4 ;TRUNCATE clusterandtweet4 ;TRUNCATE processedtweets4 ;TRUNCATE processtimes4 ;
TRUNCATE eventcluster3 ;TRUNCATE events3 ;TRUNCATE cluster3 ;TRUNCATE clusterandtweet3 ;TRUNCATE processedtweets3 ;TRUNCATE processtimes3 ;
TRUNCATE eventcluster ;TRUNCATE events ;TRUNCATE cluster ;TRUNCATE clusterandtweet ;TRUNCATE processedtweets ;TRUNCATE processtimes ;
TRUNCATE eventclusterForExperiment ;TRUNCATE clusterForExperiment ;TRUNCATE clusterandtweetForExperiment ;TRUNCATE processedtweetsForExperiment ;TRUNCATE processtimesForExperiment ;
TRUNCATE countsForExperiment ;TRUNCATE eventsForExperiment ;TRUNCATE processedForExperiment ; TRUNCATE processtimesKeyBasedForExperiment;
TRUNCATE countsForExperimentSleep ;TRUNCATE eventsForExperimentSleep ;TRUNCATE processedForExperimentSleep ; TRUNCATE processtimesKeyBasedForExperimentSleep;



CREATE TABLE tweetcollection.hybridclusters5_sub_4 (
country text,
id timeuuid,
cosinevector map<text, double>,
currentnumtweets int,
lastround bigint,
prevnumtweets int,
PRIMARY KEY (country, id)
);

CREATE TABLE tweetcollection.hybridcounts5_sub_4 (
round bigint,
word text,
country text,
count bigint,
totalnumofwords bigint,
PRIMARY KEY (round, word, country)
);

CREATE TABLE tweetcollection.hybridevents5_sub_4 (
round bigint,
clusterid timeuuid,
cosinevector map<text, double>,
country text,
incrementrate double,
numtweet int,
PRIMARY KEY (round, clusterid)
);
CREATE TABLE tweetcollection.hybrideventskeybased5_sub_4(
round bigint,
country text,
word text,
incrementpercent double,
PRIMARY KEY (round, country, word)
);
CREATE TABLE tweetcollection.hybridbolts5_sub_4 (
round bigint,
boltid int,
finished boolean,
PRIMARY KEY (round, boltid)
);


CREATE TABLE tweetcollection.hybridtimes5_sub_4 (
row int,
column int,
id int,
PRIMARY KEY (row, column)
);

CREATE TABLE tweetcollection.hybridtweets5_sub_4 (
round bigint,
clusterid timeuuid,
tweetid bigint,
PRIMARY KEY (round, tweetid, clusterid)
);

TRUNCATE hybridclusters4; TRUNCATE hybridcounts4; TRUNCATE hybridevents4; TRUNCATE hybrideventskeybased4; TRUNCATE hybridbolts4; TRUNCATE hybridtimes4; TRUNCATE hybridtweets4;




clustering.clusters.table=clusterforexperiment_thesis
clustering.events.table=eventclusterforexperiment_thesis
clustering.events_wordbased.table=eventsForExperiment_thesis
clustering.processed_tweets.table=processedtweetsforexperiment_thesis
clustering.processtimes.table=processtimesforexperiment_thesis
clustering.tweetsandcluster.table=tweetsandcluster_thesis



CREATE TABLE tweetcollection.clusteringClusters5_sub_4(
country text,
id timeuuid,
cosinevector map<text, double>,
currentnumtweets int,
lastround bigint,
prevnumtweets int,
PRIMARY KEY (country, id)
);
CREATE TABLE tweetcollection.clusteringEvents5_sub_4(
round bigint,
clusterid timeuuid,
cosinevector map<text, double>,
country text,
incrementrate double,
numtweet int,
PRIMARY KEY (round, clusterid)
);
CREATE TABLE tweetcollection.clusteringTimes5_sub_4(
row int,
column int,
id int,
PRIMARY KEY (row, column)
);
CREATE TABLE tweetcollection.clusteringBolts5_sub_4(
round bigint,
boltid int,
boltprocessed bigint,
country text,
finished boolean,
spoutsent bigint,
PRIMARY KEY (round, boltid)
);
CREATE TABLE tweetcollection.clusteringTweets5_sub_4(
round bigint,
clusterid timeuuid,
tweetid bigint,
PRIMARY KEY (round, tweetid, clusterid)
);


TRUNCATE clusteringClusters4; TRUNCATE clusteringEvents4; TRUNCATE clusteringTimes4; TRUNCATE clusteringBolts4; TRUNCATE clusteringTweets4;






CREATE TABLE tweetcollection.countsforexperiment_thesis (
round bigint,
word text,
country text,
count bigint,
totalnumofwords bigint,
PRIMARY KEY (round, word, country)
);
CREATE TABLE tweetcollection.eventsforexperiment_thesis (
round bigint,
country text,
word text,
incrementpercent double,
PRIMARY KEY (round, country, word)
);
CREATE TABLE tweetcollection.processedforexperiment_thesis (
round bigint,
boltid int,
finished boolean,
PRIMARY KEY (round, boltid)
);
CREATE TABLE tweetcollection.processtimeskeybasedforexperiment_thesis (
row int,
column int,
id int,
PRIMARY KEY (row, column)
);


TRUNCATE countsforexperiment_thesis; TRUNCATE eventsforexperiment_thesis; TRUNCATE processedforexperiment_thesis; TRUNCATE processtimeskeybasedforexperiment_thesis;




CREATE TABLE tweetcollection.countsforexperimentSleep_thesis (
round bigint,
word text,
country text,
count bigint,
totalnumofwords bigint,
PRIMARY KEY (round, word, country)
);
CREATE TABLE tweetcollection.eventsforexperimentSleep_thesis (
round bigint,
country text,
word text,
incrementpercent double,
PRIMARY KEY (round, country, word)
);
CREATE TABLE tweetcollection.processedforexperimentSleep_thesis (
round bigint,
boltid int,
finished boolean,
PRIMARY KEY (round, boltid)
);
CREATE TABLE tweetcollection.processtimeskeybasedforexperimentSleep_thesis (
row int,
column int,
id int,
PRIMARY KEY (row, column)
);

TRUNCATE countsforexperimentSleep_thesis; TRUNCATE eventsforexperimentSleep_thesis; TRUNCATE processedforexperimentSleep_thesis; TRUNCATE processtimeskeybasedforexperimentSleep_thesis;




select * from eventsforexperiment_thesis;
select * from processtimeskeybasedforexperiment_thesis;
select * from eventsforexperimentSleep_thesis;
select * from processtimeskeybasedforexperimentSleep_thesis;


TRUNCATE clustershybridforexperiment_thesis; TRUNCATE countshybridforexperiment_thesis; TRUNCATE eventskeybasedhybridforexperiment_thesis; TRUNCATE eventshybridforexperiment_thesis; TRUNCATE processedhybridforexperiment_thesis; TRUNCATE processtimeshybridforexperiment_thesis; TRUNCATE tweetsandclusterhybrid_thesis;
TRUNCATE clusterforexperiment_thesis; TRUNCATE eventclusterforexperiment_thesis; TRUNCATE eventsforexperiment_thesis; TRUNCATE processedtweetsforexperiment_thesis; TRUNCATE processtimesforexperiment_thesis; TRUNCATE processtimesforexperiment_thesis;
TRUNCATE countsforexperiment_thesis; TRUNCATE eventsforexperiment_thesis; TRUNCATE processedforexperiment_thesis; TRUNCATE processtimeskeybasedforexperiment_thesis;
TRUNCATE countsforexperimentSleep_thesis; TRUNCATE eventsforexperimentSleep_thesis; TRUNCATE processedforexperimentSleep_thesis; TRUNCATE processtimeskeybasedforexperimentSleep_thesis;




select count(*) from eventshybridforexperiment_thesis;
select count(*) from eventclusterforexperiment_thesis;
select count(*) from eventsforexperiment_thesis;


select count(*) from eventshybridforexperiment_paper1;
select count(*) from eventclusterforexperiment_paper1;
select count(*) from eventsforexperiment_paper1;

select count(*) from eventshybridforexperiment5;
select count(*) from eventclusterforexperiment5;
select count(*) from eventsforexperiment5;

./cqlsh -f resCluster.txt > outputCluster.txt
./cqlsh -f resHybrid.txt > outputHybrid.txt
./cqlsh -f resKeybased.txt > outputKeybased.txt





CREATE TABLE tweetcollection.tweets_updated (
    round bigint,
    country text,
    class_music boolean,
    class_sports boolean,
    class_politics boolean,
    tweettime timestamp,
    id bigint,
    retweetcount bigint,
    tweet text,
    userid bigint,
    PRIMARY KEY (round, country, id, class_music, class_sports, class_politics, tweettime)
)



copy hybridevents4 (round , clusterid , cosinevector , country , incrementrate , numtweet ) to '/Users/ozlemcerensahin/Desktop/thesis-withthresholds/eventshybrid.csv';
copy hybridtweets4 (round , clusterid , tweetid )  to '/Users/ozlemcerensahin/Desktop/thesis-withthresholds/tweetshybrid.csv';
copy hybridtimes4 (row, column, id)   to '/Users/ozlemcerensahin/Desktop/thesis-withthresholds/timeshybrid.csv';

copy clusteringevents3 (round , clusterid , cosinevector , country , incrementrate , numtweet ) to '/Users/ozlemcerensahin/Desktop/thesis-withthresholds/eventsclustering.csv';
copy clusteringtweets3 (round , clusterid , tweetid )  to '/Users/ozlemcerensahin/Desktop/thesis-withthresholds/tweetsclustering.csv';
copy clusteringtimes3 (row  , column, id)   to '/Users/ozlemcerensahin/Desktop/thesis-withthresholds/timesclustering.csv';

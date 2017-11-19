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



TRUNCATE eventcluster_daily ;TRUNCATE events_daily ;TRUNCATE cluster_daily ;TRUNCATE clusterandtweet_daily ;TRUNCATE processedtweets ;TRUNCATE processtimes ;
TRUNCATE eventcluster4 ;TRUNCATE events4 ;TRUNCATE cluster4 ;TRUNCATE clusterandtweet4 ;TRUNCATE processedtweets4 ;TRUNCATE processtimes4 ;
TRUNCATE eventcluster3 ;TRUNCATE events3 ;TRUNCATE cluster3 ;TRUNCATE clusterandtweet3 ;TRUNCATE processedtweets3 ;TRUNCATE processtimes3 ;
TRUNCATE eventcluster ;TRUNCATE events ;TRUNCATE cluster ;TRUNCATE clusterandtweet ;TRUNCATE processedtweets ;TRUNCATE processtimes ;
TRUNCATE eventclusterForExperiment ;TRUNCATE clusterForExperiment ;TRUNCATE clusterandtweetForExperiment ;TRUNCATE processedtweetsForExperiment ;TRUNCATE processtimesForExperiment ;
TRUNCATE countsForExperiment ;TRUNCATE eventsForExperiment ;TRUNCATE processedForExperiment ; TRUNCATE processtimesKeyBasedForExperiment;
TRUNCATE countsForExperimentSleep ;TRUNCATE eventsForExperimentSleep ;TRUNCATE processedForExperimentSleep ; TRUNCATE processtimesKeyBasedForExperimentSleep;



CREATE TABLE tweetcollection.clustershybridforexperiment_paper3 (
country text,
id timeuuid,
cosinevector map<text, double>,
currentnumtweets int,
lastround bigint,
prevnumtweets int,
PRIMARY KEY (country, id)
);

CREATE TABLE tweetcollection.countshybridforexperiment_paper3 (
round bigint,
word text,
country text,
count bigint,
totalnumofwords bigint,
PRIMARY KEY (round, word, country)
);

CREATE TABLE tweetcollection.eventshybridforexperiment_paper3 (
round bigint,
clusterid timeuuid,
cosinevector map<text, double>,
country text,
incrementrate double,
numtweet int,
PRIMARY KEY (round, clusterid)
);
CREATE TABLE tweetcollection.eventskeybasedhybridforexperiment_paper3 (
round bigint,
country text,
word text,
incrementpercent double,
PRIMARY KEY (round, country, word)
);
CREATE TABLE tweetcollection.processedhybridforexperiment_paper3 (
round bigint,
boltid int,
finished boolean,
PRIMARY KEY (round, boltid)
);

CREATE TABLE tweetcollection.processtimeshybridforexperiment_paper3 (
row int,
column int,
id int,
PRIMARY KEY (row, column)
);





clustering.clusters.table=clusterforexperiment_eval
clustering.events.table=eventclusterforexperiment_eval
clustering.events_wordbased.table=eventsForExperiment_eval
clustering.processed_tweets.table=processedtweetsforexperiment_eval
clustering.processtimes.table=processtimesforexperiment_eval
clustering.tweetsandcluster.table=tweetsandcluster_eval


CREATE TABLE tweetcollection.clusterforexperiment_eval (
country text,
id timeuuid,
cosinevector map<text, double>,
currentnumtweets int,
lastround bigint,
prevnumtweets int,
PRIMARY KEY (country, id)
);
CREATE TABLE tweetcollection.eventclusterforexperiment_eval (
round bigint,
clusterid timeuuid,
cosinevector map<text, double>,
country text,
incrementrate double,
numtweet int,
PRIMARY KEY (round, clusterid)
);
CREATE TABLE tweetcollection.processedtweetsforexperiment_eval (
round bigint,
boltid int,
boltprocessed bigint,
country text,
finished boolean,
spoutsent bigint,
PRIMARY KEY (round, boltid)
);
CREATE TABLE tweetcollection.processtimesforexperiment_eval (
row int,
column int,
id int,
PRIMARY KEY (row, column)
);
CREATE TABLE tweetcollection.tweetsandcluster_eval (
round bigint,
clusterid text,
tweetid bigint,
PRIMARY KEY (round, tweetid)
);








CREATE TABLE tweetcollection.countsforexperiment_paper3 (
round bigint,
word text,
country text,
count bigint,
totalnumofwords bigint,
PRIMARY KEY (round, word, country)
);
CREATE TABLE tweetcollection.eventsforexperiment_paper3 (
round bigint,
country text,
word text,
incrementpercent double,
PRIMARY KEY (round, country, word)
);
CREATE TABLE tweetcollection.processedforexperiment_paper3 (
round bigint,
boltid int,
finished boolean,
PRIMARY KEY (round, boltid)
);
CREATE TABLE tweetcollection.processtimeskeybasedforexperiment_paper3 (
row int,
column int,
id int,
PRIMARY KEY (row, column)
);






CREATE TABLE tweetcollection.countsforexperimentSleep_paper3 (
round bigint,
word text,
country text,
count bigint,
totalnumofwords bigint,
PRIMARY KEY (round, word, country)
);
CREATE TABLE tweetcollection.eventsforexperimentSleep_paper3 (
round bigint,
country text,
word text,
incrementpercent double,
PRIMARY KEY (round, country, word)
);
CREATE TABLE tweetcollection.processedforexperimentSleep_paper3 (
round bigint,
boltid int,
finished boolean,
PRIMARY KEY (round, boltid)
);
CREATE TABLE tweetcollection.processtimeskeybasedforexperimentSleep_paper3 (
row int,
column int,
id int,
PRIMARY KEY (row, column)
);


TRUNCATE clustershybridforexperiment_paper3; TRUNCATE countshybridforexperiment_paper3; TRUNCATE eventskeybasedhybridforexperiment_paper3; TRUNCATE eventshybridforexperiment_paper3; TRUNCATE processedhybridforexperiment_paper3; TRUNCATE processtimeshybridforexperiment_paper3;
TRUNCATE clusterforexperiment_eval; TRUNCATE eventclusterforexperiment_eval; TRUNCATE eventsforexperiment_eval; TRUNCATE processedtweetsforexperiment_eval; TRUNCATE processtimesforexperiment_eval; TRUNCATE processtimesforexperiment_eval;
TRUNCATE countsforexperiment_paper3; TRUNCATE eventsforexperiment_paper3; TRUNCATE processedforexperiment_paper3; TRUNCATE processtimeskeybasedforexperiment_paper3;
TRUNCATE countsforexperimentSleep_paper3; TRUNCATE eventsforexperimentSleep_paper3; TRUNCATE processedforexperimentSleep_paper3; TRUNCATE processtimeskeybasedforexperimentSleep_paper3;


select count(*) from eventshybridforexperiment_paper3;
select count(*) from eventclusterforexperiment_paper3;
select count(*) from eventsforexperiment_paper3;


select count(*) from eventshybridforexperiment_paper1;
select count(*) from eventclusterforexperiment_paper1;
select count(*) from eventsforexperiment_paper1;

select count(*) from eventshybridforexperiment5;
select count(*) from eventclusterforexperiment5;
select count(*) from eventsforexperiment5;

./cqlsh -f resCluster.txt > outputCluster.txt
./cqlsh -f resHybrid.txt > outputHybrid.txt
./cqlsh -f resKeybased.txt > outputKeybased.txt
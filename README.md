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



CREATE TABLE tweetcollection.clustershybridforexperiment5 (
country text,
id timeuuid,
cosinevector map<text, double>,
currentnumtweets int,
lastround bigint,
prevnumtweets int,
PRIMARY KEY (country, id)
);

CREATE TABLE tweetcollection.countshybridforexperiment5 (
round bigint,
word text,
country text,
count bigint,
totalnumofwords bigint,
PRIMARY KEY (round, word, country)
);

CREATE TABLE tweetcollection.eventshybridforexperiment5 (
round bigint,
clusterid timeuuid,
cosinevector map<text, double>,
country text,
incrementrate double,
numtweet int,
PRIMARY KEY (round, clusterid)
);
CREATE TABLE tweetcollection.eventskeybasedhybridforexperiment5 (
round bigint,
country text,
word text,
incrementpercent double,
PRIMARY KEY (round, country, word)
);
CREATE TABLE tweetcollection.processedhybridforexperiment5 (
round bigint,
boltid int,
finished boolean,
PRIMARY KEY (round, boltid)
);

CREATE TABLE tweetcollection.processtimeshybridforexperiment5 (
row int,
column int,
id int,
PRIMARY KEY (row, column)
);








CREATE TABLE tweetcollection.clusterforexperiment5 (
country text,
id timeuuid,
cosinevector map<text, double>,
currentnumtweets int,
lastround bigint,
prevnumtweets int,
PRIMARY KEY (country, id)
);
CREATE TABLE tweetcollection.eventclusterforexperiment5 (
round bigint,
clusterid timeuuid,
cosinevector map<text, double>,
country text,
incrementrate double,
numtweet int,
PRIMARY KEY (round, clusterid)
);
CREATE TABLE tweetcollection.processedtweetsforexperiment5 (
round bigint,
boltid int,
boltprocessed bigint,
country text,
finished boolean,
spoutsent bigint,
PRIMARY KEY (round, boltid)
);
CREATE TABLE tweetcollection.processtimesforexperiment5 (
row int,
column int,
id int,
PRIMARY KEY (row, column)
);








CREATE TABLE tweetcollection.countsforexperiment5 (
round bigint,
word text,
country text,
count bigint,
totalnumofwords bigint,
PRIMARY KEY (round, word, country)
);
CREATE TABLE tweetcollection.eventsforexperiment5 (
round bigint,
country text,
word text,
incrementpercent double,
PRIMARY KEY (round, country, word)
);
CREATE TABLE tweetcollection.processedforexperiment5 (
round bigint,
boltid int,
finished boolean,
PRIMARY KEY (round, boltid)
);
CREATE TABLE tweetcollection.processtimeskeybasedforexperiment5 (
row int,
column int,
id int,
PRIMARY KEY (row, column)
);






CREATE TABLE tweetcollection.countsforexperimentSleep5 (
round bigint,
word text,
country text,
count bigint,
totalnumofwords bigint,
PRIMARY KEY (round, word, country)
);
CREATE TABLE tweetcollection.eventsforexperimentSleep5 (
round bigint,
country text,
word text,
incrementpercent double,
PRIMARY KEY (round, country, word)
);
CREATE TABLE tweetcollection.processedforexperimentSleep5 (
round bigint,
boltid int,
finished boolean,
PRIMARY KEY (round, boltid)
);
CREATE TABLE tweetcollection.processtimeskeybasedforexperimentSleep5 (
row int,
column int,
id int,
PRIMARY KEY (row, column)
);


TRUNCATE clustershybridforexperiment5; TRUNCATE countshybridforexperiment5; TRUNCATE eventskeybasedhybridforexperiment5; TRUNCATE eventshybridforexperiment5; TRUNCATE processedhybridforexperiment5; TRUNCATE processtimeshybridforexperiment5;
TRUNCATE clusterforexperiment4; TRUNCATE eventclusterforexperiment4; TRUNCATE eventsforexperiment4; TRUNCATE processedtweetsforexperiment4; TRUNCATE processtimesforexperiment4;
TRUNCATE countsforexperiment4; TRUNCATE eventsforexperiment4; TRUNCATE processedforexperiment4; TRUNCATE processtimeskeybasedforexperiment4;
TRUNCATE countsforexperimentSleep4; TRUNCATE eventsforexperimentSleep4; TRUNCATE processedforexperimentSleep4; TRUNCATE processtimeskeybasedforexperimentSleep4;


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



CREATE TABLE tweetcollection.clustershybridforexperiment_paper_single (
country text,
id timeuuid,
cosinevector map<text, double>,
currentnumtweets int,
lastround bigint,
prevnumtweets int,
PRIMARY KEY (country, id)
);

CREATE TABLE tweetcollection.countshybridforexperiment_paper_single (
round bigint,
word text,
country text,
count bigint,
totalnumofwords bigint,
PRIMARY KEY (round, word, country)
);

CREATE TABLE tweetcollection.eventshybridforexperiment_paper_single (
round bigint,
clusterid timeuuid,
cosinevector map<text, double>,
country text,
incrementrate double,
numtweet int,
PRIMARY KEY (round, clusterid)
);
CREATE TABLE tweetcollection.eventskeybasedhybridforexperiment_paper_single (
round bigint,
country text,
word text,
incrementpercent double,
PRIMARY KEY (round, country, word)
);
CREATE TABLE tweetcollection.processedhybridforexperiment_paper_single (
round bigint,
boltid int,
finished boolean,
PRIMARY KEY (round, boltid)
);

CREATE TABLE tweetcollection.processtimeshybridforexperiment_paper_single (
row int,
column int,
id int,
PRIMARY KEY (row, column)
);








CREATE TABLE tweetcollection.clusterforexperiment_paper_single (
country text,
id timeuuid,
cosinevector map<text, double>,
currentnumtweets int,
lastround bigint,
prevnumtweets int,
PRIMARY KEY (country, id)
);
CREATE TABLE tweetcollection.eventclusterforexperiment_paper_single (
round bigint,
clusterid timeuuid,
cosinevector map<text, double>,
country text,
incrementrate double,
numtweet int,
PRIMARY KEY (round, clusterid)
);
CREATE TABLE tweetcollection.processedtweetsforexperiment_paper_single (
round bigint,
boltid int,
boltprocessed bigint,
country text,
finished boolean,
spoutsent bigint,
PRIMARY KEY (round, boltid)
);
CREATE TABLE tweetcollection.processtimesforexperiment_paper_single (
row int,
column int,
id int,
PRIMARY KEY (row, column)
);








CREATE TABLE tweetcollection.countsforexperiment_paper_single (
round bigint,
word text,
country text,
count bigint,
totalnumofwords bigint,
PRIMARY KEY (round, word, country)
);
CREATE TABLE tweetcollection.eventsforexperiment_paper_single (
round bigint,
country text,
word text,
incrementpercent double,
PRIMARY KEY (round, country, word)
);
CREATE TABLE tweetcollection.processedforexperiment_paper_single (
round bigint,
boltid int,
finished boolean,
PRIMARY KEY (round, boltid)
);
CREATE TABLE tweetcollection.processtimeskeybasedforexperiment_paper_single (
row int,
column int,
id int,
PRIMARY KEY (row, column)
);






CREATE TABLE tweetcollection.countsforexperimentSleep_paper_single (
round bigint,
word text,
country text,
count bigint,
totalnumofwords bigint,
PRIMARY KEY (round, word, country)
);
CREATE TABLE tweetcollection.eventsforexperimentSleep_paper_single (
round bigint,
country text,
word text,
incrementpercent double,
PRIMARY KEY (round, country, word)
);
CREATE TABLE tweetcollection.processedforexperimentSleep_paper_single (
round bigint,
boltid int,
finished boolean,
PRIMARY KEY (round, boltid)
);
CREATE TABLE tweetcollection.processtimeskeybasedSleep_paper_single (
row int,
column int,
id int,
PRIMARY KEY (row, column)
);


TRUNCATE clustershybridforexperiment_paper_single; TRUNCATE countshybridforexperiment_paper_single; TRUNCATE eventskeybasedhybridforexperiment_paper_single; TRUNCATE eventshybridforexperiment_paper_single; TRUNCATE processedhybridforexperiment_paper_single; TRUNCATE processtimeshybridforexperiment_paper_single;
TRUNCATE clusterforexperiment_paper_single; TRUNCATE eventclusterforexperiment_paper_single; TRUNCATE eventsforexperiment_paper_single; TRUNCATE processedtweetsforexperiment_paper_single; TRUNCATE processtimesforexperiment_paper_single;
TRUNCATE countsforexperiment_paper_single; TRUNCATE eventsforexperiment_paper_single; TRUNCATE processedforexperiment_paper_single; TRUNCATE processtimeskeybasedforexperiment_paper_single;
TRUNCATE countsforexperimentSleep_paper_single; TRUNCATE eventsforexperimentSleep_paper_single; TRUNCATE processedforexperimentSleep_paper_single; TRUNCATE processtimeskeybasedSleep_paper_single;


select count(*) from eventshybridforexperiment_paper_single;
select count(*) from eventclusterforexperiment_paper_single;
select count(*) from eventsforexperiment_paper_single;


select count(*) from eventshybridforexperiment_paper1;
select count(*) from eventclusterforexperiment_paper1;
select count(*) from eventsforexperiment_paper1;

select count(*) from eventshybridforexperiment5;
select count(*) from eventclusterforexperiment5;
select count(*) from eventsforexperiment5;

./cqlsh -f resCluster.txt > outputCluster.txt
./cqlsh -f resHybrid.txt > outputHybrid.txt
./cqlsh -f resKeybased.txt > outputKeybased.txt
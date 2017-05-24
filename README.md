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


CREATE TABLE tweetcollection.clustersHybridForExperiment (
    id timeuuid,
    cosinevector map<text, double>,
    prevnumtweets int,
    currentnumtweets int,
    lastround bigint,
    country text,
    PRIMARY KEY (country, id)
);

CREATE TABLE tweetcollection.processtimesHybridForExperiment (
    row int,
    column int,
    id int,
    PRIMARY KEY (row, column)
);


CREATE TABLE tweetcollection.eventsHybridForExperiment (
    round bigint,
    clusterid timeuuid,
    country text,
    cosinevector map<text, double>,
    incrementrate double,
    numtweet int,
    PRIMARY KEY (round, clusterid)
);


CREATE TABLE tweetcollection.countsHybridForExperiment  (
    round bigint,
    word text,
    country text,
    count bigint,
    totalnumofwords bigint,
    PRIMARY KEY (round, word, country)
);


CREATE TABLE tweetcollection.processedHybridForExperiment  (
    round bigint,
    boltid int,
    finished boolean,
    PRIMARY KEY (round, boltid)
);




CREATE TABLE tweetcollection.clusterandtweetForExperiment (
    clusterid timeuuid,
    tweetid bigint,
    PRIMARY KEY (clusterid, tweetid)
);


CREATE TABLE tweetcollection.processedtweetsForExperiment (
    round bigint,
    boltid int,
    boltprocessed bigint,
    country text,
    finished boolean,
    spoutsent bigint,
    PRIMARY KEY (round, boltid)
);


CREATE TABLE tweetcollection.eventsForExperiment (
    round bigint,
    country text,
    word text,
    incrementpercent double,
    PRIMARY KEY (round, country, word)
);



CREATE TABLE tweetcollection.processtimesKeyBasedForExperimentSleep (
    row int,
    column int,
    id int,
    PRIMARY KEY (row, column)
);

CREATE TABLE tweetcollection.processtimesKeyBasedForExperiment (
    row int,
    column int,
    id int,
    PRIMARY KEY (row, column)
);

TRUNCATE eventcluster_daily ;TRUNCATE events_daily ;TRUNCATE cluster_daily ;TRUNCATE clusterandtweet_daily ;TRUNCATE processedtweets ;TRUNCATE processtimes ;
TRUNCATE eventcluster4 ;TRUNCATE events4 ;TRUNCATE cluster4 ;TRUNCATE clusterandtweet4 ;TRUNCATE processedtweets4 ;TRUNCATE processtimes4 ;
TRUNCATE eventcluster3 ;TRUNCATE events3 ;TRUNCATE cluster3 ;TRUNCATE clusterandtweet3 ;TRUNCATE processedtweets3 ;TRUNCATE processtimes3 ;
TRUNCATE eventcluster ;TRUNCATE events ;TRUNCATE cluster ;TRUNCATE clusterandtweet ;TRUNCATE processedtweets ;TRUNCATE processtimes ;
TRUNCATE eventclusterForExperiment ;TRUNCATE clusterForExperiment ;TRUNCATE clusterandtweetForExperiment ;TRUNCATE processedtweetsForExperiment ;TRUNCATE processtimesForExperiment ;
TRUNCATE countsForExperiment ;TRUNCATE eventsForExperiment ;TRUNCATE processedForExperiment ; TRUNCATE processtimesKeyBasedForExperiment;
TRUNCATE countsForExperimentSleep ;TRUNCATE eventsForExperimentSleep ;TRUNCATE processedForExperimentSleep ; TRUNCATE processtimesKeyBasedForExperimentSleep;







CREATE TABLE tweetcollection.eventsForExperimentSleep  (
    round bigint,
    country text,
    word text,
    incrementpercent double,
    PRIMARY KEY (round, country, word)
);


CREATE TABLE tweetcollection.processedForExperimentSleep  (
    round bigint,
    boltid int,
    finished boolean,
    PRIMARY KEY (round, boltid)
);




INSERT INTO tweetsmini (round , country , class_music , class_sports , class_politics , tweettime , id , retweetcount , tweet , userid ) VALUES ( 2033721, 'CAN', True, True, True, '1970-01-01 00:20:34+0000', 15, 0, 'bolu abant picnic', 1);
INSERT INTO tweetsmini (round , country , class_music , class_sports , class_politics , tweettime , id , retweetcount , tweet , userid ) VALUES ( 2033721, 'CAN', True, True, True, '1970-01-01 00:20:34+0000', 16, 0, 'bolu abant picnic', 1);
INSERT INTO tweetsmini (round , country , class_music , class_sports , class_politics , tweettime , id , retweetcount , tweet , userid ) VALUES ( 2033721, 'CAN', True, True, True, '1970-01-01 00:20:34+0000', 17, 0, 'bolu abant picnic', 1);
INSERT INTO tweetsmini (round , country , class_music , class_sports , class_politics , tweettime , id , retweetcount , tweet , userid ) VALUES ( 2033721, 'CAN', True, True, True, '1970-01-01 00:20:34+0000', 18, 0, 'bolu abant picnic', 1);
INSERT INTO tweetsmini (round , country , class_music , class_sports , class_politics , tweettime , id , retweetcount , tweet , userid ) VALUES ( 2033721, 'CAN', True, True, True, '1970-01-01 00:20:34+0000', 19, 0, 'ece abant picnic', 1);
INSERT INTO tweetsmini (round , country , class_music , class_sports , class_politics , tweettime , id , retweetcount , tweet , userid ) VALUES ( 2033721, 'CAN', True, True, True, '1970-01-01 00:20:34+0000', 20, 0, 'bolu giresun picnic', 1);
INSERT INTO tweetsmini (round , country , class_music , class_sports , class_politics , tweettime , id , retweetcount , tweet , userid ) VALUES ( 2033721, 'CAN', True, True, True, '1970-01-01 00:20:34+0000', 21, 0, 'bolu abant picnic', 1);
INSERT INTO tweetsmini (round , country , class_music , class_sports , class_politics , tweettime , id , retweetcount , tweet , userid ) VALUES ( 2033721, 'CAN', True, True, True, '1970-01-01 00:20:34+0000', 22, 0, 'adl abant picnic', 1);
INSERT INTO tweetsmini (round , country , class_music , class_sports , class_politics , tweettime , id , retweetcount , tweet , userid ) VALUES ( 2033721, 'CAN', True, True, True, '1970-01-01 00:20:34+0000', 23, 0, 'bolu ccc picnic', 1);
INSERT INTO tweetsmini (round , country , class_music , class_sports , class_politics , tweettime , id , retweetcount , tweet , userid ) VALUES ( 2033723, 'CAN', True, True, True, '1970-01-01 00:20:34+0000', 24, 0, ' abant picnic', 1);
INSERT INTO tweetsmini (round , country , class_music , class_sports , class_politics , tweettime , id , retweetcount , tweet , userid ) VALUES ( 2033725, 'CAN', True, True, True, '1970-01-01 00:20:34+0000', 25, 0, 'bolu  picnic', 1);
INSERT INTO tweetsmini (round , country , class_music , class_sports , class_politics , tweettime , id , retweetcount , tweet , userid ) VALUES ( 2033727, 'CAN', True, True, True, '1970-01-01 00:20:34+0000', 26, 0, 'bolu abant ', 1);
INSERT INTO tweetsmini (round , country , class_music , class_sports , class_politics , tweettime , id , retweetcount , tweet , userid ) VALUES ( 2033727, 'CAN', True, True, True, '1970-01-01 00:20:34+0000', 27, 0, ' picnic', 1);
INSERT INTO tweetsmini (round , country , class_music , class_sports , class_politics , tweettime , id , retweetcount , tweet , userid ) VALUES ( 2033727, 'CAN', True, True, True, '1970-01-01 00:20:34+0000', 28, 0, 'ece ceren picnic', 1);



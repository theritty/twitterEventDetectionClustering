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



CREATE TABLE tweetcollection.clustershybridThesis (
country text,
id timeuuid,
cosinevector map<text, double>,
currentnumtweets int,
lastround bigint,
prevnumtweets int,
PRIMARY KEY (country, id)
);

CREATE TABLE tweetcollection.countshybridThesis (
round bigint,
word text,
country text,
count bigint,
totalnumofwords bigint,
PRIMARY KEY (round, word, country)
);

CREATE TABLE tweetcollection.eventshybridThesis (
round bigint,
clusterid timeuuid,
cosinevector map<text, double>,
country text,
incrementrate double,
numtweet int,
PRIMARY KEY (round, clusterid)
);

CREATE TABLE tweetcollection.processtimeshybridThesis (
row int,
column int,
id int,
PRIMARY KEY (row, column)
);
CREATE TABLE tweetcollection.eventskeybasedhybridThesis(
round bigint,
country text,
word text,
incrementpercent double,
PRIMARY KEY (round, country, word)
);
CREATE TABLE tweetcollection.processedhybridThesis (
round bigint,
boltid int,
finished boolean,
PRIMARY KEY (round, boltid)
);


CREATE TABLE tweetcollection.tweetsandclusterhybridThesis (
round bigint,
clusterid timeuuid,
tweetid bigint,
PRIMARY KEY (round, tweetid, clusterid)
);

TRUNCATE clustershybridThesis; TRUNCATE countshybridThesis; TRUNCATE eventskeybasedhybridThesis; TRUNCATE eventshybridThesis; TRUNCATE processedhybridThesis; TRUNCATE processtimeshybridThesis; TRUNCATE tweetsandclusterhybrid_thesis;



clustering.clusters.table=clusteringClusters1
clustering.events.table=clusteringEvents1
clustering.events_wordbased.table=eventsForExperiment_thesis
clustering.processed_tweets.table=clusteringBolts1
clustering.processtimes.table=clusteringTimes1
clustering.tweetsandcluster.table=clusteringTweets1


CREATE TABLE tweetcollection.clusteringClusters10(
country text,
id timeuuid,
cosinevector map<text, double>,
currentnumtweets int,
lastround bigint,
prevnumtweets int,
PRIMARY KEY (country, id)
);
CREATE TABLE tweetcollection.clusteringEvents10(
round bigint,
clusterid timeuuid,
cosinevector map<text, double>,
country text,
incrementrate double,
numtweet int,
PRIMARY KEY (round, clusterid)
);
CREATE TABLE tweetcollection.clusteringTimes10(
row int,
column int,
id int,
PRIMARY KEY (row, column)
);
CREATE TABLE tweetcollection.clusteringBolts10(
round bigint,
boltid int,
boltprocessed bigint,
country text,
finished boolean,
spoutsent bigint,
PRIMARY KEY (round, boltid)
);
CREATE TABLE tweetcollection.clusteringTweets10(
round bigint,
clusterid timeuuid,
tweetid bigint,
PRIMARY KEY (round, tweetid, clusterid)
);

TRUNCATE clusteringClusters1; TRUNCATE clusteringEvents1; TRUNCATE clusteringBolts1; TRUNCATE clusteringTimes1; TRUNCATE clusteringTweets1;
TRUNCATE clusteringClusters2; TRUNCATE clusteringEvents2; TRUNCATE clusteringBolts2; TRUNCATE clusteringTimes2; TRUNCATE clusteringTweets2;
TRUNCATE clusteringClusters3; TRUNCATE clusteringEvents3; TRUNCATE clusteringBolts3; TRUNCATE clusteringTimes3; TRUNCATE clusteringTweets3;
TRUNCATE clusteringClusters4; TRUNCATE clusteringEvents4; TRUNCATE clusteringBolts4; TRUNCATE clusteringTimes4; TRUNCATE clusteringTweets4;
TRUNCATE clusteringClusters5; TRUNCATE clusteringEvents5; TRUNCATE clusteringBolts5; TRUNCATE clusteringTimes5; TRUNCATE clusteringTweets5;

TRUNCATE clusteringClusters6; TRUNCATE clusteringEvents6; TRUNCATE clusteringBolts6; TRUNCATE clusteringTimes6; TRUNCATE clusteringTweets6;
TRUNCATE clusteringClusters7; TRUNCATE clusteringEvents7; TRUNCATE clusteringBolts7; TRUNCATE clusteringTimes7; TRUNCATE clusteringTweets7;
TRUNCATE clusteringClusters8; TRUNCATE clusteringEvents8; TRUNCATE clusteringBolts8; TRUNCATE clusteringTimes8; TRUNCATE clusteringTweets8;
TRUNCATE clusteringClusters9; TRUNCATE clusteringEvents9; TRUNCATE clusteringBolts9; TRUNCATE clusteringTimes9; TRUNCATE clusteringTweets9;
TRUNCATE clusteringClusters10; TRUNCATE clusteringEvents10; TRUNCATE clusteringBolts10; TRUNCATE clusteringTimes10; TRUNCATE clusteringTweets10;



[4068481, 4068482, 4068487, 4068515, 4068517, 4068534, 4068538, 4068542, 4068547, 4068550, 4068558, 4068559, 4068565, 4068566, 4068569, 4068574, 4068575, 4068582, 4068659, 4068660, 4068668, 4068669, 4068670, 4068671, 4068679, 4068682, 4068685, 4068689, 4068700, 4068718, 4068723, 4068732, 4068736, 4068740, 4068742, 4068745, 4068751, 4068752, 4068754, 4068781, 4068787, 4068789, 4068820, 4068849, 4068896, 4068900, 4068905, 4068909, 4068910, 4068920, 4068924, 4068934, 4068937, 4068943, 4068957, 4068960, 4068961, 4068968, 4068973, 4068976, 4068984, 4068985, 4068996, 4069008, 4069015, 4069021, 4069031, 4069034, 4069044, 4069052, 4069056, 4069059, 4069060, 4069135, 4069136, 4069139, 4069140, 4069146, 4069149, 4069150, 4069152, 4069159, 4069170, 4069173, 4069187, 4069192, 4069193, 4069197, 4069200, 4069204, 4069206, 4069207, 4069208, 4069210, 4069215, 4069218, 4069220, 4069246, 4069249, 4069253, 4069259, 4069264, 4069268, 4069270, 4069271, 4069273, 4069274, 4069276, 4069278, 4069283, 4069287, 4069289, 4069291, 4069292, 4069295, 4069296, 4069302, 4069309, 4069379, 4069380, 4069381, 4069385, 4069389, 4069391, 4069395, 4069396, 4069400, 4069403, 4069404, 4069405, 4069410, 4069421, 4069430, 4069436, 4069439, 4069452, 4069466, 4069468, 4069477, 4069484, 4069495, 4069498, 4069529, 4069530, 4069534, 4069535, 4069541, 4069543, 4069544, 4069545, 4069619, 4069620, 4069628, 4069630, 4069631, 4069636, 4069639, 4069646, 4069652, 4069655, 4069663, 4069665, 4069672, 4069675, 4069679, 4069687, 4069692, 4069698, 4069701, 4069710, 4069714, 4069725, 4069726, 4069740, 4069757, 4069769, 4069774, 4069776, 4069781, 4069812, 4069853, 4069859, 4069860, 4069870, 4069871, 4069875, 4069878, 4069880, 4069883, 4069894, 4069898, 4069904, 4069905, 4069907, 4069908, 4069910, 4069917, 4069922, 4069925, 4069930, 4069932, 4069940, 4069947, 4069953, 4069955, 4069956, 4069958, 4069964, 4069966, 4069968, 4069976, 4069980, 4069981, 4069989, 4069997, 4069998, 4069999, 4070001, 4070005, 4070007, 4070012, 4070020, 4070032, 4070033, 4070051, 4070099, 4070100, 4070109, 4070110, 4070114, 4070119, 4070120, 4070126, 4070130, 4070131, 4070136, 4070158]


CREATE TABLE tweetcollection.countsThesis (
round bigint,
word text,
country text,
count bigint,
totalnumofwords bigint,
PRIMARY KEY (round, word, country)
);
CREATE TABLE tweetcollection.eventsThesis (
round bigint,
country text,
word text,
incrementpercent double,
PRIMARY KEY (round, country, word)
);
CREATE TABLE tweetcollection.processtimeskeybasedThesis (
row int,
column int,
id int,
PRIMARY KEY (row, column)
);
CREATE TABLE tweetcollection.processedThesis (
round bigint,
boltid int,
finished boolean,
PRIMARY KEY (round, boltid)
);


TRUNCATE countsThesis; TRUNCATE eventsThesis; TRUNCATE processedThesis; TRUNCATE processtimeskeybasedThesis;




CREATE TABLE tweetcollection.countsforexperimentSleep_thesis (
round bigint,
word text,
country text,
count bigint,
totalnumofwords bigint,
PRIMARY KEY (round, word, country)
);
CREATE TABLE tweetcollection.eventsSleepThesis (
round bigint,
country text,
word text,
incrementpercent double,
PRIMARY KEY (round, country, word)
);
CREATE TABLE tweetcollection.processtimesSleepThesis (
row int,
column int,
id int,
PRIMARY KEY (row, column)
);
CREATE TABLE tweetcollection.processedforexperimentSleep_thesis (
round bigint,
boltid int,
finished boolean,
PRIMARY KEY (round, boltid)
);

TRUNCATE countsforexperimentSleep_thesis; TRUNCATE eventsforexperimentSleep_thesis; TRUNCATE processedforexperimentSleep_thesis; TRUNCATE processtimeskeybasedforexperimentSleep_thesis;







TRUNCATE clustershybridThesis; TRUNCATE countshybridThesis; TRUNCATE eventskeybasedhybridThesis; TRUNCATE eventshybridThesis; TRUNCATE processedhybridThesis; TRUNCATE processtimeshybridThesis; TRUNCATE tweetsandclusterhybrid_thesis;
TRUNCATE clusterThesis; TRUNCATE eventclusterThesis; TRUNCATE eventsThesis; TRUNCATE processedtweetsThesis; TRUNCATE processtimesThesis; TRUNCATE processtimesThesis;
TRUNCATE countsThesis; TRUNCATE eventsThesis; TRUNCATE processedThesis; TRUNCATE processtimeskeybasedThesis;
TRUNCATE countsforexperimentSleep_thesis; TRUNCATE eventsforexperimentSleep_thesis; TRUNCATE processedforexperimentSleep_thesis; TRUNCATE processtimeskeybasedforexperimentSleep_thesis;




select count(*) from eventshybridThesis;
select count(*) from eventclusterThesis;
select count(*) from eventsThesis;


select count(*) from eventshybridforexperiment_paper1;
select count(*) from eventclusterforexperiment_paper1;
select count(*) from eventsforexperiment_paper1;

select count(*) from eventshybridforexperiment5;
select count(*) from eventclusterforexperiment5;
select count(*) from eventsforexperiment5;

./cqlsh -f resCluster.txt > outputCluster.txt
./cqlsh -f resHybrid.txt > outputHybrid.txt
./cqlsh -f resKeybased.txt > outputKeybased.txt
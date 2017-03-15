# twitterEventDetectionClustering

CREATE TABLE tweetcollection.cluster_daily (
    id timeuuid,
    cosinevector map<text, double>,
    numberoftweets int,
    lastround bigint,
    country text,
    PRIMARY KEY (country, id)
);

CREATE TABLE tweetcollection.clusterinfo_daily (
    round bigint,
    id timeuuid,
    numberoftweets int,
    country text,
    PRIMARY KEY (round, country, id)
);

CREATE TABLE tweetcollection.clusterandtweet_daily (
    clusterid timeuuid,
    tweetid bigint,
    PRIMARY KEY (clusterid, tweetid)
);

CREATE TABLE tweetcollection.eventcluster_daily (
    round bigint,
    clusterid timeuuid,
    country text,
    cosinevector map<text, double>,
    incrementrate double,
    numtweet int,
    PRIMARY KEY (round, clusterid)
);

CREATE TABLE tweetcollection.processedtweets (
    round bigint,
    boltid int,
    boltprocessed bigint,
    country text,
    finished boolean,
    spoutsent bigint,
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



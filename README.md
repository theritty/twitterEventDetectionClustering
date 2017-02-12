# twitterEventDetectionClustering

CREATE TABLE tweetcollection.cluster2 (
    id timeuuid,
    cosinevector map<text, double>,
    numberoftweets int,
    country text,
    lastTweetTime bigint,
    PRIMARY KEY (country, id)
);

CREATE TABLE tweetcollection.clusterinfo2 (
    round bigint,
    country text,
    id timeuuid,
    numberoftweets int,
    PRIMARY KEY (round, country, id)
);

CREATE TABLE tweetcollection.clusterandtweet2 (
    clusterid timeuuid,
    tweetid bigint,
    PRIMARY KEY (clusterid, tweetid)
);

CREATE TABLE tweetcollection.eventcluster2 (
    round bigint,
    clusterid timeuuid,
    country text,
    cosinevector map<text, double>,
    incrementrate double,
    numtweet int,
    PRIMARY KEY (round, clusterid)
);

TRUNCATE eventcluster2; TRUNCATE cluster2; TRUNCATE clusterandtweet2; TRUNCATE clusterinfo2;


# twitterEventDetectionClustering

CREATE TABLE tweetcollection.cluster (
    round bigint,
    id timeuuid,
    cosinevector map<text, double>,
    numberoftweets int,
    PRIMARY KEY (round, id)
);


CREATE TABLE tweetcollection.clusterandtweet (
    clusterid timeuuid,
    tweetid bigint,
    PRIMARY KEY (clusterid, tweetid)
);
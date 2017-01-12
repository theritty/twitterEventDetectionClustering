# twitterEventDetectionClustering

CREATE TABLE tweetcollection.cluster (
    round bigint,
    id timeuuid,
    cosinevector list<int>,
    PRIMARY KEY (round, id)
)


CREATE TABLE tweetcollection.clusterandtweet (
    clusterid timeuuid,
    tweetid bigint,
    PRIMARY KEY (clusterid)
)
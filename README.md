# twitterEventDetectionClustering

CREATE TABLE tweetcollection.cluster (
    id timeuuid,
    round bigint,
    cosinevector list<int>,
    PRIMARY KEY (id)
)


CREATE TABLE tweetcollection.clusterandtweet (
    clusterid timeuuid,
    tweetid bigint,
    PRIMARY KEY (clusterid)
)
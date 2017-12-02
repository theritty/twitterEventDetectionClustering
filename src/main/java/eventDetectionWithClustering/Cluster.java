package eventDetectionWithClustering;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

/**
 * Created by ozlemcerensahin on 02/12/2017.
 */
public class Cluster {
    public String country;
    public UUID id;
    public HashMap<String, Double> cosinevector;
    public int currentnumtweets;
    public long lastround;
    public int prevnumtweets;
    public List<Long> tweetList;
    public Cluster(String country, UUID id, HashMap<String, Double> cosinevector, int currentnumtweets, long lastround, int prevnumtweets) {
        this.country = country;
        this.id = id;
        this.cosinevector = cosinevector;
        this.currentnumtweets = currentnumtweets;
        this.lastround = lastround;
        this.prevnumtweets = prevnumtweets;
        this.tweetList = new ArrayList<>();
    }
}
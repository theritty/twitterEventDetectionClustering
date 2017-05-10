package eventDetector.algorithms;

import cassandraConnector.CassandraDaoKeyBased;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by ozlemcerensahin on 10/05/2017.
 */
public class TFIDFCalculatorWithCassandraKeyBased {
    /**
     * @return term frequency of term in document
     */
    public double tf(CassandraDaoKeyBased cassandraDao, long round, String country, String term) {
        CountCalculatorKeyBased countCalculator = new CountCalculatorKeyBased();
        HashMap<String, Double> hm = countCalculator.getCountOfWord(cassandraDao, term, round, country);
        if(hm == null) return 0;
        if(hm.get("totalnumofwords") == 0)
            System.out.println("Term " + term + " total num zero");
        return hm.get("count") / hm.get("totalnumofwords");
    }

    /**
     * @return the inverse term frequency of term in documents
     */
    public double idf(CassandraDaoKeyBased cassandraDao, ArrayList<Long> rounds, String country, String term) {

        double wordCount=0L, totalNumOfWords=0L;
        for (long r : rounds) {
            CountCalculatorKeyBased countCalculator = new CountCalculatorKeyBased();
            HashMap<String, Double> hm = countCalculator.getCountOfWord(cassandraDao, term, r, country);
            if(hm == null) return 0;
            wordCount += hm.get("count");
            totalNumOfWords += hm.get("totalnumofwords");
        }
        return Math.log(totalNumOfWords / wordCount);
    }

    /**
     * @return the TF-IDF of term
     */
    public double tfIdf(CassandraDaoKeyBased cassandraDao, ArrayList<Long> rounds, String term, long roundNum, String country) {
        double tf = tf(cassandraDao, roundNum, country, term) ;
        if(tf == 0)  return 0;
        double idf = idf(cassandraDao, rounds, country,term);
        double result = tf * idf;
        return result;
    }



}

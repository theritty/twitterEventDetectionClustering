package algorithms;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;

public class CosineSimilarity implements Serializable {

    /**
     * Method to calculate cosine similarity between two documents.
     * @param docVector1 : document vector 1 (a)
     * @param docVector2 : document vector 2 (b)
     * @return
     */

    public static double cosineSimilarity(ArrayList<Integer> docVector1, ArrayList<Integer> docVector2) {
        double dotProduct = 0.0;
        double magnitude1 = 0.0;
        double magnitude2 = 0.0;
        double cosineSimilarity = 0.0;

        for (int i = 0; i < docVector1.size(); i++) //docVector1 and docVector2 must be of same length
        {
            dotProduct += docVector1.get(i) * docVector2.get(i);  //a.b
            magnitude1 += Math.pow(docVector1.get(i), 2);  //(a^2)
            magnitude2 += Math.pow(docVector2.get(i), 2); //(b^2)
        }

        magnitude1 = Math.sqrt(magnitude1);//sqrt(a^2)
        magnitude2 = Math.sqrt(magnitude2);//sqrt(b^2)

        if (magnitude1 != 0.0 || magnitude2 != 0.0) {
            cosineSimilarity = dotProduct / (magnitude1 * magnitude2);
        }

        return cosineSimilarity;
    }

    public double cosineSimilarityFromMap(Map<String, Double> docVector1, ArrayList<String> docVector2, double magnitude2) {
        double dotProduct = 0.0;
        double magnitude1 = 0.0;
        double cosineSimilarity;

        HashSet<String> intersection = new HashSet<>(docVector1.keySet());
        intersection.retainAll(docVector2);

        //Calculate dot product
        for (String item : intersection) {
            double value = docVector1.get(item);
            dotProduct += value ;
        }
        for (String key : docVector1.keySet()) {
            magnitude1 += Math.exp(Math.log(docVector1.get(key))*2) ;
        }
        magnitude1 = Math.sqrt(magnitude1);//sqrt(a^2)

        if (Math.abs(magnitude1) < 0.001 && Math.abs(magnitude2) < 0.001) {
            return 0.0;
        } else {
            cosineSimilarity = dotProduct / (magnitude1 * magnitude2);
        }
        return cosineSimilarity;
    }

    public double cosineSimilarityFromMap(Map<String, Double> docVector1, Map<String, Double> docVector2) {
        double dotProduct = 0.0;
        double magnitude1 = 0.0;
        double magnitude2 = 0.0;
        double cosineSimilarity;
        double numTweets1 = 0.0;
        double numTweets2 = 0.0;

        if(docVector1.containsKey("numTweets")) {
            numTweets1 = docVector1.get("numTweets");
            docVector1.remove("numTweets");
        }
        if(docVector2.containsKey("numTweets")) {
            numTweets2 = docVector2.get("numTweets");
            docVector2.remove("numTweets");
        }


        HashSet<String> intersection = new HashSet<>(docVector1.keySet());
        intersection.retainAll(docVector2.keySet());

        //Calculate dot product
        for (String item : intersection) {
            double value = docVector1.get(item);
            dotProduct += value * docVector2.get(item);
        }
        for (String key : docVector1.keySet()) {
            magnitude1 += Math.exp(Math.log(docVector1.get(key))*2) ;
        }
        magnitude1 = Math.sqrt(magnitude1);//sqrt(a^2)
        for (String key : docVector2.keySet()) {
            magnitude2 += Math.exp(Math.log(docVector2.get(key))*2) ;
        }
        magnitude2 = Math.sqrt(magnitude2);//sqrt(a^2)

        if (Math.abs(magnitude1) < 0.001 && Math.abs(magnitude2) < 0.001) {
            return 0.0;
        } else {
            cosineSimilarity = dotProduct / (magnitude1 * magnitude2);
        }

        docVector1.put("numTweets", numTweets1);
        docVector2.put("numTweets", numTweets2);

        return cosineSimilarity;
    }
}

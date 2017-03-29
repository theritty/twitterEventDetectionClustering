
package eventDetector.algorithms;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;

public class CosineSimilarity implements Serializable {

    /**
     * Method to calculate cosine similarity between two documents.
     * @param docVector1 : document vector 1 (a)
     * @param docVector2 : document vector 2 (b)
     * @return
     */
    public double cosineSimilarityFromMap(Map<String, Double> docVector1, Map<String, Double> docVector2, double magnitude2) {
        double dotProduct = 0.0;
        double magnitude1 = 0.0;
        double cosineSimilarity;

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

        if (Math.abs(magnitude1) < 0.001 && Math.abs(magnitude2) < 0.001) {
            return 0.0;
        } else {
            cosineSimilarity = dotProduct / (magnitude1 * magnitude2);
        }
        return cosineSimilarity;
    }
}

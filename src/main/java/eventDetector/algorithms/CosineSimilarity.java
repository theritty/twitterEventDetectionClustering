package eventDetector.algorithms;

import java.util.HashMap;
import java.util.Map;

public class CosineSimilarity {

    /**
     * Method to calculate cosine similarity between two documents.
     * @param docVector1 : document vector 1 (a)
     * @param docVector2 : document vector 2 (b)
     * @return
     */
    public static double cosineSimilarityFromMap(HashMap<String, Double> docVector1, HashMap<String, Double> docVector2) {
        double dotProduct = 0.0;
        double magnitude1 = 0.0;
        double magnitude2 = 0.0;
        double cosineSimilarity = 0.0;

        for(Map.Entry<String, Double> entry : docVector1.entrySet()) {
            String key = entry.getKey();
            double value = entry.getValue();

            if(docVector2.get(key) != null)
                dotProduct += value * docVector2.get(key);
            magnitude1 += Math.pow(value, 2);
        }
        for(Map.Entry<String, Double> entry : docVector2.entrySet()) {
            double value = entry.getValue();
            magnitude2 += Math.pow(value, 2);
        }

        magnitude1 = Math.sqrt(magnitude1);//sqrt(a^2)
        magnitude2 = Math.sqrt(magnitude2);//sqrt(b^2)

        if (magnitude1 != 0.0 | magnitude2 != 0.0) {
            cosineSimilarity = dotProduct / (magnitude1 * magnitude2);
        } else {
            return 0.0;
        }
        return cosineSimilarity;
    }
}

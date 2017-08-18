package sparkinterface;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;

public class VectorClojure {
    public static Vector dense(String[] args) {
        double[] prompt = new double[args.length];
        for (int i =0; i<args.length; i++){
            prompt[i] = Double.parseDouble(args[i]);
        }
        Vector denseVec = Vectors.dense(prompt);
        return denseVec;
    }
}

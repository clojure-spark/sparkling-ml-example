package sparkinterface;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.StreamingLinearRegressionWithSGD;

public class VectorClojure {
    
    public static Vector dense(String[] args) {
        double[] prompt = new double[args.length];
        for (int i =0; i<args.length; i++){
            prompt[i] = Double.parseDouble(args[i]);
        }
        Vector denseVec = Vectors.dense(prompt);
        return denseVec;
    }
    
    public static StreamingLinearRegressionWithSGD model(int num, int iter) {
//        StreamingLinearRegressionWithSGD slr = new StreamingLinearRegressionWithSGD()
//            .setNumIterations(2)
//            .setInitialWeights(Vectors.dense(0.0));
        StreamingLinearRegressionWithSGD model = new StreamingLinearRegressionWithSGD()
            .setStepSize(0.5)
            .setNumIterations(10)
            .setInitialWeights(Vectors.dense(0.0));
//        StreamingLinearRegressionWithSGD slr = new StreamingLinearRegressionWithSGD()
//            .setNumIterations(iter)
//            .setInitialWeights(Vectors.dense(0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0))
//            .setStepSize(0.01);
        return model;
    }
    
}

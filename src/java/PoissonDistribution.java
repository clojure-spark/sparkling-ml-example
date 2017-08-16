package distributions;

import cern.jet.random.Poisson;
import cern.jet.random.engine.DRand;
import cern.jet.random.engine.RandomEngine;

import java.util.Random;


/*
(def distribution (PoissonDistribution. (float 10)))
(.getNextValue distribution) ;;=> 9
(.getLambda distribution) ;;=> 10.0
 * Created by augusto on 9/17/16.
 */
public class PoissonDistribution implements DistributionInterface {

    private final Float lambda;
    private final Poisson poissonGenerator;

    public PoissonDistribution(Float lambda) {
        Random randomSeedGenerator = new Random();
        RandomEngine engine = new DRand(randomSeedGenerator.nextInt());

        this.lambda = lambda;
        this.poissonGenerator = new Poisson(lambda, engine);
    }

    @Override
    public Integer getNextValue() {
        return poissonGenerator.nextInt();
    }


    public Float getLambda() {
        return lambda;
    }
}

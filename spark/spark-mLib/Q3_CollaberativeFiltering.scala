
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating


//used in recommendation system to fill out the missing parameters or user suggestions using kNN
//spark-mil -> supports model-based collaborative filtering, in which users and products 
//are described by a small set of latent factors that can be used to predict missing entries.
//ALS (alternating least squares) to learn latent factors

object Q3_CollaberativeFiltering {
def main(args: Array[String]) {
  
  val sc = new SparkContext("local[*]", "ALS")
  val data = sc.textFile("/Users/devendralad/Desktop/ratings.dat")
    
  val ratings = data.map(_.split("::") match { case Array(userID, movieID, ratings,timestamp) =>
  Rating(userID.toInt, movieID.toInt, ratings.toDouble)
    })
  val splits = ratings.randomSplit(Array(0.6, 0.4), seed = 11L)
  val training=splits(0)
  val testing=splits(1)
  //number of latent factors in the model  
  val rank = 5
  //number of iterations for ALS to converge
  val numIterations = 10
  val model = ALS.train(training, rank, numIterations, 0.01)

  val usersProducts = testing.map { case Rating(user, product, rate) =>
  (user, product)}
  
  val predictions = model.predict(usersProducts).map { case Rating(user, product, rate) =>
    ((user, product), rate)}
  
  val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>
    ((user, product), rate)}.join(predictions)
    
  val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
  val err = (r1 - r2)
  err * err}.mean()
  
println("Mean Squared Error for ALS model is= " + MSE + "%")
println("Therefore, Accuracy is= "+ (100-MSE) + "%")
  }
}




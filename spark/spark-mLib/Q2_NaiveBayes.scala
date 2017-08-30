
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;

object Q2_NaiveBayes {
  def main(args: Array[String]) {
    
    val sc = new SparkContext("local[*]", "NB")
    val data = sc.textFile("/Users/devendralad/Desktop/glass.data")
  
    val classifyData = data.map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(10).toDouble, Vectors.dense(parts(0).toDouble, parts(1).toDouble, parts(2).toDouble, parts(3).toDouble, parts(4).toDouble, parts(5).toDouble, parts(6).toDouble, parts(7).toDouble, parts(8).toDouble, parts(9).toDouble))
    }

    val splits = classifyData.randomSplit(Array(0.6, 0.4), seed = 11L)
    val trainingData = splits(0)
    val testData = splits(1)
    //lambda - laplas smoothing parameter
    val model = NaiveBayes.train(trainingData, lambda = 1.0)

    val predictionAndLabel = testData.map(p => (model.predict(p.features), p.label))
    val accuracy = 100.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / testData.count()
    println("Accuracy of Naive Bayes Classifier is= " + accuracy + "%")
  }
}
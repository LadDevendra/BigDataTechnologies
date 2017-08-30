
import org.apache.spark.SparkContext
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.impurity.Gini

object Q2_DecisionTree {
  def main(args: Array[String]) {
    
  val sc = new SparkContext("local[*]", "DecisionTree")
  val data = sc.textFile("/Users/devendralad/Desktop/glass.data")
  
    val classifyData = data.map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(10).toDouble, Vectors.dense(parts(0).toDouble, parts(1).toDouble, parts(2).toDouble, parts(3).toDouble, parts(4).toDouble, parts(5).toDouble, parts(6).toDouble, parts(7).toDouble, parts(8).toDouble, parts(9).toDouble))
    }

    val splits = classifyData.randomSplit(Array(0.6, 0.4), seed = 11L)
    val trainingData = splits(0)
    val testData = splits(1)

    val numClasses = 8
    val categoricalFeaturesInfo = Map[Int, Int]()
    //used to choose between candidates splits, decided on algo we are using 
    val impurity = "gini"
    //maximum depth of a tree
    val maxDepth = 5
    //Increasing maxBins allows the algorithm to consider more split candidates and make fine-grained split decisions. 
    val maxBins = 100

    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, impurity, maxDepth, maxBins)

    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val accuracy = 100.0 * labelAndPreds.filter(r => r._1 == r._2).count.toDouble / testData.count
    println("Accuracy of Decision Tree classifier is= " + accuracy + "%")
  }
}
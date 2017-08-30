
import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.mllib.clustering.{ KMeans, KMeansModel }
import org.apache.spark.mllib.linalg.Vectors



object Q1 {
 
  def main(args: Array[String]) {
    
    val sc = new SparkContext("local[*]", "kmeans")
  val data = sc.textFile("/Users/devendralad/Desktop/itemusermat")

    val parsedData = data.map(s => Vectors.dense(s.split(' ').drop(1).map(_.toDouble))).cache()

    val numClusters = 10
    val numIterations = 10
    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    val prediction = data.map { line => (line.split(' ')(0), clusters.predict(Vectors.dense(line.split(' ').drop(1).map(_.toDouble)))) }

    val movie = sc.textFile("/Users/devendralad/Desktop/movies.dat")
    val moviesData = movie.map(line => (line.split("::"))).map(p => (p(0), (p(1) + " , " + p(2))))

    val joinedResult = prediction.join(moviesData)
    val groupedData = joinedResult.map(p => (p._2._1, (p._1, p._2._2))).groupByKey()

    val result = groupedData.map(p => (p._1, p._2.toList))

    val output = result.map(p => (p._1, p._2.take(5)))
    println("ClusterID , First 5 Movies list in the cluster")
    output.foreach(p => println("ClusterID " + p._1 + " , " + p._2.mkString("")))
  }
}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.collection.mutable.Map
import scala.math.sqrt

object Kmeans {
  def calculateDistance(p1 : Array[Double], p2 : Array[Double]) : Double = {
    def square (x : Double) = { x*x }
    return sqrt(square(p1(0)-p2(0)) + square(p1(1)-p2(1)) + square(p1(2)-p2(2)))
  }

  def searchClosestCentroid(centroids : Map[Int, Array[Double]], point : Array[Double]) : Int = {
    var idx : Int = 0
    var dist : Double = Double.PositiveInfinity
    var tmpDist : Double = 0
    for (i <- 1 to centroids.size) {
      val centroid = centroids(i)
      tmpDist = calculateDistance(point, centroid)
      if (tmpDist < dist) {
        idx = i
        dist = tmpDist
      }
    }
    return idx
  }

  def sumPoints(p1 : Array[Double], p2 : Array[Double]) : Array[Double] = {
    return Array(p1(0)+p2(0), p1(1)+p2(1), p1(2)+p2(2))
  }

  def dividePoint(p : Array[Double], num : Long) : Array[Double] = {
    return Array(p(0)/num, p(1)/num, p(2)/num)
  }

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: KMeans <input_file> <output_file> <mode> <k>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("KMeans").set("spark.cores.max", "3")
    val sc = new SparkContext(sparkConf)
    // val lines is base RDD
    val lines = sc.textFile(args(0))
    val mode = args(2).toInt

    var K: Int = 0
    var centroids: Map[Int, Array[Double]] = Map()
    val points = lines.distinct().map(s => (s.split(",")).map(x => x.toDouble))
    points.cache()

    // Set initial centroids
    if (mode == 0) {
      // Randomly sample K data points
      K = args(3).toInt
      val randomPoints = points.takeSample(false, 3, System.nanoTime.toInt)
      for (i <- 1 to K) {
        centroids += (i -> randomPoints(i-1))
      }
    }
    else {
      // User-defined centroids
      centroids = Map(1 -> Array(5, 1.2, -0.8), 2 -> Array(-3.2, -1.1, 3.0), 3 -> Array(-2.1, 5.1, 1.1))
      K = centroids.size
    }

    var finalPoints = points.map(p => (1, p)).collect
    var change : Double = 100
   
    while(change > 0.001) {
      val clusteredPoints = points.map(p => (searchClosestCentroid(centroids, p), p))
      clusteredPoints.cache()

      // Find average of each cluster and change centroids.
      val sumOfClusterElem = clusteredPoints.reduceByKey((a, b) => sumPoints(a, b)).collect.toList.groupBy(_._1).mapValues(_.map(_._2))
      val numOfClusterElem = clusteredPoints.countByKey
      val newCentroids : Map[Int, Array[Double]] = Map()
      for (i <- 1 to K) {
        newCentroids += (i -> dividePoint(sumOfClusterElem.getOrElse(i, null)(0), numOfClusterElem.getOrElse(i, -1)))
      }

      // Calculate change value
      change = 0
      for (i <- 1 to K) {
        change = change + calculateDistance(centroids(i), newCentroids(i))
      }
      centroids = newCentroids
      finalPoints = clusteredPoints.collect
    }

    val result = sc.parallelize(finalPoints)
    val sortedResult = result.map{ case (k, Array(p1, p2, p3)) => ((p1, p2, p3), k)}.sortByKey()
      .map{ case ((p1, p2, p3), k) => (k, Array(p1, p2, p3)) }.sortByKey()
    val formattedResult = sortedResult.map{ case (k, v) => (k, v.mkString(",")) }.map(tuple => tuple.productIterator.mkString("\t"))
    formattedResult.saveAsTextFile(args(1))
  }
}

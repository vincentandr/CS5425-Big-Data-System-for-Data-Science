import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions

import annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/** A raw posting, either a question or an answer */
case class Posting(postingType: Int, id: Int, parentId: Option[Int], score: Int, tags: Option[String]) extends Serializable
case class Result(position: (Int, Int), size: Int, average: Int, median: Int) extends Serializable

/** The main class */
object Assignment2 extends Assignment2 {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Assignment2")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  //sc.setLogLevel("WARN")

  /** Main function */
  def main(args: Array[String]): Unit = {

    val lines   = sc.textFile("files/QA_data.csv")
    val raw     = rawPostings(lines)
    val grouped = groupedPostings(raw)
    val scored  = scoredPostings(grouped)
    val vectors = vectorPostings(scored)

    vectors.cache()

    val means   = kmeans(sampleVectors(vectors), vectors, 0, debug = true)
    val results = clusterResults(means)
    printResults(results)
  }
}


/** The parsing and kmeans methods */
class Assignment2 extends Serializable {

  /** Languages */
  val Domains =
    List(
      "Machine-Learning", "Compute-Science", "Algorithm", "Big-Data", "Data-Analysis", "Security", "Silicon Valley", "Computer-Systems",
      "Deep-learning", "Internet-Service-Providers", "Programming-Language", "Cloud-services", "Software-Engineering", "Embedded-System", "Architecture")


  /** K-means parameter: How "far apart" languages should be for the kmeans algorithm? */
  def DomainSpread = 50000
  assert(DomainSpread > 0)

  /** K-means parameter: Number of clusters */
  def kmeansKernels = 45

  /** K-means parameter: Convergence criteria, if changes of all centriods < kmeansEta, stop*/
  def kmeansEta: Double = 20.0D

  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 120


  //
  //
  // Parsing utilities:
  //
  //

  /** Load postings from the given file */
  def rawPostings(lines: RDD[String]): RDD[Posting] =
    lines.map(line => {
      val arr = line.split(",")
      Posting(postingType =    arr(0).toInt,
        id =             arr(1).toInt,
        //acceptedAnswer = if (arr(2) == "") None else Some(arr(2).toInt),
        parentId =       if (arr(2) == "") None else Some(arr(2).toInt),
        score =          arr(3).toInt,
        tags =           if (arr.length >= 5) Some(arr(4).intern()) else None)
    })


  /** Group the questions and answers together */
  def groupedPostings(postings: RDD[Posting]): RDD[(Option[Int], (Int, Option[String]))] = {
    // Filter the questions and answers separately
    // Prepare them for a join operation by extracting the QuestionID value in the first element of a tuple.

    val questions = postings.filter(post => post.postingType == 1)
      .map(post => (Option(post.id), post.tags))

    val answers = postings.filter(post => post.postingType == 2)
      .map(post => (post.parentId, post.score))
      .join(questions)

    answers
  }


  /** Compute the maximum score for each posting */
  def scoredPostings(postings: RDD[(Option[Int], (Int, Option[String]))]): RDD[(Option[Int], (Int, Option[String]))] = {

    //ToDo
    //
    val result = postings.reduceByKey((v1, v2) =>
      if (v1._1 > v2._1) v1 else v2
    )

    result
  }


  /** Compute the vectors for the kmeans */
  def vectorPostings(postings: RDD[(Option[Int], (Int, Option[String]))]): RDD[(Int, Int)]  = {

    //ToDo
    //
    val result = postings.map{

      case (k,(v1,v2)) => (DomainSpread*Domains.indexOf(v2.get),v1)
    }

    result
  }

  //
  //
  //  Kmeans method:
  //
  //

  /** Main kmeans computation */
  @tailrec final def kmeans(samples: Array[(Int, Int)], vectors: RDD[(Int, Int)], idx: Int = 0, debug: Boolean): Array[((Int, Int), Iterable[(Int, Int)])]= {

    //ToDo
    // sum of all distances from their centroids
    def seqOp = (accumulator: Double, element: ((Int, Int), (Int, Int))) =>
      euclideanDistance(element._1, element._2)

    def combOp = (accumulator1: Double, accumulator2: Double) =>
      accumulator1 + accumulator2

    val convergence = vectors.map(d => (samples(findClosest(d,samples)), (samples(findClosest(d,samples)), d)))
      .aggregateByKey(0.0D)(seqOp, combOp)
      .collect()

    var sum = 0.0D

    for(i <- 0 until convergence.length){
      sum += convergence(i)._2
    }

    //check converged
    if(converged(sum) || idx > kmeansMaxIterations){
      vectors.map(d => (samples(findClosest(d,samples)), d))
        .groupByKey()
        .collect()
    }else{
      // update centroids position by averaging
      val groupedVals = vectors.map(d => (samples(findClosest(d,samples)), d))
        .groupByKey()
        .map( d => averageVectors(d._2))
        .collect()

      kmeans(groupedVals, vectors, idx + 1, debug)
    }
  }

  //
  //
  //  Kmeans utilities (Just some cases, you can implement your own utilities.)
  // 
  //

  def sampleVectors(vectors: RDD[(Int, Int)]) : Array[(Int, Int)]= {
    //randomly assign 45 points as centroid (random initial centroids / worse)
    //Random.shuffle(vectors.collect().toList).take(kmeansKernels).toArray

    //decide on 45 centroids using kmeans++ algorithm (better initial centroids)
    val initCentroid = Random.shuffle(vectors.collect().toList).take(1).head
    var centroid = ArrayBuffer[(Int, Int)]()
    centroid += initCentroid

    for (i <- 0 until kmeansKernels-1) {
      val dists = vectors.map(x => {
        val closestCentroid = centroid(findClosest(x, centroid.toArray))
        (x, euclideanDistance(x, closestCentroid))
      })
      centroid += dists.max()(new Ordering[Tuple2[(Int, Int), Double]]() {
        override def compare(x: ((Int, Int), Double), y: ((Int, Int), Double)): Int =
          x._2.compare(y._2)
      })._1
    }
    centroid.toArray
  }

  /** Decide whether the kmeans clustering converged */
  def converged(distance: Double) = distance < kmeansEta


  /** Return the euclidean distance between two points */
  def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double = {
    val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
    val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
    math.sqrt(part1 + part2)
  }

  /** Return the euclidean distance between two points */
  def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
    assert(a1.length == a2.length)
    var sum = 0d
    var idx = 0
    while(idx < a1.length) {
      sum += euclideanDistance(a1(idx), a2(idx))
      idx += 1
    }
    sum
  }

  /** Return the closest point */
  def findClosest(p: (Int, Int), centers: Array[(Int, Int)]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until centers.length) {
      val tempDist = euclideanDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }


  /** Average the vectors */
  def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) = {
    val iter = ps.iterator
    var count = 0
    var comp1: Long = 0
    var comp2: Long = 0
    while (iter.hasNext) {
      val item = iter.next
      comp1 += item._1
      comp2 += item._2
      count += 1
    }
    ((comp1 / count).toInt, (comp2 / count).toInt)
  }

  def computeMedian(a: Iterable[(Int, Int)]) = {
    val s = a.map(x => x._2).toArray
    val length = s.length
    val (lower, upper) = s.sortWith(_<_).splitAt(length / 2)
    if (length % 2 == 0) (lower.last + upper.head) / 2 else upper.head
  }

  def computeAverage(a: Iterable[(Int, Int)])= {
    val s = a.map(x => x._2).toArray
    s.sum / s.length
  }

  def clusterResults(means: Array[((Int, Int), Iterable[(Int, Int)])]): Array[Result] = {
    var resultArr = ArrayBuffer[Result]()
    for (i <- 0 until means.length) {
      val clusterCentroid = means(i)._1
      val clusterSize = means(i)._2.size
      val clusterAvg = computeAverage(means(i)._2)
      val clusterMedian = computeMedian(means(i)._2)
      resultArr += Result(clusterCentroid, clusterSize, clusterAvg, clusterMedian)
    }
    resultArr.toArray
  }

  //  Displaying results:

  def printResults(res: Array[Result])  = {
    for(i <- 0 until res.length)
      println("Centroid no: " + (i+1) + ", Position: " + res(i).position + ", Size: " + res(i).size + ", Average: " + res(i).average + ", Median: " + res(i).median)
  }
}
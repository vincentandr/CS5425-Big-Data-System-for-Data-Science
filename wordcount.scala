import org.apache.spark.{SparkConf, SparkContext}

object wordcount {
  def main(args:Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("common words")

    val sc = new SparkContext(conf)

    //read from file
    val stopwords_file = sc.textFile("files/stopwords.txt")
    val file = sc.textFile("files/task1-input1.txt")
    val file2 = sc.textFile("files/task1-input2.txt")

    val stopwords = stopwords_file.collect()

    //filter stopwords and count (mapreduce) no. of words on file 1
    val counts1 = file.flatMap(line => line.split(" "))
      .map(word => word.toLowerCase())
      .filter(!stopwords.contains(_))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    //filter stopwords and count (mapreduce) no. of words on file 2
    val counts2 = file2.flatMap(line => line.split(" "))
      .map(word => word.toLowerCase())
      .filter(!stopwords.contains(_))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .join(counts1) //join with file 1
      .map{ //only take common words with smaller count from both files
        case (k, (v1, v2)) => if (v1 < v2) (k,v1) else (k, v2)
      }

    //sort in descending order and take top 15 words
    val result = counts2.sortBy(_._2, false).take(15)

    sc.parallelize(result).saveAsTextFile("wordcount_output")

  }
}

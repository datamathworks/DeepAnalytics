import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]) : Unit = {
      val conf = new SparkConf().setAppName("Word Count").setMaster("local")
      val sc = new SparkContext(conf);

      // reading the file and creating an rdd of rows
      val inputFile = sc.textFile("data/macbeth.txt")
                        /* sample output:
                            Actus
                            Primus.
                            Scoena
                            Prima. */
                        .flatMap(_.split(" "))
                        /* sample output:
                            actus
                            primus
                            scoena
                            prima  */
                        .map(_.replaceAll("[,.!?:;]", "")
                              .trim
                              .toLowerCase)
                        .filter(!_.isEmpty)

                        /* sample output:
                            (actus,1)
                            (primus,1)
                            (scoena,1)
                            (prima,1)
                            (thunder,1) */
                       .map(word => (word, 1))

                        /* sample output:
                            (masking,1)
                            (fantasticall,2)
                            (battlements,2)
                            (doo't,2)
                            (oppos'd,1) */
                       .reduceByKey(_ + _)
                        /* sample output:
                            (1,masking)
                            (2,fantasticall)
                            (2,battlements)
                            (2,doo't)
                            (1,oppos'd)*/
                       .map{case(word, count) => (count,word)}
                       /* sample output:
                           (644,the)
                           (544,and)
                           (383,to)
                           (336,of)
                           (330,i)*/
                       .sortByKey(ascending = false)
                      /* sample output:
                           (the,644)
                           (and,544)
                           (to,383)
                           (of,336)
                           (i,330)*/
                       .map{case(count,word) => (word,count)}
                      /* sample output:
                          (the,644,3)
                          (and,544,3)
                          (to,383,2)
                          (of,336,2)
                          (i,330,1)*/
                       .map{case(word,count) => (word,count,word.length)}
                       .collect() take 20 foreach println;


  }
}
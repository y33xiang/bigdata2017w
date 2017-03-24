/**
  * Bespin: reference implementations of "big data" algorithms
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package ca.uwaterloo.cs.bigdata2017w.assignment6

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.collection.mutable._
import org.apache.hadoop.fs._



class Conf(args: Seq[String]) extends ScallopConf(args){
  mainOptions = Seq(input,model)
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "output directory", required = true)
  val shuffle = opt[Boolean]()
  verify()
}

object TrainSpamClassifier {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.model())

    val conf = new SparkConf().setAppName("TrainSpamClassifier")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.model())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)




    val delta = 0.002
    val rand = scala.util.Random
    val textFile = sc.textFile(args.input(),1)

    val w = Map[Int, Double]()

    def spamminess(features: Array[Int]) : Double = {
      var score = 0d
      features.foreach(f => if (w.contains(f)) score += w(f))
      score
    }




    if(args.shuffle()){



           val trained = textFile.map(line => {

             val key = rand.nextInt
             (key, line)
           }).sortByKey()
             .map(line =>{
               val tokens = line._2.split(" ")
               val docid = tokens(0)
               val isSpam = tokens(1)
               val features = tokens.drop(2).map(_.toInt)
           //    (line._1,(docid,isSpam,features))
               (0,(docid,isSpam,features))
             })
             .groupByKey(1)
             .flatMap(p =>{
               p._2.foreach(v =>{
                 val isSpam = if(v._2 =="spam") 1 else 0
                 val features = v._3
                 val score = spamminess(features)
                 val prob = 1.0 / (1 + Math.exp(-score))

                 features.foreach(f => {
                   if (w.contains(f)) {
                     w(f) += (isSpam - prob) * delta
                   } else {
                     w(f) = (isSpam - prob) * delta
                   }
                 })
               })
               w
             })


           trained.saveAsTextFile(args.model())



    }else{


    val trained = textFile.map(line =>{
      val tokens = line.split(" ")
      val docid = tokens(0)
      val isSpam = tokens(1)
    //  val features = tokens.slice(2, tokens.length).map(_.toInt)
      val features = tokens.drop(2).map(_.toInt)
    //  (scala.util.Random,(docid,isSpam, features))
      (0,(docid,isSpam, features))
    })
      .groupByKey(1)
      .flatMap(p =>{
      p._2.foreach(v =>{
        val isSpam = if(v._2 =="spam") 1 else 0
        val features = v._3
        val score = spamminess(features)
        val prob = 1.0 / (1 + Math.exp(-score))

        features.foreach(f => {
          if (w.contains(f)) {
            w(f) += (isSpam - prob) * delta
          } else {
            w(f) = (isSpam - prob) * delta
          }
        })
      })
        w
    })


    trained.saveAsTextFile(args.model())

    }
  }
}


/*    val trained = textFile.map(line =>{
      val key = rand.nextInt
      (key, line)
    }).sortByKey()
      .map(line =>{
        val tokens = line._2.split(" ")
        val docid = tokens(0)
        val isSpam = if(tokens(1)=="spam") 1 else 0
        var featuresBuffer = ArrayBuffer[Int]()
        for(i <-2 until tokens.length){
          featuresBuffer += tokens(i).toInt
        }
        val features = featuresBuffer.toArray
        (0,(docid,isSpam,features))
      }).groupByKey(1)
      .map(pair =>{
        val seaquence = pair._2.iterator
        while(seaquence.hasNext){
          val pair = seaquence.next()
          val isSpam = pair._2
          val features = pair._3
          val score = spamminess(features)
          val prob = 1.0 / (1 + Math.exp(-score))
          features.foreach(f => {
            if (w.contains(f)) {
              w(f) += (isSpam - prob) * delta
            } else {
              w(f) = (isSpam - prob) * delta
            }
          })
        }
      }).flatMap(pair =>{
      w.keys.map(i =>{
        (i,w(i))
      })
    }).saveAsTextFile(args.model())
    */


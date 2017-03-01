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

package ca.uwaterloo.cs.bigdata2017w.assignment5


import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession


class Conf_5(args: Seq[String]) extends ScallopConf(args){
  mainOptions = Seq(input)
  val input = opt[String](descr = "input path", required = true)
  //val date = opt[String](descr = "shipdate", required = true)
  val text = opt[Boolean]()
  val parquet = opt[Boolean]()
  verify()
}

object Q5 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf_5(argv)

    log.info("Input: " + args.input())
 //   log.info("Shipdate: " + args.date())
    //   log.info("File type: " + args.text())

    val conf = new SparkConf().setAppName("Shipdate Query")
    val sc = new SparkContext(conf)

 //  val inputDate = args.date()

    if(args.text()) {
      val lineitemRDD = sc.textFile(args.input() + "/lineitem.tbl")
      val ordersRDD = sc.textFile(args.input() + "/orders.tbl")
      val customerRDD = sc.textFile(args.input() + "/customer.tbl")
      val nationRDD = sc.textFile(args.input() + "/nation.tbl")


      /*   val nationrdd = nationRDD
      .filter(line =>{
        val tokens = line.split("\\|")
        tokens(1).contains("CANADA") || tokens(1).contains("UNITED STATES")
      })
      .map(line => {
        val tokens = line.split("\\|")
        (tokens(0),tokens(1))
      })*/
      //    .foreach(println)

      val nationrdd = nationRDD
        .map(line => {
          val tokens = line.split("\\|")
          (tokens(0), tokens(1))
        })
        .collectAsMap()
      val nationrddHashmap = sc.broadcast(nationrdd)


      val customerrdd = customerRDD
        .map(line => {
          val tokens = line.split("\\|")
          (tokens(0), tokens(3))
        })
        .collectAsMap()
      val customerrddHashmap = sc.broadcast(customerrdd)
      // customerrdd.map(p => (p._2,(p._1,nationrddHashmap.value(p._1))))


      val lineitem = lineitemRDD
        .map(a => {
          val tokens = a.split("\\|")
          (tokens(0), tokens(10).substring(0, 7))
        })


      val ordersrdd = ordersRDD
        .map(line => {
          val tokens = line.split("\\|")
          (tokens(0), tokens(1))
        })
        .map(p => (p._1, customerrddHashmap.value(p._2)))
        .map(p => (p._1, nationrddHashmap.value(p._2)))
        .filter(p => {
          p._2.contains("CANADA") || p._2.contains("UNITED STATES")
        })
        .cogroup(lineitem)
        .filter(p => p._2._2.iterator.hasNext && p._2._1.iterator.hasNext)
        .flatMap(p => {
          val name = p._2._1
          p._2._2.map(date => ((name, date), 1))
        })
        .reduceByKey(_ + _)
        .sortByKey()
        .collect()
        .foreach(p => println("(" + p._1._1.mkString + "," + p._1._2 + "," + p._2 + ")"))
      /*
      .cogroup(lineitem)
      .filter(p => p._2._2.iterator.hasNext)
      .map(p =>(p._1,(p._2._1.iterator.next(),p._2._2.iterator.next())))
      .map(p =>((customerrddHashmap.value(p._2._1),p._2._2),1))
      .reduceByKey(_+_)
      .map(p =>(p._1._1,(p._1._2,p._2)))
      .map(p => nationrddHashmap.value(p._1))
      .foreach(println)
*/

      //  .map(p =>(p._1._1.toInt,(nationrddHashmap.value(p._1._1),(p._1._2,p._2))))
      //  .map(p => nationrddHashmap.value(p._1._1))


      /*   .sortByKey()
      .collect()
      .foreach(p => println("("+ p._1 + "," + p._2._1 + "," + p._2._2+")"))
  */

    }else if(args.parquet()){
      val sparkSession = SparkSession.builder.getOrCreate


      val lineitemRDD = sparkSession.read.parquet(args.input()+"/lineitem")
      val ordersRDD = sparkSession.read.parquet(args.input()+"/orders")
      val customerRDD = sparkSession.read.parquet(args.input()+"/customer")
      val nationRDD = sparkSession.read.parquet(args.input()+"/nation")


      val nationrdd = nationRDD.rdd
        .map(line => {
          (line(0), line(1))
        })
        .collectAsMap()
      val nationrddHashmap = sc.broadcast(nationrdd)


      val customerrdd = customerRDD.rdd
        .map(line => {
          (line(0), line(3))
        })
        .collectAsMap()
      val customerrddHashmap = sc.broadcast(customerrdd)
      // customerrdd.map(p => (p._2,(p._1,nationrddHashmap.value(p._1))))


      val lineitem = lineitemRDD.rdd
        .map(line => {
          (line(0), line(10).toString.substring(0, 7))
        })


      val ordersrdd = ordersRDD.rdd
        .map(line => {
          (line(0), line(1))
        })
        .map(p => (p._1, customerrddHashmap.value(p._2)))
        .map(p => (p._1, nationrddHashmap.value(p._2)))
        .filter(p => {
          p._2.toString.contains("CANADA") || p._2.toString.contains("UNITED STATES")
        })
        .cogroup(lineitem)
        .filter(p => p._2._2.iterator.hasNext && p._2._1.iterator.hasNext)
        .flatMap(p => {
          val name = p._2._1.toString()
          p._2._2.map(date => ((name.toString, date), 1))
        })
        .reduceByKey(_ + _)
        .sortByKey()
        .collect()
        .foreach(p => println("(" + p._1._1.mkString + "," + p._1._2 + "," + p._2 + ")"))

    }




  }
}







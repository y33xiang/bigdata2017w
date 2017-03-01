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


class Conf_3(args: Seq[String]) extends ScallopConf(args){
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "shipdate", required = true)
  val text = opt[Boolean]()
  val parquet = opt[Boolean]()
  verify()
}

object Q3 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf_3(argv)

    log.info("Input: " + args.input())
    log.info("Shipdate: " + args.date())
    //   log.info("File type: " + args.text())

    val conf = new SparkConf().setAppName("Shipdate Query")
    val sc = new SparkContext(conf)

    val inputDate = args.date()


    if(args.text()) {
      val lineitemRDD = sc.textFile(args.input() + "/lineitem.tbl")
      val partRDD = sc.textFile(args.input() + "/part.tbl")
      val supplierRDD = sc.textFile(args.input() + "/supplier.tbl")


      val partrdd = partRDD
        .map(line => {
          val tokens = line.split("\\|")
          (tokens(0), tokens(1))
        })
        .collectAsMap()
      val partrddHashmap = sc.broadcast(partrdd)


      val supplierrdd = supplierRDD
        .map(line => {
          val tokens = line.split("\\|")
          (tokens(0), tokens(1))
        })
        .collectAsMap()
      val supplierrddHashmap = sc.broadcast(supplierrdd)


      val lineitem = lineitemRDD
        .filter(line => {
          val tokens = line.split("\\|")
          tokens(10).contains(inputDate)
        })
        .map(a => {
          val tokens = a.split("\\|")
          (tokens(0).toLong, (tokens(1), tokens(2)))
        })
        .sortByKey()
        .take(20)
        .map(p => (p._1, (partrddHashmap.value(p._2._1), supplierrddHashmap.value(p._2._2))))
        .foreach(p => println("(" + p._1 + "," + p._2._1.toList.mkString + "," + p._2._2.toList.mkString + ")"))
    }else if(args.parquet()){
      val sparkSession = SparkSession.builder.getOrCreate

      val lineitemRDD = sparkSession.read.parquet("TPC-H-0.1-PARQUET/lineitem")
      val partRDD = sparkSession.read.parquet("TPC-H-0.1-PARQUET/part")
      val supplierRDD = sparkSession.read.parquet("TPC-H-0.1-PARQUET/supplier")


      val partrdd = partRDD.rdd
        .map(line => {
          (line(0), line(1))
        })
        .collectAsMap()
      val partrddHashmap = sc.broadcast(partrdd)


      val supplierrdd = supplierRDD.rdd
        .map(line => {
          (line(0), line(1))
        })
        .collectAsMap()
      val supplierrddHashmap = sc.broadcast(supplierrdd)


      val lineitem = lineitemRDD.rdd
        .filter(line => {
          line(10).toString.contains(inputDate)
        })
        .map(a => {
          (a(0).toString.toLong, (a(1), a(2)))
        })
        .sortByKey()
        .take(20)
        .map(p => (p._1, (partrddHashmap.value(p._2._1), supplierrddHashmap.value(p._2._2))))
        .foreach(p => println("(" + p._1 + "," + p._2._1.toString.toList.mkString + "," + p._2._2.toString.toList.mkString + ")"))
    }
  }
}







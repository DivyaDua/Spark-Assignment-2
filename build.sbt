name := "Spark-Assignment-2"

version := "0.1"

scalaVersion := "2.11.8"

val sparkCore = "org.apache.spark" % "spark-core_2.11" % "2.1.0"
val sparkSQl = "org.apache.spark" % "spark-sql_2.11" % "2.1.0"

libraryDependencies ++= Seq(sparkCore, sparkSQl)
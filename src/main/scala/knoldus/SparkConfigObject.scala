package knoldus

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

object SparkConfigObject extends App {

  Logger.getLogger("org").setLevel(Level.OFF)

  val sparkConf: SparkConf = new SparkConf().setAppName("Spark-Assignment-2")
    .setMaster("local[4]")

  val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  val filePath = "src/main/resources/D1.csv"

  /**Read the input CSV file using Spark Session and create a DataFrame.
    */
  val matchDF = sparkSession.read.option("header", "true").option("inferSchema", "true").csv(filePath)
  matchDF.createOrReplaceTempView("teams")

  /**Total number of match played by each team as HOME TEAM.
    */
  val homeTeamMatchesDF = sparkSession.sql("SELECT HomeTeam , count(*) as MatchesAsHomeTeam FROM teams GROUP BY HomeTeam UNION SELECT AwayTeam , 0 FROM teams WHERE AwayTeam NOT IN (SELECT HomeTeam FROM teams) GROUP BY AwayTeam")
  homeTeamMatchesDF.show(false)

  /**Top 10 team with highest wining percentage.
    */
  homeTeamMatchesDF.createOrReplaceTempView("hometeam")
  sparkSession.sql("SELECT AwayTeam , count(*) as MatchesAsAwayTeam FROM teams GROUP BY AwayTeam UNION SELECT HomeTeam , 0 FROM teams WHERE HomeTeam NOT IN (SELECT AwayTeam FROM teams) GROUP BY HomeTeam").createOrReplaceTempView("awayteam")

  sparkSession.sql("SELECT  HomeTeam as Team, MatchesAsHomeTeam, MatchesAsAwayTeam, (MatchesAsAwayTeam + MatchesAsHomeTeam) as TotalMatches FROM hometeam FULL OUTER JOIN awayteam ON awayteam.AwayTeam=hometeam.HomeTeam").createOrReplaceTempView("teammatches")

  sparkSession.sql("SELECT HomeTeam, count(FTR) as Wins FROM teams WHERE FTR = 'H' GROUP BY HomeTeam").createOrReplaceTempView("hometeamwins")
  sparkSession.sql("SELECT AwayTeam, count(FTR) as Wins FROM teams WHERE FTR = 'A' GROUP BY AwayTeam").createOrReplaceTempView("awayteamwins")

  sparkSession.sql("SELECT  HomeTeam as Team, (hometeamwins.Wins + awayteamwins.Wins) as Wins FROM hometeamwins FULL OUTER JOIN awayteamwins ON awayteamwins.AwayTeam=hometeamwins.HomeTeam").createOrReplaceTempView("teamwins")
  val df1 = sparkSession.sql("SELECT teammatches.Team, (Wins/TotalMatches)*100 as WinPercentage FROM teammatches FULL OUTER JOIN teamwins ON teammatches.Team = teamwins.Team ORDER BY WinPercentage DESC limit 10")
  df1.show(10)


  import sparkSession.implicits._

  /**Convert the DataFrame created above to DataSet by using only following fields:
    * HomeTeam, AwayTeam, FTHG, FTAG and FTR.
    */
  val matchDataSet: Dataset[Match] = matchDF.map(row => Match(row.getString(2),row.getString(3),row.getInt(4),row.getInt(5),row.getString(6)))

  /**Total number of match played by each team.
    */
  val matchesCount: DataFrame = matchDataSet.select("HomeTeam").withColumnRenamed("HomeTeam", "Team")
    .union(matchDataSet.select("AwayTeam").withColumnRenamed("AwayTeam", "Team")).groupBy("Team").count()
    .withColumnRenamed("count", "TotalMatches")
  matchesCount.show(false)

  /**Top Ten team with highest wins.
    */
  val homeDF = matchDataSet.select("HomeTeam","FTR").where("FTR = 'H'" ).groupBy("HomeTeam").count().withColumnRenamed("count", "HomeWins")
  val awayDF = matchDataSet.select("AwayTeam","FTR").where("FTR = 'A'").groupBy("AwayTeam").count().withColumnRenamed("count", "AwayWins")

  val teamsDF = homeDF.join(awayDF, homeDF.col("HomeTeam") === awayDF.col("AwayTeam"))

  val add: (Int, Int) => Int = (a: Int, b: Int) => a + b
  val total = udf(add)
  teamsDF.withColumn("TotalWins", total(col("HomeWins"), col("AwayWins"))).select("HomeTeam","TotalWins")
    .withColumnRenamed("HomeTeam","Team").sort(desc("TotalWins")).limit(10).show(false)

}


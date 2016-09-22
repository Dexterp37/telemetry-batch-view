package com.mozilla.telemetry.views

import org.apache.spark.sql.SparkSession
import org.joda.time.{DateTime, Days, format}
import org.joda.time.format.DateTimeFormat
import org.rogach.scallop._

private case class Client(id: Int,
                      width: Double,
                      height: Double,
                      depth: Double,
                      material: String,
                      color: String)

object GameHardwareSurveyView {
  private class Opts(args: Array[String]) extends ScallopConf(args) {
    val from = opt[String]("from", descr = "From submission date", required = true)
    val to = opt[String]("to", descr = "To submission date", required = true)
    val outputBucket = opt[String]("bucket", descr = "The output S3 bucket", required = true)
    verify()
  }

  private def generateDatesList(from: DateTime, to: DateTime) = {
    val daysCount = Days.daysBetween(from, to).getDays()
    (0 to daysCount).map(from.plusDays(_).toString("yyyyMMdd"))
  }

  def runSurvey(from: DateTime, to: DateTime) = {
    // Create and configure a SparkSession.
    val warehouseLocation = "file:///C:/tmp"//"file:${system:user.dir}/spark-warehouse"
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    //val activityDays = generateDatesList(from, to)
    val localData = spark.read.parquet("file:///C:/Mozilla/ffdaab31-65a5-4eae-9ead-c86b7182d9a2")
    localData.createOrReplaceTempView("longitudinal")

    val clientData = spark.sql("select * from longitudinal")
      .selectExpr("build", "client_id", "system_os", "submission_date", "system", "geo_country",
                  "system_gfx", "system_cpu", "normalized_channel")
      .where("normalized_channel = 'release'")
      .where("build is not null and build[0].application_name = 'Firefox'")
      //.where submission_date in array dates generated from "from" to "to"
    /*

    val clientAddons = hiveContext.sql("select * from longitudinal")
      .where("active_addons is not null")
      .where("build is not null and build[0].application_name = 'Firefox'")
      .selectExpr("client_id", "active_addons[0] as active_addons")
      .as[Addons]
      .flatMap{ case Addons(Some(clientId), Some(addons)) =>
        for {
          (addonId, meta) <- addons
          if !List("loop@mozilla.org","firefox@getpocket.com", "e10srollout@mozilla.org", "firefox-hotfix@mozilla.org").contains(addonId) &&
            AMODatabase.contains(addonId)
          addonName <- meta.name
          blocklisted <- meta.blocklisted
          signedState <- meta.signed_state
          userDisabled <- meta.user_disabled
          appDisabled <- meta.app_disabled
          addonType <- meta.`type`
          if !blocklisted && (addonType != "extension" || signedState == 2) && !userDisabled && !appDisabled
        } yield {
          (clientId, addonId, hash(clientId), hash(addonId))
        }
      }*/
  }

  def main(args: Array[String]): Unit = {
    val opts = new Opts(args)
    val from = opts.from()
    val to = opts.to()

    val formatter = DateTimeFormat.forPattern("yyyyMMdd")
    val fromDate = formatter.parseDateTime(from)
    val toDate = formatter.parseDateTime(to)

    runSurvey(fromDate, toDate)
  }
}


package com.mozilla.telemetry.views

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import org.apache.spark.sql.functions._
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.json4s.jackson.Serialization.write
import org.rogach.scallop._
import org.apache.spark.sql.functions.concat_ws

private case class Build(application_id: Option[String],
                         application_name: Option[String],
                         architecture: Option[String],
                         architectures_in_binary: Option[String],
                         build_id: Option[String],
                         version: Option[String],
                         vendor: Option[String],
                         platform_version: Option[String],
                         xpcom_abi: Option[String],
                         hotfix_version: Option[String])

private case class Plugin(name: Option[String],
                          version: Option[String],
                          description: Option[String],
                          blocklisted: Option[Boolean],
                          disabled: Option[Boolean],
                          clicktoplay: Option[Boolean],
                          mime_types: Option[Array[Option[String]]],
                          update_day: Option[Long])

private case class SystemMemory(memory_mb: Option[Long], virtual_max_mb: Option[Long], is_wow64: Option[Boolean])
private case class SystemCPU(cores: Option[Int],
                             count: Option[Int],
                             vendor: Option[String],
                             family: Option[Int],
                             model: Option[Int],
                             stepping: Option[Int],
                             l2cache_kb: Option[Int],
                             l3cache_kb: Option[Int],
                             extensions: Option[Array[String]],
                             speed_mhz: Option[Int])

private case class GfxAdapter(description: Option[String],
                              vendor_id: Option[String],
                              device_id: Option[String],
                              subsys_id: Option[String],
                              ram: Option[Int],
                              driver: Option[String],
                              driver_version: Option[String],
                              driver_date: Option[String],
                              gpu_active: Option[Boolean])

private case class GfxMonitor(screen_width: Option[Int],
                              screen_height: Option[Int],
                              refresh_rate: Option[String],
                              pseudo_display: Option[Boolean],
                              scale: Option[Double])

private case class SystemGFX(d2d_enabled: Option[Boolean],
                             d_write_enabled: Option[Boolean],
                             adapters: Option[Array[Option[GfxAdapter]]],
                             monitors: Option[Array[Option[GfxMonitor]]])

private case class SystemOS(name: Option[String],
                            version: Option[String],
                            kernel_version: Option[String],
                            service_pack_major: Option[Long],
                            service_pack_minor: Option[Long],
                            windows_build_number: Option[Long],
                            windows_ubr: Option[Long],
                            install_year: Option[Long],
                            locale: Option[String])

private case class ClientData(client_id: Option[String],
                              submission_date: Option[Array[java.sql.Date]],
                              build: Option[Array[Build]],
                              system: Option[Array[SystemMemory]],
                              system_os: Option[Array[SystemOS]],
                              system_cpu: Option[Array[SystemCPU]],
                              system_gfx: Option[Array[SystemGFX]],
                              active_plugins: Option[Array[Array[Option[Plugin]]]])

private case class HardwareSurveyRecord(browser_arch: Option[String],
                                        os_arch: Option[String],
                                        os_name: Option[String],
                                        os_version: Option[String],
                                        ram: Option[Long],
                                        is_wow64: Option[Boolean],
                                        first_gpu: (Option[String], Option[String]),
                                        first_monitor: (Option[Int], Option[Int]),
                                        cpu_cores: Option[Int],
                                        cpu_vendor: Option[String],
                                        cpu_speed: Option[Int],
                                        has_flash: Boolean,
                                        has_silverlight: Boolean,
                                        has_unity: Boolean)

private case class AggregatedDataEntry(key: String, count: BigInt)

object FirefoxHardwareSurveyView {
  implicit val formats = org.json4s.DefaultFormats

  private class Opts(args: Array[String]) extends ScallopConf(args) {
    val from = opt[String]("from", descr = "From submission date", required = true)
    val to = opt[String]("to", descr = "To submission date", required = true)
    val outputBucket = opt[String]("bucket", descr = "The output S3 bucket", required = true)
    verify()
  }

  private def inferOS(browserArch: Option[String], osName: Option[String], isWow64: Option[Boolean]):
    Option[String] = {
    (browserArch, osName, isWow64) match {
      // If it's a 64bit browser build, then we're on a 64bit system.
      case (Some(arch), _, _) => Some("x86-64")
      // If we're on Windows with a 32bit browser, and isWow64 = true,
      // hen we're on a 64 bit system.
      case (Some(arch), Some("Windows_NT"), Some(true)) => Some("x86-64")
      // Well, we can assume we're on a 32bit os now.
      case _ => Some("x86")
    }
  }

  private def getNextPowerOfTwo(num: Long): Long = {
    scala.math.pow(2, scala.math.ceil(scala.math.log(2935) / scala.math.log(2))).toInt
  }

  private def prepareData(date: java.sql.Date, build: Build, sys: SystemMemory, os: SystemOS, cpu: SystemCPU,
                  gfx: SystemGFX, plugins: Array[Option[Plugin]]) = {

    val firstGPU = gfx.adapters match {
      case Some(adapters) => adapters(0) match {
        case Some(g) => (g.vendor_id, g.device_id)
      }
    }

    val firstMonitor = gfx.monitors match {
      case Some(monitors) => monitors(0) match {
        case Some(m) => (m.screen_width, m.screen_height)
      }
    }

    HardwareSurveyRecord(
      build.architecture,
      inferOS(build.architecture, os.name, sys.is_wow64),
      os.name,
      os.version,
      sys.memory_mb match { case Some(mem) => Some(getNextPowerOfTwo(mem)) },
      sys.is_wow64,
      firstGPU,
      firstMonitor,
      cpu.cores,
      cpu.vendor,
      cpu.speed_mhz,
      plugins.exists { case Some(p) => p.name.contains("Shockwave Flash") },
      plugins.exists { case Some(p) => p.name.contains("Silverlight Plug-In") },
      plugins.exists { case Some(p) => p.name.contains("Unity Player") }
    )
  }

  def runSurvey(from: DateTime, to: DateTime) = {
    System.setProperty("hadoop.home.dir", "E:\\Mozilla\\winutils\\hadoop-2.7.1")
    System.setProperty("spark.sql.warehouse.dir", "file:///C:/tmp/spark-warehouse")

    // Create and configure a SparkSession.
    val warehouseLocation = "file:///E:/Tmp"//"file:${system:user.dir}/spark-warehouse"
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    val localData = spark.read.parquet("file:///E:/Mozilla/ffdaab31-65a5-4eae-9ead-c86b7182d9a2")
    localData.createOrReplaceTempView("longitudinal")

    import spark.implicits._
    val clientData = spark.sql("select * from longitudinal")
      .where("normalized_channel = 'release'")
      .where("build is not null and build[0].application_name = 'Firefox'")
      // TODO: is there a better way to do that?
      .where("system is not null")
      .where("system_os is not null")
      .where("system_cpu is not null")
      .where("system_gfx is not null")
      .where("active_plugins is not null")
      .selectExpr("client_id", "submission_date", "build", "system", "system_os", "system_cpu", "system_gfx", "active_plugins")
      .limit(200) // TODO Remove ME
      .as[ClientData]

    // Only fetch the data for active clients.
    val activeClientsData =
      clientData.flatMap{ case ClientData(Some(clientId), Some(submission_date), Some(build), Some(system), Some(os),
                                Some(cpu), Some(gfx), Some(plugins)) =>
        /*for {
          date <- submission_date if date.after(from.toDate) & date.before(to.toDate)
          build <- build
          sys <- system
          os <- os
          cpu <- cpu
          gfx <- gfx
          plugins <- plugins
        } yield {
          (clientId, date, build, sys, os, cpu, gfx, plugins)
        }*/

        for {
          (date, i) <- submission_date.zipWithIndex
          if date.after(from.toDate) & date.before(to.toDate) &
            List(Some(build), Some(system), Some(os), Some(cpu), Some(gfx), Some(plugins)).forall(_.isDefined)
        } yield {
          (clientId, date, build(i), system(i), os(i), cpu(i), gfx(i), plugins(i))
        }
      }

    // Take one entry per client.
    val onePerClient = activeClientsData.dropDuplicates("_1")

    // Count the number of total and active users.
    val totalUsers = clientData.count()
    val totalActiveCount = onePerClient.count()

    // Transform the data in a more comfortable format.
    val transformedData = onePerClient.map(x => prepareData(x._2, x._3, x._4, x._5, x._6, x._7, x._8))

    // Aggregate the data. Define how to facet the data and then do the
    // aggregation using datasets.

    // TODO: 0.1 * active users
    val collapsingThreshold = 10

    def noCollapsing(dims: Seq[String], ds: sql.Dataset[_]): Array[AggregatedDataEntry] = {
      // Sum all the counts in the table and return a single AggregatedDataEntry
      // with the key "Other" and the value as the sum.
      val colsToConcat = dims.map(colName => ds.col(colName))
      ds.select(concat_ws("_", colsToConcat:_*).alias("key"), ds.col("count"))
        .where(s"count < $collapsingThreshold")
        .as[AggregatedDataEntry]
        .collect()
    }

    def simpleCollapsing(dims: Seq[String], ds: sql.Dataset[_]): Array[AggregatedDataEntry] = {
      // Sum all the counts in the table and return a single AggregatedDataEntry
      // with the key "Other" and the value as the sum.
      Array[AggregatedDataEntry](AggregatedDataEntry("Other", ds.agg(sum("count")).first.getAs[Long](0)))
    }

    def foldLeftCollapsing(dims: Seq[String], ds: sql.Dataset[_]): Array[AggregatedDataEntry] = {
      // This strategy assume we have at least two fields other than the count.
      // Group by the first facet. If the entries have enough items, good, return them.
      // Otherwise, do a second pass.
      val foldedLeft = ds.groupBy(dims(0)).sum("count")
        .withColumnRenamed(dims.head, "key")
        .withColumnRenamed("sum(count)", "count")

      val validAggregates = foldedLeft
        .where(s"count >= $collapsingThreshold")
        // TODO: we should append the _other postfix to the first column.
        .as[AggregatedDataEntry]
        .collect()

      validAggregates ++ simpleCollapsing(dims, foldedLeft.where(s"count < $collapsingThreshold"))
    }

    // TODO: define a case like Facet(prefix, Seq(facets), CollapsingStrategyFunc)
    case class AggregatedView(dimensions: Seq[String],
                              name: String,
                              collapseFunction: (Seq[String], sql.Dataset[_]) => Array[AggregatedDataEntry])
    val dimensions = List[AggregatedView](
      AggregatedView(Seq("os_name", "os_version"), "os", foldLeftCollapsing),
      AggregatedView(Seq("browser_arch"), "browser", simpleCollapsing),
      AggregatedView(Seq("ram"), "ram", simpleCollapsing),
      //AggregatedView(Seq("first_gpu"), "gpu", simpleCollapsing),
      //AggregatedView(Seq("first_monitor"), "screen", simpleCollapsing),
      AggregatedView(Seq("cpu_vendor"), "cpu", simpleCollapsing),
      AggregatedView(Seq("cpu_cores", "cpu_speed"), "cpu_cores", foldLeftCollapsing),
      AggregatedView(Seq("has_flash"), "has_flash", noCollapsing),
      AggregatedView(Seq("has_silverlight"), "has_silverlight", noCollapsing),
      AggregatedView(Seq("has_unity"), "has_unity", noCollapsing)
    )

    // Aggregate the data along the provided dimensions.
    val aggregates = dimensions.map { view =>
      transformedData.groupBy(view.dimensions.head, view.dimensions.tail: _*).count()
    }

    // TODO: Collapse the groups.
    // Edge case resolution: round hundred
    // Edge case os: try to aggregate w\o the version number

    val perViewAggregates = dimensions.zip(aggregates).map{ case (view, data) =>
      // Concatenate the values for all the columns in the row to build a "key". For example,
      // if we aggregated the data over 2 dimensions "a" and "b", the built key would look
      // like "Val(a)_Val(b)". We store each key and the relative count in an AggregateDataEntry.
      val colsToConcat = view.dimensions.map(colName => data.col(colName))
      val entries = data
        .select(concat_ws("_", colsToConcat:_*).alias("key"), data.col("count"))
        .where(s"count >= $collapsingThreshold")
        .as[AggregatedDataEntry]
        .collect()

      // Apply a collapsing strategy to the other entries.
      val rowsToCollapse = data.where(s"count < $collapsingThreshold")
      val collapsedEntries = view.collapseFunction(view.dimensions, rowsToCollapse)

      // Keep the aggregated data grouped by the view name. This allows for some
      // flexibility when serializing the data.
      (view.name, (entries ++ collapsedEntries).map(x => (x.key, x.count)))
    }

    // Save to JSON. Convert the data point labels into the "Group_Value: Count" form and convert
    // raw numbers to ratios.
    val scaledData = perViewAggregates.flatMap{ case (viewName, data) =>
      data.map(x => (s"${viewName}_${x._1}", x._2.toDouble / totalActiveCount))
    }

    val serializedData = write(scaledData.toMap)
    val chunkDataPath = Paths.get("hw_survey.json")
    Files.write(chunkDataPath, serializedData.getBytes(StandardCharsets.UTF_8))

    // TODO: Store the partial to S3
    // TODO: Fetch the full state from S3 and merge the new partial to it.
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

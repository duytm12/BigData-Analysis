package billy

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.io.StdIn.readLine

object ETL_logContent {
  private def get_date_list(startDay: String, endDay: String): List[String] = {
    val date_format = DateTimeFormatter.ofPattern("yyyyMMdd")
    val start_day = LocalDate.parse(startDay, date_format)
    val end_day = LocalDate.parse(endDay, date_format)
    val date_range = start_day.toEpochDay.to(end_day.toEpochDay)
      .map(LocalDate.ofEpochDay).toList
    val list_of_date_range = date_range.map(date => date_format.format(date))
    list_of_date_range
  }

  def read_file(folderPath: String, fileName: String, spark: SparkSession): DataFrame = {
    spark.read
      .option("header", value = true)
      .json(s"$folderPath/$fileName.json")
  }

  def process_file(fileName: String, df: DataFrame): DataFrame = {
    val df1 = df.select(col("_source.Contract"),
      col("_source.AppName"),
      col("_source.TotalDuration"))

    val df2 = df1.withColumn("Category",
      when(col("AppName") === "KPLUS" || col("AppName") === "CHANNEL", "TV")
        .when(col("AppName") === "RELAX", "Relax")
        .when(col("AppName") === "CHILD", "Child")
        .when(col("AppName") === "SPORT", "Sport")
        .otherwise("Movie"))
      .withColumn("Date", to_date(lit(fileName.split(".json")(0)), "yyyyMMdd"))

    df2.filter(col("Category").isNotNull)
      .groupBy("Contract", "Date")
      .pivot("Category")
      .sum("TotalDuration")
      .na.fill(0)
      .withColumnRenamed("Child", "ChildDuration")
      .withColumnRenamed("Movie", "MovieDuration")
      .withColumnRenamed("Relax", "RelaxDuration")
      .withColumnRenamed("Sport", "SportDuration")
      .withColumnRenamed("TV", "TVDuration")
  }

  private def get_most_watch(df: DataFrame): DataFrame = {
    val highest_duration = greatest(col("ChildDuration"), col("MovieDuration"), col("RelaxDuration"),
      col("SportDuration"),col("TVDuration"))
    val df_1 = df.drop("Date")
      .groupBy("Contract")
      .agg(sum(col("ChildDuration").cast(IntegerType)).as("ChildDuration"),
        sum(col("MovieDuration").cast(IntegerType)).as("MovieDuration"),
        sum(col("SportDuration").cast(IntegerType)).as("SportDuration"),
        sum(col("TVDuration").cast(IntegerType)).as("TVDuration"),
        sum(col("RelaxDuration").cast(IntegerType)).as("RelaxDuration")
      )

    val df_2 = df_1.withColumn("MostWatch",
      when(highest_duration === col("ChildDuration"), "Child")
        .when(highest_duration === col("MovieDuration"), "Movie")
        .when(highest_duration === col("RelaxDuration"), "Relax")
        .when(highest_duration === col("SportDuration"), "Sport")
        .otherwise("TV"))
    df_2
  }

  private def get_customer_taste(df: DataFrame): DataFrame = {
    val df_3 = df.withColumn("Taste",
      concat_ws(
        "-",
        when(col("ChildDuration") > 0, lit("Child")),
        when(col("MovieDuration")> 0, lit("Movie")),
        when(col("SportDuration")> 0, lit("Sport")),
        when(col("TVDuration")> 0, lit("TV")),
        when(col("RelaxDuration")> 0, lit("Relax"))
      )
    )
    df_3
  }

  private def get_activeness(df: DataFrame): DataFrame = {
    val df_4 = df.groupBy("Contract")
      .agg(count("Date").as("DateCount"))
      .withColumn("Activeness", round((col("DateCount")/ 30.0) * 100,2))
      .withColumnRenamed("Contract", "Contract_4")

    df_4
  }

  private def save_topath(path: String, df:DataFrame): Unit = {
    df.repartition(1)
      .write.format("csv")
      .option("header", value = true)
      .mode("overwrite")
      .save(path)
  }

  private def write_toMySQL(df: DataFrame): Unit = {
    df.repartition(1)
      .write
      .format("jdbc")
      .option("header", value = true)
      .option("url", "jdbc:mysql://127.0.0.1:3306/datawarehouse")
      .option("dbtable", "log_search_olap")
      .option("user", "root")
      .option("password", "")
      .mode(SaveMode.Overwrite) // or SaveMode.Append
      .save()
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ETL")
      .master("local[*]")
      .getOrCreate()

    val start_date = readLine("Enter ETL start date (yyyymmdd): ")
    val end_date = readLine("Enter ETL end date (yyyymmdd): ")
    val date_list = get_date_list(start_date, end_date)

    val folder_path = "Data/log_content"
    val save_path = s"cleanData/ETLdayRange $start_date-$end_date"
    var final_df: DataFrame = null
    for (file_name <- date_list) {
      println("---------")
      println(s"Extracting file names $file_name.json")
      println("---------")

      println("---------")
      println(s"Reading file $file_name")
      println("---------")
      val df = read_file(folder_path, file_name, spark)

      println("---------")
      println(s"Processing file $file_name")
      println("---------")
      val processed_df = process_file(file_name, df)
      println("---------")
      println(s"Completed ETL file $file_name")
      println("---------")


      if (final_df == null) {final_df = processed_df}
      else
        {final_df = final_df.unionAll(processed_df)
          println("---------")
          println(s"Union $file_name successfully")
          println("---------")
        }
    }

    println("---------")
    println("Getting most watch")
    println("---------")
    val df_2 = get_most_watch(final_df)

    println("---------")
    println("Getting customer taste")
    println("---------")
    val df_3 = get_customer_taste(df_2)

    println("---------")
    println("Getting customer activeness")
    println("---------")
    val df_4 = get_activeness(final_df)

    println("---------")
    println("Getting final OLAP Output")
    println("---------")
    final_df = df_3.join(df_4, df_3("Contract") === df_4("Contract_4"), "full_outer")
    final_df = final_df.drop("Contract_4")
    final_df = final_df.filter(col("Contract").isNotNull)
    final_df.show()

    println("---------")
    println("Saving final result to path")
    println("---------")
    save_topath(save_path,final_df)

    println("---------")
    println("Importing to MySQL DataWarehouse")
    println("---------")
    write_toMySQL(final_df)

    spark.stop()
    println("--------")
    println("Final ETL Completed")
    println("--------")
  }
}

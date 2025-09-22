import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

println("=== TITANIC DATASET ANALYSIS ===")

// Load the DataFrame
val data = spark.read.option("header", "true").option("inferSchema", "true").csv("train.csv")

println("Dataset loaded successfully!")
println(s"Total rows: ${data.count()}")
println(s"Total columns: ${data.columns.length}")

println("\nDataset overview:")
data.printSchema()
data.show(5)

// QUESTION 1: What is the average ticket fare for each Ticket class?

println("\n" + "="*80)
println("QUESTION 1: What is the average ticket fare for each Ticket class?")
println("(1st = Upper; 2nd = Middle; 3rd = Lower)")
println("="*80)

println("\nAverage Fare by Class:")
data.filter(col("Fare").isNotNull)
  .groupBy("Pclass")
  .agg(round(avg("Fare"), 2).alias("Average_Fare"))
  .orderBy("Pclass")
  .show()

// Calculate individual fares
val fare1Result = data.filter(col("Fare").isNotNull && col("Pclass") === 1).agg(round(avg("Fare"), 2).alias("fare")).collect()(0).getAs[Double]("fare")
val fare2Result = data.filter(col("Fare").isNotNull && col("Pclass") === 2).agg(round(avg("Fare"), 2).alias("fare")).collect()(0).getAs[Double]("fare")
val fare3Result = data.filter(col("Fare").isNotNull && col("Pclass") === 3).agg(round(avg("Fare"), 2).alias("fare")).collect()(0).getAs[Double]("fare")

println("ANSWER TO QUESTION 1:")
println(s"1st Class (Upper): $$${fare1Result} average fare")
println(s"2nd Class (Middle): $$${fare2Result} average fare")
println(s"3rd Class (Lower): $$${fare3Result} average fare")

// QUESTION 2: Survival percentage by class? Which class has highest survival rate?

println("\n" + "="*80)
println("QUESTION 2: What is the survival percentage for each Ticket class?")
println("Which class has the highest survival rate?")
println("="*80)

println("\nSurvival Statistics by Class:")
data.groupBy("Pclass")
  .agg(
    count("*").alias("Total"),
    sum("Survived").alias("Survivors"),
    round((sum("Survived") * 100.0 / count("*")), 2).alias("Survival_Rate")
  )
  .orderBy("Pclass")
  .show()

// Calculate individual survival rates
val survival1 = data.filter(col("Pclass") === 1).agg(round((sum("Survived") * 100.0 / count("*")), 2).alias("rate")).collect()(0).getAs[Double]("rate")
val survival2 = data.filter(col("Pclass") === 2).agg(round((sum("Survived") * 100.0 / count("*")), 2).alias("rate")).collect()(0).getAs[Double]("rate")
val survival3 = data.filter(col("Pclass") === 3).agg(round((sum("Survived") * 100.0 / count("*")), 2).alias("rate")).collect()(0).getAs[Double]("rate")

val bestClassNum = if (survival1 >= survival2 && survival1 >= survival3) 1 else if (survival2 >= survival3) 2 else 3
val bestClassRate = if (bestClassNum == 1) survival1 else if (bestClassNum == 2) survival2 else survival3

println("ANSWER TO QUESTION 2:")
println(s"1st Class (Upper): ${survival1}% survival rate")
println(s"2nd Class (Middle): ${survival2}% survival rate")
println(s"3rd Class (Lower): ${survival3}% survival rate")
println(s"Class $bestClassNum has the HIGHEST survival rate at ${bestClassRate}%")

// QUESTION 3: Find passengers who could possibly be Rose DeWitt Bukater

println("\n" + "="*80)
println("QUESTION 3: Find passengers who could possibly be Rose DeWitt Bukater")
println("Rose's characteristics:")
println("- Age: 17 years old")
println("- Gender: Female")
println("- Class: 1st Class")
println("- Traveling with: 1 parent (Parch = 1)")
println("="*80)

val rose = data.filter(
  col("Age") === 17 &&
  col("Sex") === "female" &&
  col("Pclass") === 1 &&
  col("Parch") === 1
)

val roseCount = rose.count()
println(s"\nNumber of passengers who could possibly be Rose: $roseCount")

if (roseCount > 0) {
  println("\nPossible Rose candidates:")
  rose.select("PassengerId", "Name", "Age", "Sex", "Pclass", "Parch", "Survived").show(false)
  
  val roseSurvived = rose.filter(col("Survived") === 1).count()
  println(s"Survival: $roseSurvived out of $roseCount survived")
  if (roseSurvived > 0) println("This matches the movie - Rose survived!")
} else {
  println("No exact matches found for Rose's characteristics.")
}

println(s"\nANSWER TO QUESTION 3: $roseCount passengers could possibly be Rose")

// QUESTION 4: Find passengers who could possibly be Jack Dawson

println("\n" + "="*80)
println("QUESTION 4: Find passengers who could possibly be Jack Dawson")
println("Jack's characteristics:")
println("- Born: 1892, Died: April 15, 1912")
println("- Age: 19 or 20 years old")
println("- Gender: Male")
println("- Class: 3rd Class")
println("- No relatives onboard (SibSp = 0, Parch = 0)")
println("="*80)

val jack = data.filter(
  (col("Age") === 19 || col("Age") === 20) &&
  col("Sex") === "male" &&
  col("Pclass") === 3 &&
  col("SibSp") === 0 &&
  col("Parch") === 0
)

val jackCount = jack.count()
println(s"\nNumber of passengers who could possibly be Jack: $jackCount")

if (jackCount > 0) {
  println("\nPossible Jack candidates:")
  jack.select("PassengerId", "Name", "Age", "Sex", "Pclass", "SibSp", "Parch", "Survived").show(jackCount.toInt, false)
  
  val jackSurvived = jack.filter(col("Survived") === 1).count()
  val jackDied = jackCount - jackSurvived
  println(s"Survival: $jackSurvived survived, $jackDied did not survive")
  if (jackDied > 0) println("Found matches for people who can be jack")
} else {
  println("No exact matches found for Jack's characteristics.")
}

println(s"\nANSWER TO QUESTION 4: $jackCount passengers could possibly be Jack")

// QUESTION 5: Age group analysis - fare relation and survival

println("\n" + "="*80)
println("QUESTION 5: Age group analysis")
println("Split age into groups: 1-10, 11-20, 21-30, 31-40, 41-50, 51-60, 61-70, 71-80")
println("A) What is the relation between ages and ticket fare?")
println("B) Which age group most likely survived?")
println("="*80)

// Create age groups using when/otherwise for better organization
val dataWithAgeGroup = data.withColumn("AgeGroup",
  when(col("Age").isNull, "Unknown")
    .when(ceil(col("Age")) >= 0 && ceil(col("Age")) < 11, "00-10")
    .when(ceil(col("Age")) >= 11 && ceil(col("Age")) < 21, "11-20")
    .when(ceil(col("Age")) >= 21 && ceil(col("Age")) < 31, "21-30")
    .when(ceil(col("Age")) >= 31 && ceil(col("Age")) < 41, "31-40")
    .when(ceil(col("Age")) >= 41 && ceil(col("Age")) < 51, "41-50")
    .when(ceil(col("Age")) >= 51 && ceil(col("Age")) < 61, "51-60")
    .when(ceil(col("Age")) >= 61 && ceil(col("Age")) < 71, "61-70")
    .when(ceil(col("Age")) >= 71 && ceil(col("Age")) < 81, "71-80")
    .when(ceil(col("Age")) >= 81 && ceil(col("Age")) < 91, "81-90")
    .otherwise("91+")
)

// PART A: Fare analysis by age group
println("\n--- PART A: Average Ticket Fare by Age Group ---")

// Create aggregated fare data - do it in multiple steps to ensure proper assignment
val fareAggDF = dataWithAgeGroup.filter(col("Fare").isNotNull).groupBy("AgeGroup").agg(
  round(avg("Fare"), 2).alias("Average_Fare"),
  count("*").alias("Passenger_Count")
).orderBy("AgeGroup")

// Now collect the results
val fareDataArray = fareAggDF.collect()

// Display the table
println("\n+----------+-------------+-------+")
println("| Age Group| Avg Fare ($)| Count |")
println("+----------+-------------+-------+")
fareDataArray.foreach { row =>
  val ageGroup = row.getAs[String]("AgeGroup")
  val avgFare = row.getAs[Double]("Average_Fare")
  val count = row.getAs[Long]("Passenger_Count")
  println(f"| $ageGroup%-9s| $avgFare%11.2f | $count%5d |")
}
println("+----------+-------------+-------+")

// PART B: Survival analysis by age group
println("\n--- PART B: Survival Rate by Age Group ---")

// Create aggregated survival data - do it in multiple steps to ensure proper assignment
val survivalAggDF = dataWithAgeGroup.groupBy("AgeGroup").agg(
  count("*").alias("Total"),
  sum("Survived").alias("Survivors"),
  round((sum("Survived") * 100.0 / count("*")), 2).alias("Survival_Rate")
).orderBy("AgeGroup")

// Now collect the results
val survivalDataArray = survivalAggDF.collect()

// Display the table
println("\n+----------+---------+-----------+--------------+")
println("| Age Group| Total   | Survived  | Survival Rate|")
println("+----------+---------+-----------+--------------+")
survivalDataArray.foreach { row =>
  val ageGroup = row.getAs[String]("AgeGroup")
  val total = row.getAs[Long]("Total")
  val survivors = row.getAs[Long]("Survivors")
  val survivalRate = row.getAs[Double]("Survival_Rate")
  println(f"| $ageGroup%-9s| $total%7d | $survivors%9d | $survivalRate%11.2f%% |")
}
println("+----------+---------+-----------+--------------+")

// Find best survival and fare groups
val maxFareRow = fareDataArray.maxBy(_.getAs[Double]("Average_Fare"))
val minFareRow = fareDataArray.minBy(_.getAs[Double]("Average_Fare"))
val maxSurvivalRow = survivalDataArray.maxBy(_.getAs[Double]("Survival_Rate"))

val highestFareGroup = maxFareRow.getAs[String]("AgeGroup")
val highestFareAmount = maxFareRow.getAs[Double]("Average_Fare")
val lowestFareGroup = minFareRow.getAs[String]("AgeGroup")
val lowestFareAmount = minFareRow.getAs[Double]("Average_Fare")
val bestSurvivalGroup = maxSurvivalRow.getAs[String]("AgeGroup")
val bestSurvivalRate = maxSurvivalRow.getAs[Double]("Survival_Rate")

println("\nANSWER TO QUESTION 5:")
println("A) Relation between age and fare:")
println(s"   - Highest average fare: $highestFareGroup ($$${highestFareAmount})")
println(s"   - Lowest average fare: $lowestFareGroup ($$${lowestFareAmount})")
println("B) Age group most likely to survive:")
println(s"   - $bestSurvivalGroup with ${bestSurvivalRate}% survival rate")

// ================================================================================
// FINAL SUMMARY OF ALL ANSWERS
// ================================================================================

println("\n" + "="*90)
println("FINAL SUMMARY")
println("="*90)

println("\nQUESTION 1: Average ticket fare for each class")
println(s"  ANSWER: 1st Class (Upper): $$${fare1Result}")
println(s"          2nd Class (Middle): $$${fare2Result}")
println(s"          3rd Class (Lower): $$${fare3Result}")

println(s"\nQUESTION 2: Survival percentage by class")
println(s"  ANSWER: 1st Class (Upper): ${survival1}%")
println(s"          2nd Class (Middle): ${survival2}%")
println(s"          3rd Class (Lower): ${survival3}%")
println(s"          Class $bestClassNum has the HIGHEST survival rate")

println(s"\nQUESTION 3: Rose candidates")
println(s"  ANSWER: $roseCount passengers could possibly be Rose DeWitt Bukater")

println(s"\nQUESTION 4: Jack candidates")
println(s"  ANSWER: $jackCount passengers could possibly be Jack Dawson")

println(s"\nQUESTION 5: Age group analysis")
println(s"  ANSWER: A) Highest avg fare: $highestFareGroup ($$${highestFareAmount})")
println(s"             Lowest avg fare: $lowestFareGroup ($$${lowestFareAmount})")
println(s"          B) Best survival rate: $bestSurvivalGroup (${bestSurvivalRate}%)")
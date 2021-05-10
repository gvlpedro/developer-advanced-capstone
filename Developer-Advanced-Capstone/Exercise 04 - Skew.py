# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md 
# MAGIC # Exercise #4 - Skew
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC A significant number of performance problems manifest themselves during, or because, of shuffle operations.
# MAGIC 
# MAGIC The catch to diagnosing and resolving these issues is in identifying the root cause.
# MAGIC 
# MAGIC To complicate things further, the root cause can be anything from arbitrary execution of wide transformations (e.g. unnecissarily sorting of the dataset) to accidental cross joins.
# MAGIC 
# MAGIC In some cases the shuffle is 100% unavoidable however, even when unavoidable, the performance of the shuffle operation can be degrarded when spark-partions are forced to spill to disk.
# MAGIC 
# MAGIC Spiling to disk can be mitigated through a number of different techniques such as using larger VMs with more memory but even then, we really need to go back and fix the root cause.
# MAGIC 
# MAGIC All that being said, some of the easiest problems to diagnose are directly related to a skewed dataset.
# MAGIC 
# MAGIC In these cases, we generally see spilling of spark-partitions during the shuffle operation for some percentage of the spark-partitions. This can easily be verified by comparing the size of spilling partitions to non-spilling partitions. A little more analysis of the data should confirm your hunches.
# MAGIC 
# MAGIC Once the skew is identified, it needs to be mitigated by subdividing large partitions such that they don't spill to disk. Ideally, this also means **not** subdiving smaller partitions, but depending on the solution, this isn't always an option. 
# MAGIC 
# MAGIC Even then, more smaller partitions with a little more processing overhead almost always outperforms any situation where partitions are spilling to disk.

# COMMAND ----------

# MAGIC %md ### It Depends
# MAGIC 
# MAGIC In this exercise, the skew has already be identified for us.
# MAGIC 
# MAGIC Our job in this exercise is to employ different mitigation strategies that aim to preclude any spill.
# MAGIC 
# MAGIC In every case to follow, our dataset is skewed by **`city_id`**, manifesting itself as we attempt to execute a join between our **`transactions`** and **`cities`** datasets.
# MAGIC 
# MAGIC As in the previous exercise, there are multiple solutions to this problem.
# MAGIC 
# MAGIC If you are asking, "which one you should we employ?" well the answer is, "It depends..."

# COMMAND ----------

# MAGIC %md # Exercise #4.A - Setup
# MAGIC 
# MAGIC **In this step you will need to:**
# MAGIC 1. Configure a cluster to run DBR 7.3 LTS with 8 cores and attach it to this notebook
# MAGIC 2. Specify your Registration ID
# MAGIC 3. Run the setup notebook for this specific exercise
# MAGIC 4. Install the datasets for this specific exercise
# MAGIC 5. Run the reality check to verify your configuration and that the datasets were correctly installed

# COMMAND ----------

# MAGIC %md ### Setup - Registration ID
# MAGIC 
# MAGIC In the next commmand, please update the variable **`registration_id`** with the Registration ID you received when you signed up for this project.
# MAGIC 
# MAGIC For more information, see [Registration ID]($./Registration ID)

# COMMAND ----------

# TODO
registration_id = "FILL_IN"

# COMMAND ----------

# MAGIC %md ### Setup - Run the exercise setup
# MAGIC 
# MAGIC Run the following cell to setup this exercise, declaring exercise-specific variables and functions.

# COMMAND ----------

# MAGIC %run ./_includes/Setup-Exercise-04

# COMMAND ----------

# MAGIC %md ### Setup - Install the Dataset
# MAGIC 
# MAGIC Simply run the following command to install the exercise's datasets into your workspace.

# COMMAND ----------

install_datasets_04(reinstall=False)

# COMMAND ----------

# MAGIC %md ### Reality Check #4.A
# MAGIC Run the following command to ensure that you are on track:

# COMMAND ----------

reality_check_04_a()

# COMMAND ----------

# MAGIC %md ## Visualizing the Skew
# MAGIC 
# MAGIC Before proceeding, let's visualize the skew.
# MAGIC 0. Run the cell below.
# MAGIC 0. Below the table of **`city_id`** and **`count`**, click the chart icon
# MAGIC 0. If the graph renders properly by default, click **`Plot Options...`**
# MAGIC   * Set **`Keys`** to either **`<id>`** or **`city_id`** (but not both)
# MAGIC   * Set **`Values`** to **`count`**
# MAGIC   * Click **`Apply`** to close the dialog

# COMMAND ----------

# Run this cell to visualize the skew in this dataset
# After running it, plot it with a bar graph (default settings should be sufficient)
display(spark.read.parquet(trx_src_path).groupBy("city_id").count().orderBy(pyspark.sql.functions.col("count").desc()))

# COMMAND ----------

# MAGIC %md # Exercise #4.B - Skew-Join w/AQE
# MAGIC 
# MAGIC Introduced in Spark 3, the Adaptive Query Execution framework (AQE) provides native support for mitigating skew and thus precluding spill during the shuffle operations.
# MAGIC 
# MAGIC This is the most effective solution to-date for mitigating skew but may not yet be available if the client is required to use Spark 2.x, as we will see in later challenges.
# MAGIC 
# MAGIC While our dataset is skewed, and while we can use AQE's Skew-Join to resolve the skew, our test dataset is too small to be identified and resolved automatically by AQE. 
# MAGIC 
# MAGIC However, we have identified the desired skewed partition factor, the threshold and desired partion size for our test dataset.
# MAGIC 
# MAGIC Knowing that we can identify the production equivilent of these values another day, for now we will go ahead and implement a parameterized function that accepts the values specified below.
# MAGIC 
# MAGIC **In this step you will need to:**
# MAGIC 1. Implement the function **`skew_join_with_aqe(trx_path, cty_path, join_column, skewed_partition_factor, partition_size)`** which configures the current Spark session and returns the resulting **`DataFrame`**
# MAGIC   * The location of the **`transactions`** dataset is identified as **`trx_path`**
# MAGIC   * The location of the **`cities`** dataset is identified as **`cty_path`**
# MAGIC   * The column the datasets should be joined by is identified as **`join_column`**
# MAGIC   * The value for the **`skewedPartitionFactor`** is identified as **`skewed_partition_factor`**
# MAGIC   * The value for both the **`skewedPartitionThresholdInBytes`** and **`advisoryPartitionSizeInBytes`** is identified as **`partition_size`**
# MAGIC   * This function should enable AQE's Skew-Join feature
# MAGIC   * This function should read both the **`transactions`** and **`cities`** dataset, join them by the column identified as **`join_column`**, and return the resulting **`DataFrame`** object
# MAGIC   
# MAGIC **Special Notes:**
# MAGIC * To ensure continuity between steps, **`skew_join_with_aqe(..)`** will be invoked after
# MAGIC   * Resetting the spark environment
# MAGIC   * Disabling auto-broadcasting
# MAGIC   * Setting **`spark.sql.shuffle.partitions`** to the value specified by **`distinct_cities`**
# MAGIC 
# MAGIC **Specific Requirements:**
# MAGIC * The average's divergence from the maximum Spark-partition size must be improved from the baseline

# COMMAND ----------

# Show special details for this milestone  
show_exercise_04_b_details()

# COMMAND ----------

# MAGIC %md ### Implement Exercise #4.B
# MAGIC 
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

# TODO
# Use this cell to complete your solution

def skew_join_with_aqe(trx_path:str, cty_path:str, join_column:str, skewed_partition_factor:int, partition_size:int) -> pyspark.sql.DataFrame:
  sc.setJobDescription("Exercise #4.B")
  # FILL_IN

  
# Rest and configure our environment
reset_environment_04_b()

# Execute and debug your code
df_b = skew_join_with_aqe(trx_src_path, cty_src_path, e5b_join_column, e5b_skewed_partition_factor, e5b_partition_size)
if df_b:
  results_b = records_per_partition(df_b)
  print_statistics(results_b)

# COMMAND ----------

# MAGIC %md ### Reality Check #4.B
# MAGIC Run the following command to ensure that you are on track:

# COMMAND ----------

reality_check_04_b()

# COMMAND ----------

# MAGIC %md # Exercise #4.C - Skew-Join w/AQE
# MAGIC 
# MAGIC When running on Spark 2, or when AQE is otherwise unavailable, we need an alternative to AQE's Skew-Join.
# MAGIC 
# MAGIC For that reason, and before Spark 3 was even available, Databricks provided a propritary Skew-Hint that significantly reduced the size of skewed partitions and thus significantly reduced the resulting spills
# MAGIC 
# MAGIC **In this step you will need to:**
# MAGIC 1. Implement the function **`hinted_skew_join(trx_path, cty_path, join_column)`** which employs the skew hint and returns the resulting **`DataFrame`**
# MAGIC   * The location of the **`transactions`** dataset is identified as **`trx_path`**
# MAGIC   * The location of the **`cities`** dataset is identified as **`cty_path`**
# MAGIC   * The column the datasets should be joined by is identified as **`join_column`**
# MAGIC   * This function should read both the **`transactions`** and **`cities`** dataset, join them by the column identified as **`join_column`** while employing the **`skew`** hint, and return the resulting **`DataFrame`** object
# MAGIC   
# MAGIC **Special Notes:**
# MAGIC * To ensure continuity between steps, **`hinted_skew_join(..)`** will be invoked after
# MAGIC   * Resetting the spark environment
# MAGIC   * Disabling the AQE framework
# MAGIC   * Disabling auto-broadcasting
# MAGIC   * Setting **`spark.sql.shuffle.partitions`** to the value specified by **`distinct_cities`**
# MAGIC 
# MAGIC **Specific Requirements:**
# MAGIC * The Skew-Hint must be properly employed
# MAGIC * The average's divergence from the maximum Spark-partition size must be improved from the baseline

# COMMAND ----------

# Show special details for this milestone  
show_exercise_04_c_details()

# COMMAND ----------

# MAGIC %md ### Implement Exercise #4.C
# MAGIC 
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

# TODO
# Use this cell to complete your solution

def hinted_skew_join(trx_path:str, cty_path:str, join_column:str) -> pyspark.sql.DataFrame:
  sc.setJobDescription("Exercise #4.C")
  # FILL_IN

# Rest and configure our environment
reset_environment_04_c()

# Execute and debug your code
df_c = hinted_skew_join(trx_src_path, cty_src_path, e5c_join_column)
if df_c: 
  results_c = records_per_partition(df_c)
  print_statistics(results_c)

# COMMAND ----------

# MAGIC %md ### Reality Check #4.C
# MAGIC Run the following command to ensure that you are on track:

# COMMAND ----------

reality_check_04_c()

# COMMAND ----------

# MAGIC %md # Exercise #4.D - Manual Salting
# MAGIC 
# MAGIC 
# MAGIC With proper knowldge of your dataset, it is possible to salt the dataset such that the size of large partitions will be reduced and as a result the spilling of partitions to disk will also be reduced.
# MAGIC 
# MAGIC This is a viable solution when you have intimate knowledge of your data, when running on Spark 2 or when AQE is otherwise unavailable.
# MAGIC 
# MAGIC A "perfect" solution would attempt to salt only the skewed partions, in our case only transactions in the US, but even an suboptimal solution, which we will implement here, can provide significant reduction in spill.
# MAGIC 
# MAGIC While our soltion results in reducing even the small, non-spilling partions, the overhead of processing these now extra small partions is nothing compared to the gains seen by the overall reduction of spill.
# MAGIC 
# MAGIC The degree to which values should be salted, and how many partions we want at the conclusion of this step requires a fair amount of testing which we have done for you.
# MAGIC 
# MAGIC To salt our datsaet, you will need to implement four disinct functions as outlined below. As previously mentioned, we will specify for you all the parameters required for this test dataset. We will leave the determination of the correct paramters for the production dataset as a task for another day.
# MAGIC 
# MAGIC **In this step you will need to:**
# MAGIC 0. Implement the function **`salts(skew_factor, salt_column)`** which creates a single range of values
# MAGIC   * Create a **`DataFrame`** with one column identified as **`salt_column`**
# MAGIC   * The values of the **`DataFrame`** should be from zero to **`skew_factor`**, exclusive.
# MAGIC 0. Implement the function **`salt_cities_dataset(salts, cty_path, partitions, salt_column, join_column)`**
# MAGIC   * Read in the **`cities`** dataset from the location identified by **`cty_path`**
# MAGIC   * To mitigate the impact of the upcoming join operation, the **`cities`** dataset should be repartioned by the value specified as **`partions`**.
# MAGIC   * The two dataframes should be joined so as to produce one distinct record for every combination of the **`cities`** dataset and the specified salt values, identified as **`salts`**
# MAGIC   * After the join operation, a new string column, identified as **`join_column`**, must be created by concatenating the **`city_id`** column, an underscore, and the column identified as **`salt_column`**. Examples include:
# MAGIC       * **`123456_0`** where the **`city_id`** was "123456" and the salt value was "0"
# MAGIC       * **`456789_1`** where the **`city_id`** was "456789" and the salt value was "1"
# MAGIC       * **`777777_2`** where the **`city_id`** was "777777" and the salt value was "2"
# MAGIC   * After all transformations are complete, the column identified by **`salt_column`** should be dropped
# MAGIC   * Finally, the resulting **`DataFrame`** object should be returned by the function
# MAGIC   
# MAGIC 0. Implement the function **`salt_transactions_dataset(skew_factor, trx_path, salt_column, join_column)`**
# MAGIC   * Read in the **`transactions`** dataset from the location identified by **`trx_path`**
# MAGIC   * A new interger column, identified as **`salt_column`**, should be a random number between zero and **`skew_factor`**, exclusive
# MAGIC   * A new string column, identified as **`join_column`**, must be created by concatenating the **`city_id`** column, an underscore, and the column identified as **`salt_column`** just as we did in the previous function
# MAGIC   * After all transformations are complete, the column identified by **`salt_column`** should be dropped
# MAGIC   * Finally, the resulting **`DataFrame`** object should be returned by the function
# MAGIC   
# MAGIC 0. Implement the function **`salted_join(trx_df, cty_df, join_column)`**
# MAGIC   * Join the two **`DataFrame`** objects by the column specified by **`join_column`**
# MAGIC   * Drop the column specified by **`join_column`**
# MAGIC   * Finally, the resulting **`DataFrame`** object should be returned by the function
# MAGIC 
# MAGIC **Special Notes:**
# MAGIC * To ensure continuity between steps, all four functions will be invoked after
# MAGIC   * Resetting the spark environment
# MAGIC   * Disabling the AQE framework
# MAGIC   * Disabling auto-broadcasting
# MAGIC   * Setting **`spark.sql.shuffle.partitions`** to the value specified by **`distinct_cities`**
# MAGIC 
# MAGIC **Specific Requirements:**
# MAGIC * The average's divergence from the maximum Spark-partition size must be improved from the baseline
# MAGIC * The number of records returned by each function must meet the expectations seen in the reality-check below

# COMMAND ----------

# Show special details for this milestone  
show_exercise_04_d_details()

# COMMAND ----------

# MAGIC %md ### Implement Exercise #4.D
# MAGIC 
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

# TODO
# Use this cell to complete your solution

def salts(skew_factor:int, salt_column:str) -> pyspark.sql.DataFrame:
  sc.setJobDescription("Exercise #4.D - Create Salts")
  # FILL_IN

def salt_cities_dataset(salts:pyspark.sql.DataFrame, cty_path:str, partitions:int, salt_column:str, join_column:str) -> pyspark.sql.DataFrame:
  sc.setJobDescription("Exercise #4.D - Salt Cities Dataset")
  # FILL_IN

def salt_transactions_dataset(skew_factor:int, trx_path:str, salt_column:str, join_column:str) -> pyspark.sql.DataFrame:
  sc.setJobDescription("Exercise #4.D - Salt Transactions Dataset")
  # FILL_IN

def salted_join(trx_df:pyspark.sql.DataFrame, cty_df:pyspark.sql.DataFrame, join_column:str) -> pyspark.sql.DataFrame:
  sc.setJobDescription("Exercise #4.D - Salted Join")
  # FILL_IN

  
# Rest and configure our environment
reset_environment_04_d()

# Execute and debug your code
salts_df = salts(e5d_skew_factor, e5d_salt_column)
if salts_df: 
  salted_cty_df = salt_cities_dataset(salts_df, cty_src_path, e5d_cty_partitions, e5d_salt_column, e5d_join_column)
  salted_trx_df = salt_transactions_dataset(e5d_skew_factor, trx_src_path, e5d_salt_column, e5d_join_column)
  if salted_trx_df and salted_cty_df: 
    df_d = salted_join(salted_trx_df, salted_cty_df, e5d_join_column)
    if df_d:
      results_d = records_per_partition(df_d)
      print_statistics(results_d)

# COMMAND ----------

# MAGIC %md ### Reality Check #4.D
# MAGIC Run the following command to ensure that you are on track:

# COMMAND ----------

reality_check_04_d()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #4 - Final Check</h2>
# MAGIC 
# MAGIC Run the following command to make sure this exercise is complete:

# COMMAND ----------

reality_check_04_final()

# COMMAND ----------

# MAGIC %md
# MAGIC If all of the reality checks pass, feel free to continue on to [Exercise 05 - Submission]($./Exercise 05 - Submission).

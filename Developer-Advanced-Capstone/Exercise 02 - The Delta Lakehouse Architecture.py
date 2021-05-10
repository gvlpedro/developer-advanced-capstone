# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Exercise #2 - The Delta Lakehouse Architecture
# MAGIC 
# MAGIC ## Overview
# MAGIC The Delta Lakehouse Architecture calls for a series of datasets, each serving a specific purpose in the data lake as illustrated in the diagram below.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img style="width:800px" src="https://files.training.databricks.com/images/raw-bronze-silver-gold-pipeline.png">

# COMMAND ----------

# MAGIC %md
# MAGIC In this exercise, you will be asked to create various parts of a fictitious customer's Delta Lakehouse.
# MAGIC 
# MAGIC So that we can focus on key optimization techniques, we
# MAGIC * Have minimized the need for data transformations
# MAGIC * Are skipping the creation of tables typically created in this scenario
# MAGIC * Are in general skipping some of the grunt work.
# MAGIC 
# MAGIC Nonetheless, it will be up to you to implement all the pertinent optimizations for the specified datasets.

# COMMAND ----------

# MAGIC %md # Exercise #2.A - Setup
# MAGIC 
# MAGIC **In this step you will need to:**
# MAGIC 1. Configure a cluster to run DBR 7.3 LTS with 8 cores and attach it to this notebook
# MAGIC 2. Specify your Registration ID
# MAGIC 3. Run the setup notebook for this specific exercise
# MAGIC 4. Install the datasets for this specific exercise
# MAGIC 5. Run the reality check to verify your configuration and that the datasets were correctly installed

# COMMAND ----------

# MAGIC %md ## Setup - Registration ID
# MAGIC 
# MAGIC In the next commmand, please update the variable **`registration_id`** with the Registration ID you received when you signed up for this project.
# MAGIC 
# MAGIC For more information, see [Registration ID]($./Registration ID)

# COMMAND ----------

registration_id = "FILL_IN"

# COMMAND ----------

# MAGIC %md ## Setup - Run the exercise setup
# MAGIC 
# MAGIC Run the following cell to setup this exercise, declaring exercise-specific variables and functions.

# COMMAND ----------

# MAGIC %run ./_includes/Setup-Exercise-02

# COMMAND ----------

# MAGIC %md ## Setup - Install the Dataset
# MAGIC 
# MAGIC Run the following command to install the exercise's datasets into your workspace.
# MAGIC 
# MAGIC If you have successfully installed this exercise's dataset, there is no reason to install it a second time.

# COMMAND ----------

install_datasets_02(reinstall=False)

# COMMAND ----------

# MAGIC %md ## Reality Check #2.A
# MAGIC Run the following command to ensure that you are on track:

# COMMAND ----------

reality_check_02_a()

# COMMAND ----------

# MAGIC %md # Exercise #2.B - Bronze Datasets
# MAGIC 
# MAGIC The purpose of our bronze datasets is to "land" all our datasets into the Delta Lakehouse, perserving the original data and schema, but improving the stoage by updating the storage format and addressing issues like the classic "Tiny Files" problem.
# MAGIC 
# MAGIC Our bronze layer will consist of only three datasets, hereinafter referred to as **`bronze_cities`**, **`bronze_retailers`** and **`bronze_transactions`**.
# MAGIC 
# MAGIC Our main goal in processing the bronze datasets is to ingest all the raw data using establied best practices for the Delta Lakehouse.
# MAGIC 
# MAGIC While this "test" data is representative of the actual data to be processed in production, there are key features of each dataset that you need to be aware of:
# MAGIC 
# MAGIC ## The Cities Dataset
# MAGIC The dimension table commonly referred to as **`cities`** is finite - it is not expected to ever grow beyond it's current size and is representative of its production counterpart.
# MAGIC 
# MAGIC The assumption here is that it is a complete list of cities in the world and new cities are not added frequently enough to effect any decisions to be made here.
# MAGIC 
# MAGIC ## The Retailers Dataset
# MAGIC The dimension table commonly referred to as **`retailers`** is **NOT** finite - it is expected to ever grow over time but no where near as fast as our **`raw_transactions`** dataset.
# MAGIC 
# MAGIC Our test data has only ~100 records, but the production version averages around 215 GBs and only grows by a few GBs each year.
# MAGIC 
# MAGIC ## The Transactions Dataset
# MAGIC The fact table commonly referred to as **`transactions`** is the larget and fastest growing dataset in our Lakehouse.
# MAGIC 
# MAGIC The production version of this dataset is expected to grow into the 10-100s of GB per year. As a whole, it is expected to grow into the Terabites.
# MAGIC 
# MAGIC This dataset is heavily skewed towards US cities where most of our transaction occur, creating additional performance problems for our end users.
# MAGIC 
# MAGIC The job/function being developed here is for the first iteration of the dataset and should not account for any type of reocurring updates.
# MAGIC 
# MAGIC **In this step you will need to:**
# MAGIC 0. Implement the function **`configure_bronze_job(max_part_file_size)`**
# MAGIC   * Use this function to configure your Spark environment, inclusive of managing the maximum file size.
# MAGIC 0. Implement the function **`create_bronze_dataset(src_path, dst_path)`**
# MAGIC   * This function should ingest any specified parquet dataset and write that data, unmodified, to the path specified for that dataset.
# MAGIC   * The location of the raw dataset is identified as **`src_path`**.
# MAGIC   * The location of the bronze dataset is identified as **`dst_path`**.
# MAGIC 
# MAGIC **Special Notes:**
# MAGIC * To ensure continuity between steps, **`configure_bronze_job(..)`** will be invoked before each call to **`create_bronze_dataset(..)`** by the reality-checks below.
# MAGIC * The Spark environment will be reset before invoking your solutions.
# MAGIC 
# MAGIC **Specific Requirements:**
# MAGIC 0. The destination datasets must be in the Delta file format.
# MAGIC 0. The classic "Tiny Files Problem" must be mitigated - measured as having no more than one part-file less than 128 MB (134,217,728 bytes).
# MAGIC 0. The entire dataset should be optimized towards **`max_file_size_bytes`** (512 MB or 536,870,912 bytes) - measured as
# MAGIC   * Having configured the Delta engine to produce part-files no larger than the specified value.
# MAGIC   * Having no part-files larger than the specified value.
# MAGIC 0. All stray part-files must be removed from the final dataset so as to reduce storage costs.
# MAGIC 0. Auto-Optimization and Auto-Compaction must remained disabled - while it is generally a good idea to enable these features, they prohibit our ability to assess this exercise's core metrics.

# COMMAND ----------

# Show special details for this milestone
show_exercise_02_b_details()

# COMMAND ----------

# MAGIC %md ## Implement Exercise #2.B
# MAGIC 
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

# TODO
# Use this cell to complete your solution

def configure_bronze_job(max_part_file_size:int) -> None:
  sc.setJobDescription(f"Exercise 2.B - Configure Job")
  # FILL_IN

def create_bronze_dataset(src_path:str, dst_path:str, name:str) -> None:
  sc.setJobDescription(f"Exercise 2.B - Bronze-{name} Dataset")
  # FILL_IN

configure_bronze_job(max_part_file_size=max_file_size_bytes)         # Configure the Spark session for this job
create_bronze_dataset(raw_cty_path, bronze_cty_path, "Cities")       # Create the bronze version of the cities dataset
create_bronze_dataset(raw_ret_path, bronze_ret_path, "Retailers")    # Create the bronze version of the retailers dataset
create_bronze_dataset(raw_trx_path, bronze_trx_path, "Transactions") # Create the bronze version of the transactions dataset

# COMMAND ----------

# MAGIC %md ## Reality Check #2.B
# MAGIC Run the following three commands to ensure that you are on track with the bronze datasets:

# COMMAND ----------

reality_check_02_b_city()

# COMMAND ----------

reality_check_02_b_retailers()

# COMMAND ----------

reality_check_02_b_transactions()

# COMMAND ----------

# MAGIC %md # Exercise #2.C - Silver Datasets
# MAGIC 
# MAGIC Our silver datasets are used for serving data to Business Analyst and Data Scientest.
# MAGIC 
# MAGIC Given the inherit quality of our bronze data, we can simply copy all three datasets into the silver layer with very few changes (e.g. converting the file format to Delta and optimizing for file size).
# MAGIC 
# MAGIC For the sake of simplicity, we are going to forgo the simple "copy" of these three datasets into the silver layer and instead focus on a more important use cases.
# MAGIC 
# MAGIC ## Reporting By Year
# MAGIC Our Business Analyst join the **`silver_cities`** and **`silver_transactions`** datasets together so frequently that they are encountering significant performance problems.
# MAGIC 
# MAGIC These problems include skew, large shuffles, spilling partitions to disk but most notably, unnecissary ingestion of the entire dataset.
# MAGIC 
# MAGIC Most of these issues can be addressed by combining **`cities`** and **`transactions`** into one new datasets - aka denormalizing.
# MAGIC 
# MAGIC Further more, the reports our business analyst generate are almost exclusively limited to a single year of data allowing us to employ a low-cardinality index.
# MAGIC 
# MAGIC We will futher optimization this new datasets by creating the apppropriate high-cardinality index for those occasional needle-in-the-haystack queries.
# MAGIC 
# MAGIC **In this step you will need to:**
# MAGIC 0. Implement the function **`configure_silver_job(max_part_file_size)`**
# MAGIC   * Use this function to configure your Spark environment, inclusive of managing the maximum part-file size.
# MAGIC 0. Implement the function **`create_silver_trx_cty_df(cty_src_path, trx_src_path)`**
# MAGIC   * This function should ingest the dataset **`bronze_cities`**, the dataset **`bronze_transactions`**, join the two dataset by the column **`city_id`** and return the resulting **`DataFrame`** object for use in the subsequent function.
# MAGIC   * The location of the **`bronze_cities`** dataset is identified as **`cty_src_path`**.
# MAGIC   * The location of the **`bronze_transactions`** dataset is identified as **`trx_src_path`**.
# MAGIC 0. Implement the fucntion **`write_silver_trx_cty(df, dst_path)`**
# MAGIC   * This function should start with the **`DataFrame`** returned by **`create_silver_trx_cty_df`** identified as **`df`**.
# MAGIC   * The location of the new **`silver_trx_cty`** dataset is identified as **`dst_path`**.
# MAGIC 0. Employ the appropriate optimizations for this use case, optimizing both this job and future reads of the **`silver_trx_cty`** dataset.
# MAGIC 
# MAGIC **Special Notes:**
# MAGIC * To ensure continuity between steps, **`configure_silver_job(..)`** will be invoked before invoking **`create_silver_trx_cty_df(..)`** and **`write_silver_trx_cty(..)`** by the reality-check below.
# MAGIC * The Spark environment will be reset before invoking your solutions.
# MAGIC 
# MAGIC **Specific Requirements:**
# MAGIC 0. The destination dataset must be in the Delta file format.
# MAGIC 0. The classic "Tiny Files Problem" must be mitigated - measured as having no more than one part-file less than 128 MB (134,217,728 bytes).
# MAGIC 0. The entire dataset should be optimized towards **`max_file_size_bytes`** (512 MB or 536,870,912 bytes) - measured as
# MAGIC   * Having configured the Delta engine to produce part-files no larger than the specified value.
# MAGIC   * Having no part-files larger than the specified value.
# MAGIC 0. All stray part-files must be removed from the final dataset so as to reduce storage costs.
# MAGIC 0. Auto-Optimization and Auto-Compaction must remained disabled - while it is generally a good idea to enable these features, they prohibit our ability to assess this exercise's core metrics.
# MAGIC 0. The dataset **`silver_trx_cty`** should not have any duplicate columns
# MAGIC 0. The dataset **`silver_trx_cty`** should have a low-cardinality index on the column **`year`** using the "standard Spark" strategy for this type of index.
# MAGIC 0. The dataset **`silver_trx_cty`** should have three high-cardinality indexes on the columns **`trx_id`**, **`city_id`**, and **`retailer_id`** using the "standard Delta" strategy for this type of index.
# MAGIC 0. Considering the small and finite size of the **`bronze_cities`** dataset, the join operation should be optimized so as to preclude a Sort-Merge join.
# MAGIC 0. All the indexes in the **`silver_trx_cty`** dataset should advertise their particular indexing strategy by prefixing one of the following values to the appropriate column's name:
# MAGIC   * **`p_`** for Partitioned indexes
# MAGIC   * **`cp_`** for Computed-Partitioned indexes
# MAGIC   * **`z_`** for Z-Ordered indexes
# MAGIC   * **`bk_`** for Buckted indexes
# MAGIC   * **`bf_`** for Bloom Filters indexes
# MAGIC 0. Because we know the **`bronze_transactions`** dataset is heavily skewed by **`city_id`** the production version of this job will face large shuffle operations and subsequent spilling of those partitions. For this reason, the skew must be mitigated using AQE's Skew-Join found in Spark 3.x

# COMMAND ----------

# Show special details for this milestone
show_exercise_02_c_details()

# COMMAND ----------

# MAGIC %md ## Implement Exercise #2.C
# MAGIC 
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

# TODO
# Use this cell to complete your solution

def configure_silver_job(max_part_file_size:int) -> None:
  sc.setJobDescription(f"Exercise 2.C - Configure Job")
  # FILL_IN

def create_silver_trx_cty_df(cty_src_path:str, trx_src_path:str) -> pyspark.sql.DataFrame:
  sc.setJobDescription(f"Exercise 2.C - Create Silver Trx-Cty DataFrame")
  # FILL_IN

def write_silver_trx_cty(df:pyspark.sql.DataFrame, dst_path:str) -> None:
  sc.setJobDescription(f"Exercise 2.C - Create Silver Trx-Cty Dataset")
  
# Configure the Spark session for this job
configure_silver_job(max_part_file_size=max_file_size_bytes)
# Create the initial DataFrame that joins our two dataset
df = create_silver_trx_cty_df(bronze_cty_path, bronze_trx_path)
# Create the silver version of the cities-transactions dataset by writing it to disk
write_silver_trx_cty(df, silver_trx_cty_path)

# COMMAND ----------

# MAGIC %md ## Reality Check #2.C
# MAGIC Run the following command to ensure that you are on track:

# COMMAND ----------

reality_check_02_c()

# COMMAND ----------

# MAGIC %md # Exercise #2.D - Gold Datasets
# MAGIC 
# MAGIC Generally speaking, the silver and gold dataset are considered to be in the "serving layer" - often combined as needed by Data Scientest and Data Analyst.
# MAGIC 
# MAGIC Compare that to the "bronze layer" which is generally kept from the public and is used soley to produce silver and gold dataset.
# MAGIC 
# MAGIC All that being said, gold dataset are for specialty products that aim to serve a specific need beyond basic optimization techniques like denormalization.
# MAGIC 
# MAGIC This can often include data cubes, dataset that are optimized for machine learning, or in as in this case, bucketing.
# MAGIC 
# MAGIC ## Terabyte Data Science
# MAGIC 
# MAGIC While our Data Scientist make regular use of the **`silver_trx_cty`** dataset, they have an additional use case not supported by the current inventory of dataset.
# MAGIC 
# MAGIC In this use case, Data Scientist have the need to train models against the join of the **`retailers`** dataset and the **`transactions`** dataset.
# MAGIC 
# MAGIC Unlike our Business Analyst's use case that almost exclusively looks at a year at a time, our Data Scientest need to train against all historical data but have the additonal requirement to explore the transactions of a specific retailer as they analize the results.
# MAGIC 
# MAGIC To futher complicate our problem, at terabyte scale the shuffle operations resulting from a join between **`silver_transactions`** and **`silver_retailers`** would make model training cost-prohibited - especially given the number of runs needed to tune and test many models.
# MAGIC 
# MAGIC After some internal debate, it was decided to create a bucketed dataset from the two datasets which should preclude the expensive shuffle operations and also meet the aditional analysis requirement by partitioning the data by retailer.
# MAGIC 
# MAGIC **In this step you will need to:**
# MAGIC 0. Implement the function **`bucket_dataset(src_path, dst_path, table_name, buckets, name)`**
# MAGIC   * This function should ingest the specified "bronze" dataset, bucket the dataset, and then use the result to create the specified bucketed dataset.
# MAGIC   * The location of the bronze dataset is identified as **`src_path`**.
# MAGIC   * The location of the bucketed dataset is identified as **`dst_path`**.
# MAGIC   * The name of the bucketed table is identified as **`table_name`**.
# MAGIC   * The total number of buckets is identified as **`buckets`**.
# MAGIC 
# MAGIC **Special Notes:**
# MAGIC * To ensure continuity between steps, **`bucket_dataset(..)`** will be invoked twice by the reality-checks below.
# MAGIC   * Once for the **`bucketed_ret`** table
# MAGIC   * Again for the **`bucketed_trx`** table
# MAGIC * The Spark environment will be reset before invoking your solutions.
# MAGIC * The two tables will be further tested by executing a join by **`retailer_id`** on the two tables.
# MAGIC 
# MAGIC **Specific Requirements:**
# MAGIC 0. The destination datasets must be in the parqut file format.
# MAGIC 0. The classic "Tiny Files Problem" must be mitigated - measured as having no more than one part-file less than 128 MB (134,217,728 bytes).
# MAGIC 0. All the indexes in the **`bucketed_ret`** and **`bucketed_trx`** tables should advertise their particular indexing strategy as outlined in Step 2.C above.

# COMMAND ----------

# Show special details for this milestone
show_exercise_02_d_details()

# COMMAND ----------

# MAGIC %md ## Implement Exercise #2.D
# MAGIC 
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

# TODO
# Use this cell to complete your solution

def bucket_dataset(src_path:str, dst_path:str, table_name:str, buckets:int, name:str) -> None:
  sc.setJobDescription(f"Creating Table {table_name}")
  # FILL_IN

bucket_dataset(bronze_ret_path, bucketed_ret_path, bucketed_ret_table, buckets, "Retailers")
bucket_dataset(bronze_trx_path, bucketed_trx_path, bucketed_trx_table, buckets, "Transactions")

# COMMAND ----------

# MAGIC %md ## Reality Check #2.D
# MAGIC Run the following two commands to ensure that you are on track with the tables **`bucketed_ret`** and **`bucketed_trx`**

# COMMAND ----------

reality_check_02_d_retailers()

# COMMAND ----------

reality_check_02_d_transactions()

# COMMAND ----------

# MAGIC %md # Exercise #2 - Final Check
# MAGIC 
# MAGIC Run the following command to make sure this exercise is complete:

# COMMAND ----------

reality_check_02_final()

# COMMAND ----------

# MAGIC %md
# MAGIC If all of the reality checks pass, feel free to continue on to [Exercise 03 - Optimal File Size]($./Exercise 03 - Optimal File Size).

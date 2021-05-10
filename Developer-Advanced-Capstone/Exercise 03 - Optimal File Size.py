# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md 
# MAGIC # Exercise #3 - Optimal File Size
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC In Exercise #2, one of the key requirements was to manage files on disk.
# MAGIC 
# MAGIC One reasons is to manage costs, but more importantly, properly structured datasets is a key concept behind optimizing Apache Spark.
# MAGIC 
# MAGIC However, there is not [yet] one magical solution for producing that perfect sized part-file.
# MAGIC 
# MAGIC This is due in part to the fact that different situations call for different requirements - that classic "it depends".
# MAGIC 
# MAGIC For example, prevailing wisdom calls for 128-1024 MB part-files but, datasets that frequently employing a Delta-Merge can actually bennifit from significantly smaller part-files - in the neighborhood of 32 MBs in some cases.
# MAGIC 
# MAGIC There is also the reality that while some of the "automatic" methods for optimizing part-files can produce reall good results, it is often at the cost of running additional jobs, and thus more compute costs, and thus longer runs in the production of a single datasets, not to mention, an entire pipline.
# MAGIC 
# MAGIC The idea is that this overhead on the production side pays for itself in the end because the Data Engineer is not having to solve this and on the read side, Data Scientist and Data Analyst will benifit from even the simplist of automatic solutions.
# MAGIC 
# MAGIC In this exercise, you will be called upon to solve our classic "Tiny Files Problems" in a multitude of different ways.
# MAGIC 
# MAGIC While it might seem somewhat redundant to employ N different solutions, just remember that answering the question "Which one of these is the best solution" starts with the statement "It depends..."

# COMMAND ----------

# MAGIC %md # Exercise #3.A - Setup 
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

# MAGIC %run ./_includes/Setup-Exercise-03

# COMMAND ----------

# MAGIC %md ## Setup - Install the Dataset
# MAGIC 
# MAGIC Simply run the following command to install the exercise's datasets into your workspace.

# COMMAND ----------

install_datasets_03(reinstall=False)

# COMMAND ----------

# MAGIC %md ## Reality Check #3.A
# MAGIC Run the following command to ensure that you are on track:

# COMMAND ----------

reality_check_03_a()

# COMMAND ----------

# MAGIC %md # Exercise #3.B - Manual Compaction
# MAGIC 
# MAGIC The main goal in this step is to manually compact our dataset towards a target part-file size.
# MAGIC 
# MAGIC There are at least two potential reasons this may be the preferred solution:
# MAGIC 0. The goal is to optimize **this** job by avoiding the extra jobs associated with the various "automatic" solutions
# MAGIC 0. Requirements call for a data format other than "Delta" where these "automatic" options are not even available
# MAGIC 
# MAGIC For this solution, we will be making our computations based on the dataset's file size on disk.<br/>
# MAGIC In this case, we are consciously ignoring the compression ratios between disk and memory.<br/>
# MAGIC More advanced solutions might factor this in, but we won't be doing that today.
# MAGIC 
# MAGIC Another soultion might be to estimate the size of each row and then base the computation on the number of rows.<br/>
# MAGIC We will also be saving this solution for another day because the pattern is roughly the same.
# MAGIC 
# MAGIC **In this step you will need to:**
# MAGIC 1. Implement the function **`calculate_part_file_count(files, target_part_file_size)`** which returns the number of desired part-files
# MAGIC   * The list of files on disk is identified as **`files`**, an instance of **`dbutils.FileInfo`**
# MAGIC   * The desired, average file size is identified as **`target_part_file_size`** 
# MAGIC 2. Implement the function **`compact_dataset(src_path, dst_path, part_files_count)`** which reads in the dataset, transforms it as required, and then writes it back to disk.
# MAGIC   * The location of the dataset is identified as **`src_path`**
# MAGIC   * The location of the dataset is identified as **`dst_path`**
# MAGIC   * The final number of part-files is identified as **`part_files_count`**
# MAGIC 
# MAGIC **Special Notes:**
# MAGIC * In your calculations, all fractions should be rounded to the ceiling
# MAGIC * To ensure continuity between steps, **`calculate_part_file_count(..)`** and **`compact_dataset(..)`** will be invoked by the reality-check below
# MAGIC * The Spark environment will be reset before invoking your solutions
# MAGIC * Your implementation of **`calculate_part_file_count(..)`** will be retested against multiple, samples datasets
# MAGIC * The actual algorithim you are expected to follow is outlined a few cells below.
# MAGIC 
# MAGIC **Specific Requirements:**
# MAGIC * The destination dataset must be in the parquet file format

# COMMAND ----------

show_exercise_03_b_details()

# COMMAND ----------

# MAGIC %md ## Implement Exercise #3.B
# MAGIC 
# MAGIC **The general algorithim is as follows:**
# MAGIC 1. Determine the size of your datset on disk
# MAGIC   * A list of **`FileInfo`** objects will provided for you
# MAGIC 2. Decide what your ideal part-file size is
# MAGIC   * The "ideal" size will be declared for you (**`max_file_size_bytes`**)
# MAGIC 3. Compute the estimated number of part-files required by dividing the **`size-on-disk`** by the **`ideal-file-size`**
# MAGIC 4. Configure a cluster with enough cores to write all spark-partitions in a single iteration (e.g. cores >= partitions)
# MAGIC   * We will use the "standard" cluster setup at the start of this exercise
# MAGIC 5. Read in your data, transform the data so as to produce the correct number of part-files upon write, and then write that datset to disk
# MAGIC 6. Check the Spark UI for spill and any other issues
# MAGIC   * We will skip this step entirely as we are not looking to "tune" the method, just implement it
# MAGIC 
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

# TODO
# Use this cell to complete your solution
from dbutils import FileInfo

def calculate_part_file_count(files:FileInfo, target_part_file_size:int) -> int:
  sc.setJobDescription("Exercise #3.B - Calculate Part-File Count")
  # FILL_IN

def compact_dataset(src_path:str, dst_path:str, part_files_count:int) -> None:
  sc.setJobDescription("Exercise #3.B - Compacting Dataset")
  # FILL_IN
  
# List our files using dbutils.fs.ls(..)
files = dbutils.fs.ls(source_path)
# Calculate how many part files we should have.
part_files_count = calculate_part_file_count(files, step_b_file_size)
# Compact and write the dataset
compact_dataset(source_path, step_b_path, part_files_count)

# COMMAND ----------

# MAGIC %md ## Reality Check #3.B
# MAGIC Run the following command to ensure that you are on track:

# COMMAND ----------

reality_check_03_b()

# COMMAND ----------

# MAGIC %md # Exercise #3.C - AutoOptimize Existing Delta Dataset
# MAGIC 
# MAGIC The main goal in this step is to enable Delta's "auto optimize" features for an existing dataset.
# MAGIC 
# MAGIC Unlike other scenarios, our goal is **not** to optimize the existing dataset - we can presume this was already done in a previous step.
# MAGIC 
# MAGIC Rather our goal is to ensure that future part-files appended to this datasets compact towards the specified max file size - for example, as new data is appended to this dataset.
# MAGIC 
# MAGIC **In this step you will need to:**
# MAGIC 0. Implement the function **`auto_optimize_existing_dataset(path, max_file_size)`** which alters an existing dataset to enable auto optimization on future writes
# MAGIC   * This location of the existing dataset is identified as **`path`**
# MAGIC   * The maximum file size for new part-files is identified as **`max_file_size`**
# MAGIC   * This function should enable the Delta features known as Optimize Write and Auto Compact (together commonly known as "Auto Optimize")
# MAGIC 
# MAGIC **Special Notes:** 
# MAGIC * To ensure continutity between steps, a new dataset will be created for you before invoking **`auto_optimize_existing_dataset`** by the reality-check below
# MAGIC * The Spark environment will be reset before invoking your solution
# MAGIC * The provided dataset will be in the Delta format
# MAGIC 
# MAGIC **Specific Requirements:**
# MAGIC * Future DBR versions will allow you to store in the dataset the maximum part-file size. For this DBR, you will be limited to configuring the Spark session.

# COMMAND ----------

show_exercise_03_c_details()

# COMMAND ----------

# MAGIC %md ## Implement Exercise #3.C
# MAGIC 
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

# TODO
# Use this cell to complete your solution

def auto_optimize_existing_dataset(path:str, max_file_size:int) -> None:
  sc.setJobDescription("Exercise #3.C")
  # FILL_IN
  
# Re-create the dataset to be optimized, if needed
create_step_c_dataset()

# Update the dataset's configuration 
auto_optimize_existing_dataset(step_c_path, step_c_file_size)

# COMMAND ----------

# MAGIC %md ## Reality Check #3.C
# MAGIC Run the following command to ensure that you are on track:

# COMMAND ----------

reality_check_03_c()

# COMMAND ----------

# MAGIC %md # Exercise #3.D - Always AutoOptimize Delta Datasets
# MAGIC 
# MAGIC The main goal in this step is to enable Delta's "auto optimize" feature for any new datasets created during this Spark session.
# MAGIC 
# MAGIC Unlike the previous step that modified an existing dataset, in this scenario we are altering the Spark environment to enable this feature for all future datasets. 
# MAGIC 
# MAGIC In this case, you will be expected to modify the current Spark session - other options might be to alter the cluster so that this feature is enabled for all users of the cluster, or even via cluster policies.
# MAGIC 
# MAGIC **In this step you will need to:**
# MAGIC 1. Implement the function **`auto_optimize_future_datasets(max_file_size)`** which alters the environment to enable the "Auto Optimize" features for the current Spark session.
# MAGIC   * The maximum file size for new part-files is identified as **`max_file_size`**
# MAGIC   * This function should enable the Delta features known as Optimize Write and Auto Compact (together commonly known as "Auto Optimize") for all new Delta datasets
# MAGIC 
# MAGIC **Special Notes:**
# MAGIC * To ensure continutity between steps, **`auto_optimize_future_datasets(..)`** will be invoked by the reality-check below before creating the test dataset.
# MAGIC * The Spark environment will be reset before invoking your solution
# MAGIC * The final dataset (that is created for you) will be in the Delta format
# MAGIC 
# MAGIC **Specific Requirements:**
# MAGIC * Future DBR versions will allow you to store in the dataset the maximum part-file size. For this DBR, you will be limited to configuring the Spark session.
# MAGIC * The current Spark session must be configured so as to enable "Auto Optimize" on all new datasets
# MAGIC * The test datasets, which we will create, must inherit the "Auto Optimize" feature upon creation

# COMMAND ----------

show_exercise_03_d_details()

# COMMAND ----------

# MAGIC %md ## Implement Exercise #3.D
# MAGIC 
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

# TODO
# Use this cell to complete your solution

def auto_optimize_future_datasets(max_file_size):
  sc.setJobDescription("Exercise #3.D")
  # FILL_IN

# Update the dataset configuration 
auto_optimize_future_datasets(step_d_file_size)
# Create the dataset to be optimized
create_step_d_dataset()

# COMMAND ----------

# MAGIC %md ## Reality Check #3.D
# MAGIC Run the following command to ensure that you are on track:

# COMMAND ----------

reality_check_03_d()

# COMMAND ----------

# MAGIC %md # Exercise #3.E - Optimize Upon Ingest
# MAGIC 
# MAGIC One "problem" with all the previous solutions is that they all require addtional jobs and/or stages to implement a compaction operation of one form or another.
# MAGIC 
# MAGIC Another option, especially if there are no wide transformations between the initial read and write, is to ingest the data so that it starts with an ideal Spark-partition size that will resemble our ideal part-file size.
# MAGIC 
# MAGIC With this solution, the aim is to ensure that each part-file ingested into Apache Spark is coalesced into properly sized Saprk-partitions. Once the data is written to disk, assuming no wide transformations are to be invoked, the result should be properly sized part-file.
# MAGIC 
# MAGIC **In this step you will need to:**
# MAGIC 1. Implement the function **`compact_upon_ingest(max_partition_size)`** which coalesces the Spark-partition towards the specified maximum partition size upon ingest.
# MAGIC 2. The maximum size for each Spark-partition is specified by **`max_partition_size`**
# MAGIC 
# MAGIC **Special Notes:**
# MAGIC * To ensure continutity between steps, **`compact_upon_ingest(...)`** will be invoked before creating the test dataset by the reality-check below.
# MAGIC * The Spark environment will be reset before invoking your solution
# MAGIC 
# MAGIC **Specific Requirements:**
# MAGIC * No additional requirements

# COMMAND ----------

show_exercise_03_e_details()

# COMMAND ----------

# MAGIC %md ## Implement Exercise #3.E
# MAGIC 
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

# TODO
# Use this cell to complete your solution

def compact_upon_ingest(max_partition_size):
  sc.setJobDescription("Exercise #3.E")
  # FILL_IN

# Execute your solution  
compact_upon_ingest(step_e_partition_size)
# Write the dataset to disk
create_step_e_dataset()  
# Preview the result of the write operation
display(dbutils.fs.ls(step_e_path_dst))

# COMMAND ----------

# MAGIC %md ## Reality Check #3.E
# MAGIC Run the following command to ensure that you are on track:

# COMMAND ----------

reality_check_03_e()

# COMMAND ----------

# MAGIC %md # Exercise #3 - Final Check
# MAGIC 
# MAGIC Run the following command to make sure this exercise is complete:

# COMMAND ----------

reality_check_03_final()

# COMMAND ----------

# MAGIC %md
# MAGIC If all of the reality checks pass, feel free to continue on to [Exercise 04 - Salting]($./Exercise 04 - Salting).

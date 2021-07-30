# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md # Exercise #1 - Project Overview & Getting Started
# MAGIC 
# MAGIC The capstone project aims to assess advanced skills as it relates to Apache Spark, the DataFrame APIs,<br/>
# MAGIC the Delta APIs, the Structured Streaming APIs and propritary Databricks APIs.
# MAGIC 
# MAGIC The approach taken here assumes that you are familiar with and have some experience with the following entities:
# MAGIC * **`Spark APIs`**
# MAGIC * **`DataFrame APIs`**
# MAGIC * **`Structured Streaming APIs`**
# MAGIC * **`Delta APIs`** (OSS-Delta and Databricks-Delta)
# MAGIC 
# MAGIC Throughout this project, you will be given specific instructions and it is our expectation that you will<br/>
# MAGIC be able to complete each exercise drawing on your existing knowledge as well as other sources <br/>
# MAGIC such as the <a href="https://spark.apache.org/docs/latest/api.html" target="_blank">Spark API Documentation</a>, <a href="https://docs.databricks.com/delta/" target="_blank">Delta Lake and Delta Engine Docs</a> and other <a href="https://docs.databricks.com/" target="_blank">Databricks-Specific Docs</a>

# COMMAND ----------

# MAGIC %md ## Project Overview
# MAGIC The exercises in this capstone project focus on five of the most common performance problems seen by Databricks and its customers:
# MAGIC * **Skew** - the uneven distribution of data across many partition.
# MAGIC * **Shuffle** - the [unnecissary] moving of data between executors.
# MAGIC * **Spill** - the writing of partition data to disk in the absence of sufficent RAM.
# MAGIC * **Serialization** - the overhead seen in the use, or missuse, of custom code (e.g. not using higher order functions).
# MAGIC * **Storage** - often referred to as the "Tiny Files Problem", this is the overhead seen by miss-managed data at rest.
# MAGIC 
# MAGIC To minimize your time spent on this project, special considerations were made in its development:
# MAGIC 0. We are using small datasets to transform hour long job runs into mere minutes.
# MAGIC 0. We are advocating the use of small clusters, as few as 8 Spark-Cores, so as to minimize your costs.
# MAGIC 0. We are supporting the execution of this project on local-mode (or single-node) clusters.
# MAGIC 0. All exercises are designed to run aginst the same cluster configuration, as opposed to exercise-specific clusters.
# MAGIC 
# MAGIC With those limitations, there are a number of important facts to make note of as you progress through each exercise:
# MAGIC 0. You will not be required to diagnose performance problems within the Spark UI.
# MAGIC 0. Some performance problems, while they technically exist, are too small to be diagnosed as actual performance problems.
# MAGIC 0. Evaluation of an exercise will not be based on improvements of execution time, but rather by satisfying specific requirements.
# MAGIC 0. We will be altering the Spark environment, disabling key features such as AQE as required, to replicate scenarios seen in Spark 2.x, 3.x without & without AQE and even Open Source Spark and Delta.
# MAGIC 
# MAGIC All along the way, we will be providing you with specific instructions for each exercise and "reality checks" after each milestone to assert that you are meeting all of the objectives.

# COMMAND ----------

# MAGIC %md ## The Exercises
# MAGIC 
# MAGIC * In **Exercise 1** (this notebook) we introduce the registration procedure, the installation of our datasets and the reality-checks meant to aid you in your progress thought this capstone project.
# MAGIC 
# MAGIC * In **Exercise 2**, you will be tasked with producing a performant Delta Pipline with three major milestones: creation of a bronze, silver and gold table. You will be challened to employ different optimization strategies unique to the production of each table.
# MAGIC 
# MAGIC * In **Exercise 3**, the focus switches to producing, on disk, optimially sized datasets. Here you will be challenged to employ different "automatic" and "manual" strategies.
# MAGIC 
# MAGIC * In **Exercise 4**, the emphasis switches to employing the three main strategies for mitigating skew, and with that minimizing, if not precluding, spill.
# MAGIC 
# MAGIC * In **Exercise 5**, we provide final instructiosn for submitting your capstone project.

# COMMAND ----------

# MAGIC %md # Exercise #1.A - Setup
# MAGIC 
# MAGIC **In this step you will need to:**
# MAGIC 1. Configure a cluster to run DBR 7.3 LTS with 8 cores and attach it to this notebook
# MAGIC 2. Specify your Registration ID
# MAGIC 3. Run the setup notebook for this specific exercise
# MAGIC 4. Install the datasets for this specific exercise
# MAGIC 5. Run the reality check to verify your configuration and that the first dataset was correctly installed
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> These steps represent the basic pattern used by each exercise in this capstone project<br/>

# COMMAND ----------

# MAGIC %md ## Setup - Create A Cluster
# MAGIC 
# MAGIC For the sake of simplicity, every exercise can use the same cluster configuration.
# MAGIC 
# MAGIC Further more, we have designed this capstone project to work with a minimal amount of resources across all couds such as
# MAGIC * **`Standard_DS3_v2`** and **`Standard_DS4_v2`** on Microsfot Azure
# MAGIC * **`i3.xlarge`** and **`i3.2xlarge`** on Amazone Web Services
# MAGIC * or the "stock" cluster on the Databricks Communit Edition
# MAGIC 
# MAGIC The only hard requirements are:
# MAGIC * The use of Databricks Runtime 7.3 LTS
# MAGIC * The use of 8 Spark-Cores
# MAGIC 
# MAGIC Feel free to use two, one or zero workers in your cluster configuration, adjusting the **`spark.master`** configuration property as needed.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> On the Databricks Community Edition, you cluster is advertised as 2 cores.<br/>
# MAGIC This is actually 2 VM-Cores, but Apache Spark is automatically configured to use 8 Spark-Cores.

# COMMAND ----------

# MAGIC %md ## Setup - Registration ID
# MAGIC 
# MAGIC In the next commmand, please update the variable **`registration_id`** with the Registration ID you received when you signed up for this project.
# MAGIC 
# MAGIC For more information, see [Registration ID]($./Registration ID)

# COMMAND ----------

# TODO
registration_id = "FILL_IN"

# COMMAND ----------

# MAGIC %md ## Setup - Run the exercise setup
# MAGIC 
# MAGIC Run the following cell to setup this exercise, declaring exercise-specific variables and functions.

# COMMAND ----------

# MAGIC %run ./_includes/Setup-Exercise-01

# COMMAND ----------

# MAGIC %md ## Setup - Install the Dataset
# MAGIC 
# MAGIC Simply run the following command to install the exercise's datasets into your workspace.

# COMMAND ----------

# At any time during this project, you can reinstall the source datasets
# for any given exercise by setting reinstall=True. In future exercises,
# these datasets will not be automtically reinstalled so as to save time.
install_datasets_01(reinstall=True)

# COMMAND ----------

files = dbutils.fs.ls(f"{working_dir}/exercise_01/raw")
display(files)

# COMMAND ----------

text = dbutils.fs.head(f"{working_dir}/exercise_01/raw/test.txt")
print(text)

# COMMAND ----------

# MAGIC %md ## Reality Check #1.A
# MAGIC Run the following command to ensure that you are on track:

# COMMAND ----------

reality_check_01_a()

# COMMAND ----------

# MAGIC %md
# MAGIC If all of the reality checks pass, feel free to continue on to [Exercise #02 - The Delta Lakehouse Architecture]($./Exercise 02 - The Delta Lakehouse Architecture).

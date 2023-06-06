-- Databricks notebook source
select * from global_temp.gv_results_view;

-- COMMAND ----------

create database if not exists demo;

-- COMMAND ----------

show databases;

-- COMMAND ----------

desc database extended demo;

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------



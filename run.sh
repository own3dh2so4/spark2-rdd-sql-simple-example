#!/usr/bin/env bash

echo "[INFO] Compile:"
sbt assembly
echo "[INFO] Execute:"
spark-submit --class "es.own3dh2so4.Main${1}" target/scala-2.11/spark-movies-dataset-assembly-1.0.jar
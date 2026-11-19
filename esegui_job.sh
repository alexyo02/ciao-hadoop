#!/bin/bash
# Esegue i 3 job sui 3 dataset per 3 volte e salva i tempi in un file CSV.
# Uso: bash esegui_job.sh local    (oppure pseudo, yarn)

MODE=$1
JAR=BigData.jar
TIMES=tempi/tempi-$MODE.csv

if [ -z "$MODE" ]; then
    echo "Uso: bash esegui_job.sh local|pseudo|yarn"
    exit 1
fi

mkdir -p logs outputs tempi
echo "job,dataset,esecuzione,millisecondi" > $TIMES
echo "Modalità: $MODE - filesystem: $(hdfs getconf -confKey fs.defaultFS)"

for RUN in 1 2 3; do
    for JOB in 1 2 3; do
        for DATASET in A B C; do
            NAME=job$JOB-$MODE-dataset_$DATASET-run$RUN

            hadoop fs -rm -r -f hadoop_out/$NAME
            hadoop jar $JAR com.BigData.Job${JOB}_Driver datasets/dataset_$DATASET hadoop_out/$NAME 2>&1 | tee logs/$NAME.log
            hadoop fs -getmerge hadoop_out/$NAME outputs/$NAME.txt

            MS=$(grep "Tempo di esecuzione" logs/$NAME.log | awk '{print $4}')
            echo "$JOB,$DATASET,$RUN,$MS" >> $TIMES
        done
    done
done

column -t -s, $TIMES

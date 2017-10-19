#!/bin/sh
# Parametros de Entrada
TABLE=$1
ODATE=$2

echo "Tabela: $TABLE"
echo "Data..: $ODATE"

# Verifica argumentos do script
if [ $# -lt 2 ]
  then
    echo "Quantidade de argumentos invalida"
    exit 1
fi

#/sistema/bdi/cfg/python/bdi_ingestion.py

#spark-submit /sistema/bdi/bin/python/bdi_ingestion.py ingestao $TABLE $ODATE

#spark-submit --executor-memory 500M --executor-cores 1 --conf spark.yarn.appMasterEnv.SPARK_HOME=/produtos/bdr/parcels/CDH/lib/spark/ --master yarn-client /sistema/bdi/bin/python/bdi_ingestion.py ingestao $TABLE $ODATE

#spark-submit --executor-memory 2G --executor-cores 1 --jars /sistema/bdi/bin/spark/spark-csv_2.11-1.5.0.jar,/sistema/bdi/bin/spark/commons-csv-1.2.jar --conf spark.yarn.appMasterEnv.SPARK_HOME=/produtos/bdr/parcels/CDH/lib/spark/ --master yarn-client /sistema/bdi/bin/python/bdi_ingestion.py ingestao $TABLE $ODATE

spark-submit --driver-cores 4 --driver-memory 4G --executor-memory 2G --num-executors 25 --jars /sistema/bdi/bin/spark/spark-csv_2.11-1.5.0.jar,/sistema/bdi/bin/spark/commons-csv-1.2.jar --master yarn-cluster --conf spark.yarn.appMasterEnv.SPARK_HOME=/produtos/bdr/parcels/CDH/lib/spark/ /sistema/bdi/bin/python/bdi_ingestion.py ingestao $TABLE $ODATE

from pyspark.sql import HiveContext
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, TimestampType, DateType
import sys
from sys import argv
from os import system
import logging

if len(argv[1:]) < 5:
    setError('Parametros invalidos', len(argv[0:]))
else:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    fileSource = argv[1]
    pathSave = argv[2]
    database = argv[3]
    table = argv[4]
    partition = argv[5]

    sc = SparkContext()
    sqlContext = SQLContext(sc)
    hive = HiveContext(sc)

    rdd = sc.textFile('file:'+fileSource)
    rddSplit = rdd.map(lambda l: l.split(",")).map(lambda p: (p[0], p[1],p[2]))

    schemaString = "usuario produto avaliacao"
    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
    schema = StructType(fields)
    df = sqlContext.createDataFrame(rddSplit, schema)

    df.printSchema()

    df.write.mode('append').parquet(pathSave)

    cmd_hive = "ALTER TABLE " + database.lower() + "." + table.lower() + " ADD PARTITION(data='" + partition + "') location '" + pathSave + "'"
    print(cmd_hive)
    hive.sql(cmd_hive)

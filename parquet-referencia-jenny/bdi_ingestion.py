from pyspark.sql import HiveContext
from pyspark import SparkContext
from pyspark.sql import  SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, TimestampType, DateType
import sys
from re import sub
from sys import argv
import os
import subprocess
import re
import glob
from os import system
from decimal import Decimal
from datetime import date,datetime, time, timedelta
import logging
#----------------------------------------------------------
#CONSTANTS
#----------------------------------------------------------
logging.basicConfig(
     level=logging.INFO,
     format= '%(asctime)s: BDI_INGESTION %(message)s'
 )
logger = logging.getLogger("")

def setError(msg):
    #print(msg)
    logger.info(msg)
    sys.exit(1)

def setInfo(msg):
    #print(msg)
    logger.info(msg)

if len(argv[1:]) < 3:
    setInfo('--> Falta de parametro (funcao<waitFile> tabela<PEDT001> odate<120617>), total informado: ')
else:

    #SET ARGs
    func = argv[1]#'Funcao'
    table_name = argv[2] #'tabela AEDTGEN'
    odate = argv[3] #'odate  120617'
    setInfo("--> Inicio do processo Funcao:{} Tabela:{} odate:{} em {}".format(func,table_name,odate,datetime.now()))

    #CREATE CONTEXT
	#yarn-cluster
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    sqlContext.setConf('spark.sql.parquet.compression.codec', 'snappy')

    #READ CONF FILE
    readConf = sc.textFile('/sistemas/bdi/cfg/'+table_name+'.conf')
    if readConf.isEmpty():
        setError('Arquivo inexistente: /sistemas/bdi/cfg/{}.conf'.format(table_name))

    listConf = readConf.map(lambda x: x.split('='))
    listConf.cache()

    #SET CONFIG TO VARIABLE
    retention = listConf.filter(lambda x: 'retention' in x).map(lambda y: (y[1])).collect()
    hdfs_path_source = listConf.filter(lambda x: 'hdfs_path_source' in x).map(lambda y: (y[1])).collect()
    hdfs_path_save = listConf.filter(lambda x: 'hdfs_path_save' in x).map(lambda y: (y[1])).collect()
    database = listConf.filter(lambda x: 'database' in x).map(lambda y: (y[1])).collect()
    file_count = listConf.filter(lambda x: 'file_count' in x).map(lambda y: (y[1])).collect()
    os_path_source = listConf.filter(lambda x: 'os_path_source' in x).map(lambda y: (y[1])).collect()

    #NEW VARIABLE
    quote = listConf.filter(lambda x: 'quote' in x).map(lambda y: (y[1])).collect()
    demiliter = listConf.filter(lambda x: 'demiliter' in x).map(lambda y: (y[1])).collect()
    header = listConf.filter(lambda x: 'header' in x).map(lambda y: (y[1])).collect()
    quoteMode = listConf.filter(lambda x: 'quoteMode' in x).map(lambda y: (y[1])).collect()

    retention = retention[0]
    hdfs_path_source = hdfs_path_source[0]
    hdfs_path_save = hdfs_path_save[0]
    database = database[0]
    file_count = int(file_count[0])
    os_path_source= os_path_source[0]

    if len(quote) == 0:
        quote = '"'
    else:
        quote = quote[0]

    if len(demiliter) == 0:
        demiliter = ';'
    else:
        demiliter = demiliter[0]

    #true or false
    if len(header) == 0:
        header = 'false'
    else:
        header = header[0]

    # quoteMode ALL, MINIMAL (default), NON_NUMERIC, NONE
    if len(quoteMode) == 0:
        quoteMode = 'NON_NUMERIC'
    else:
        quoteMode = quoteMode[0]


    if not retention:
        setError('Configuracao inexistente para retention')
    if not hdfs_path_source:
        setError('Configuracao inexistente para hdfs_path_source')
    if not hdfs_path_save:
        setError('Configuracao inexistente para hdfs_path_save')
    if not database:
        setError('Configuracao inexistente para database')
    if not file_count:
        setError('Configuracao inexistente para file_count')
    if not os_path_source:
        setError('Configuracao inexistente para os_path_source')

    setInfo('--> retencao: {}'.format(retention))
    setInfo('--> path_source: {}'.format(hdfs_path_source))
    setInfo('--> path_save: {}'.format(hdfs_path_save))
    setInfo('--> database: {}'.format(database))
    setInfo('--> file_count: {}'.format(file_count))
    setInfo('--> os_path_source: {}'.format(os_path_source))
    setInfo('--> quote: {}'.format(quote))
    setInfo('--> demiliter: {}'.format(demiliter))
    setInfo('--> header: {}'.format(header))
    setInfo('--> quoteMode: {}'.format(quoteMode))

    #SET PATHs
    hdfs_path_csv = hdfs_path_source + table_name + '*' + odate + '.CSV'
    hdfs_path_bst = hdfs_path_source + table_name + '*' + odate + '.BST'
    hdfs_path_ctr = hdfs_path_source + table_name + '*' + odate + '.CTR'
    hdfs_path_save = hdfs_path_save
    hdfs_path_parquet =  hdfs_path_save +'odate='+ odate
    hdfs_path_all = hdfs_path_source + table_name + '*'+odate + '.*'

    os_path_csv = os_path_source + table_name + '*' + odate + '.CSV'
    os_path_bst = os_path_source+ table_name + '*' + odate + '.BST'
    os_path_ctr = os_path_source+ table_name + '*' + odate + '.CTR'
    os_path_all = os_path_source+ table_name + '*' + odate + '.*'

    hdfs_schema_model = '/sistemas/bdi/cfg/' + table_name + '.def'

    total_line=0
    total_file=0
    name_file=''
    size_file=0

    dt = datetime(int(str(date.today().year)[:2] + (odate[4:])), int(odate[2:-2]),
                           int(odate[:2])).date()
    dt = str(dt.strftime('%Y/%m/%d')).replace('/', '-')

#----------------------------------------------------------
#WAIT FILE
#----------------------------------------------------------
def waitfile():
    setInfo('--> Inciando waitfile')
    setInfo('--> Pesquinsado {}'.format(os_path_bst))
    count = countFileSystem(os_path_bst)
    setInfo('--> Total de arquivos pesquisado: {}'.format(count))
    if count == 0:
        setInfo('--> Nao existe arquivo')
        sys.exit(-1)

        setInfo('--> {}'.format(os_path_bst))
    if (count < file_count):
        setInfo('--> Quantidade de .BST ({}) menor que a esperada ({})'.format(count,file_count))
        sys.exit(-1)
    else:
        setInfo('--> Total BST: {} Esperado:{}'.format(count, file_count))

    setInfo('--> Inciando checkTotalFile')
    count_csv = countFileSystem(os_path_csv)
    count_bst = countFileSystem(os_path_bst)
    setInfo('--> PATHs: {} e {}'.format(os_path_csv,os_path_bst))
    if (file_count <> count_csv or file_count <> count_bst):
        setError('ERRO: Quantidade de arquivos divergente do esperado: CSV:{} - BST:{} - Esperao:{}'.format(count_csv,count_bst, file_count))
    else:
        setInfo('--> Total BST: {} CSV:{}'.format(count_bst, count_csv))

#----------------------------------------------------------
#DDDL VALIDATE
#----------------------------------------------------------
def validaddl():
    try:
        setInfo('--> Inciando validaddl')
        setInfo('--> Path Schema Armazenado:{}'.format(hdfs_schema_model))

        file_schema = getFilesHDFS(hdfs_path_ctr)
        setInfo('--> Path Schema:{}'.format(file_schema[0]))

        readCsvProcesso = sc.textFile(file_schema[0])
        structProcessoList = getSchemaNew(readCsvProcesso)
        structProcessoRdd = sc.parallelize(structProcessoList)
        structProcesso = structProcessoRdd.map(lambda y: y.split(",")).map(list).collect()

        try:
            readDicionario = sc.textFile(hdfs_schema_model)
            if readDicionario.isEmpty():
                setError("ERRO: O arquivo esta vazio!!!")
            else:
                dicProcesso = readDicionario.map(lambda x: x.split(",")).map(list).collect()
            try:
                if structProcesso == dicProcesso:
                    setInfo('-->Arquivo esta com a estrutura correta')
                else:
                    setError("ERRO: O arquivo esta com a estrutura incorreta: {} x {}".format(structProcesso,dicProcesso))
            except ValueError as ve:
                setError(ve)
        except ValueError as ve:
            setError(ve)
    except ValueError as ve:
        setError(ve)

#----------------------------------------------------------
# START LOOP FILE
#----------------------------------------------------------
def ingestao():
    setInfo('--> Inciando ingestao')
    setInfo('--> Check database e tabela')
    hive = HiveContext(sc)

    cmdDb = "show tables in " + database
    setInfo('--> Pesquisa no Hive {}'.format(cmdDb))
    t = hive.sql(cmdDb)
    result = t.where(t.tableName == table_name.lower()).collect()
    if len(result) == 0:
        setError('ERRO: Tabela ou Db nao existe no Hive')
    else:
        setInfo('--> Check Hive OK')

    files = getFilesHDFS(hdfs_path_csv)
    for f in files:
        try:
            # ----------------------------------------------------------
            # LOAD SCHEMA WITH DAT_REF_CARGA
            # ----------------------------------------------------------
            setInfo('--> Criando schema')
            schema = loadSchema(hdfs_schema_model)
            structSchema = StructType(schema)

            setInfo ('--> Processando arquivo %s' % f)
            # ----------------------------------------------------------
            # LOAD FILE
            # ----------------------------------------------------------
            setInfo('--> lendo origem de {} '.format(f))
            #df = sqlContext.read.format('com.databricks.spark.csv').options(header='false',delimiter=';',quoteMode='NON_NUMERIC',quote='"').load(f,schema = structSchema)
            df = sqlContext.read.format('com.databricks.spark.csv').options(header=header, delimiter=demiliter,
                                                                            quoteMode=quoteMode, quote=quote).load(f,
                                                                                                                     schema=structSchema)

            # ----------------------------------------------------------
            # COUNT LINE AND NAME FILE
            # ----------------------------------------------------------
            global total_line
            global name_file
            global total_file
            global size_file

            size_file+=getSizeFileHDFS(f)
            name_file= name_file + f.replace(hdfs_path_source,'') + ' / '
            total_file+=1
            count_read = df.count()
            total_line+=count_read

            setInfo('--> Total de linhas lidas: {} '.format(count_read))

            if (count_read == 0 and file_count == 1 ):
                setError('ERRO: Arquivo {} vazio - Arquivo unico'.format(name_file))

            if (count_read == 0):
                setInfo('-->Nao existe linhas no arquivo {}'.format(name_file))
            else:

                setInfo('--> Schema DF')
                df.printSchema()

                #----------------------------------------------------------
                #SAVE FILE HDFS
                #----------------------------------------------------------
                setInfo('--> gravando parquet em: {}'.format(hdfs_path_parquet))
                df.write.mode('append').parquet(hdfs_path_parquet)

        except ValueError as ve:
            setError('ERRO:{}'.format(ve))

    # ----------------------------------------------------------
    # ADD PARTITION
    # ----------------------------------------------------------
    if (total_line >0):
        setInfo('--> Inciando addPartition')

        cmd_hive = "ALTER TABLE " + database.lower() + "." + table_name.lower() + " ADD PARTITION(DAT_REF_CARGA='" + dt + "') location '" + hdfs_path_parquet + "'"
        try:
            hive.sql(cmd_hive)
            setInfo('---> Particao criada')
        except:
            setError('ERRO: Falha ao criar particao {}'.format(cmd_hive))
    else:
        setInfo('--> Nao foi adicionado particao - Nao existe arquivos')

    # ----------------------------------------------------------
    # LOG
    # ----------------------------------------------------------
    setInfo('--> Inserindo  reports.carga_bdi')

    try:
        insert = hive.sql(
            "select '{}' as db, '{}' as tabela, {} as qtde_arquivos, {} as qtde_linhas, {} as tamanho, '{}' as origem, '{}' as dh_exec, '{}' as arq_carregados, '{}' as dat_ref_carga".format(
                database, table_name, total_file, total_line, size_file, os_path_source, datetime.today(), name_file, dt))
        insert.write.mode("append").saveAsTable("reports.carga_bdi")

        setInfo('---> Log inserido')
    except  ValueError as ve:
        setError(ve)

#----------------------------------------------------------
#DELETE FILES
#----------------------------------------------------------
def limpastage():
    setInfo('--> Iniciando limpastage')
    files = getAllFileFileSystem(os_path_all)
    setInfo('--> Inciando deleteFileSystem - {}'.format(os_path_all))
    for filename in files:
        try:
            os.remove(filename)
            setInfo('-->{} removido com sucesso'.format(filename))
        except:
            setError('ERRO: Nao foi possivel excluir o arquivo do file system')

    setInfo('--> Inciando deleteHDFS - {}'.format(hdfs_path_all))
    folderList = getFilesHDFS(hdfs_path_all)
    for filename in folderList:
        try:
            os.popen('hadoop fs -rm ' + filename)
            setInfo('--> {} removido com sucesso'.format(filename))
        except Exception as ex:
            setError('ERRO: Erro ao excluir arquivo do HDFS - {}'.format(ex))

#----------------------------------------------------------
#DROP PARTITION
#----------------------------------------------------------
def expurgo():
    setInfo('--> Inciando expurgo')
    setInfo('--> Retencao:{}'.format(retention))

    data = datetime.today() + timedelta(days=(int(retention)*-1))
    data = data.strftime('%Y/%m/%d').replace('/','-')
    setInfo('--> Removendo data menor que :{}'.format(data))

    hive = HiveContext(sc)

    setInfo('--> Removendo particao Hive')
    delete_part = 'alter table {}.{} drop partition(dat_ref_carga < "{}")'.format(database.lower(),table_name.lower(),data)
    setInfo ('--> Execute: {}'.format(delete_part))
    try:
        hive.sql(delete_part)
    except Exception as ex:
        setError('Falha ao remover particoes. {}'.format(ex))
    else:
        setInfo('--> Particoes removidas com sucesso.')

    setInfo('--> Removendo arquivos HDFS')
    files = getFilesHDFS(hdfs_path_save)
    path=hdfs_path_save+'odate='
    filesName = map(lambda x: x.replace(path,''), files)
    setInfo('--> files:{}'.format(filesName))
    filesData = map(lambda  x:str(datetime(int(str(date.today().year)[:2] + (x[4:])), int(x[2:-2]),int(x[:2])).date().strftime('%Y/%m/%d')).replace('/', '-'),filesName)
    deleteOdate = map(lambda x: x[8:]+x[5:-3]+x[2:-6], filter(lambda x: x < data ,filesData))

    if (len(deleteOdate) <= 0):
        setInfo('--> Nao existe arquivos para excluir')
    else:
        setInfo('--> Inciando deleteHDFS')
        for file in deleteOdate:
            setInfo('--> Exluindo arquivo {}'.format(file))
            try:
                path_del = path+file
                exec_del = ['hdfs', 'dfs', '-rm', '-R', path_del]
                subprocess.check_output(exec_del)
                setInfo('--> {} removido com sucesso'.format(path_del))
            except Exception as ex:
                setError('ERRO: Erro ao excluir arquivo do HDFS - {}'.format(ex))


#----------------------------------------------------------
#PUT HDFS
#----------------------------------------------------------
def puthdfs():
    setInfo('--> Inciando putHdfs')
    # Montando comandos a executar no HDFS

    so_path_source_table_name = os_path_source + table_name +'*' + odate +'.*'

    setInfo('--> Arquivos pesquisados {}'.format(so_path_source_table_name))

    cmdHdfsTestOdate = "hdfs dfs -test -d " + hdfs_path_source
    cmdHdfsMkdirOdate = ['hdfs', 'dfs', '-mkdir', '-p', hdfs_path_source ]
    cmdHdfsDelOdate = ['hdfs', 'dfs', '-rm', '-R', hdfs_path_source]

    cmdLinuxTest = "for i in " + so_path_source_table_name + ' ; do test -f "$i" ; done'

    if system(cmdHdfsTestOdate) <> 0:
        subprocess.check_output(cmdHdfsMkdirOdate)

    listaDeArquivos = glob.glob(so_path_source_table_name)
    lista = []
    for i in range(0, len(listaDeArquivos)):
        temp = listaDeArquivos[i].split("/")
        lista.append(temp[-1])

    for i in range(0, len(listaDeArquivos)):
        lista[i] = hdfs_path_source + lista[i]

    for i in range(0, len(listaDeArquivos)):
        setInfo('--> Movendo arquivo {} {}'.format(i,str(listaDeArquivos[i])))
        cmdHdfsPut = ['hdfs', 'dfs', '-put', str(listaDeArquivos[i]), hdfs_path_source]
        try:
            subprocess.check_output(cmdHdfsPut)
        except subprocess.CalledProcessError as e:
            try:
                subprocess.check_output(cmdHdfsDelOdate)
            except subprocess.CalledProcessError as f:
                setError("ERROR: Usuario sem permissao para deletar pasta com odate: " + hdfs_path_save + " ou pasta inexistente")
            if e.returncode == 1:
                if system(cmdLinuxTest) != 0:
                    setError("ERROR: Arquivo(s) nao encontrado(s) na pasta: " + os_path_source + table_name)
                cmdHdfsTest = "hdfs dfs -test -f " + lista[i]
                setInfo('-->{}'.format(cmdHdfsTest))
                if system(cmdHdfsTest) == 0:
                    setError("ERROR: Arquivo(s) ja existe(em) na pasta: " + hdfs_path_save)
            else:
                setError("ERROR: Erro inesperado.")

#----------------------------------------------------------
#INTERNAL FUNCTIONS
#----------------------------------------------------------

def loadSchema(path):
    try:
        coluns = []
        file = sc.textFile(path)
        lines = file.collect()
        for i in range(0, len(lines)):
            col = lines[i].split(",")
            stype = ''
            #print(lines[i])
            if str(col[1]).startswith('DECIMAL'):
                stype=col[1]+','+col[2].replace(')','')+')'
            else:
                stype = col[1]
            coluns.append(StructField(col[0], deParaType(stype), True))
        return coluns
    except  ValueError as ve:
        setError('ERRO: Erro ao montar schema{}'.format(ve))

def deParaType(sType):
    setInfo(sType)
    if sType == "DATE":
        return StringType()#DateType()
    elif sType == "SMALLINT":
        return IntegerType()
    elif sType == "TIME":
        return StringType()
    elif sType == "INTEGER":
        return IntegerType()
    else:
        i = len(sType)
        i = sType.index("(")
        name = sType[:i]
        precison = sType[i + 1:-1].split(",")
        if name == "CHARACTER":
            return StringType()
        elif name == "DECIMAL":
            return DecimalType(int(precison[0]), int(precison[1]))
        elif name == "NUMERIC":
            return IntegerType()
        elif name == "TIMESTAMP":
            return  TimestampType()#TimestampType()
        else:
            return StringType()

def countFileSystem(path):
    try:
        return len(glob.glob(path))
    except:
        setError('ERROR: Erro ao consultar arquivos no file system:')

def getFilesHDFS(path):
    cmd = ['hdfs', 'dfs', '-ls','-C', path]
    files = subprocess.check_output(cmd).strip().split('\n')
    return files

def getAllFileFileSystem(path):
    cmd = ['ls', path]
    files = glob.glob(path)
    return files

def getSizeFileHDFS(file):
    cmd = ['hdfs', 'dfs', '-du', file]
    files = subprocess.check_output(cmd).strip().split(' ')
    return int(files[0])

def getSchemaNew(file):
		columns = []
		lines = file.collect()
		str1 = ''.join(lines)
		firstMatch = re.search('CREATE TABLE(.*)', str1).group(1)
		secMatch = re.search(r"\(\s(.*)\)", firstMatch).group(1)
		finalMatch = re.sub(",\s*(?![^()]*\))",';', secMatch)
		finalMatchTratado = finalMatch.replace(" , ", ",")
		splitMatchDotComma = finalMatchTratado.split(";")
		for i in range(0, len(splitMatchDotComma)):
			splitMatch = splitMatchDotComma[i].split(" ")
			columns.append(splitMatch[0]+","+splitMatch[1])
		return columns

def functions(argument):
    switcher = {
        'ingestao' : ingestao,
        'validaddl' : validaddl,
        'waitfile' : waitfile,
        'limpastage' : limpastage,
        'expurgo' : expurgo,
        'puthdfs' : puthdfs
    }
    # Get the function from switcher dictionary
    func = switcher.get(argument, lambda: "nothing")


    return func()

setInfo('--> Funcao chamada: {}'.format(func))
functions(func)

setInfo('--> Executado com sucesso')
sys.exit(0)

#----------------------------------------------------------
#EXECUTIONS
#----------------------------------------------------------
#spark-submit bdi_ingestion.py waitfile BJDTCDR 190616
#spark-submit bdi_ingestion.py puthdfs BJDTCDR 190616
#spark-submit bdi_ingestion.py validaddl BJDTCDR 190616
#spark-submit bdi_ingestion.py ingestao BJDTCDR 190616
#spark-submit bdi_ingestion.py limpastage BJDTCDR 190616
#spark-submit bdi_ingestion.py expurgo BJDTCDR 190616

#----------------------------------------------------------
#END
#----------------------------------------------------------

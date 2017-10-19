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
#----------------------------------------------------------
#CONSTANTS
#----------------------------------------------------------
def setError(msg):
    print(msg)
    sys.exit(1)

if len(argv[1:]) < 3:
    setError('Esta faltando parametro (funcao<waitFile> tabela<PEDT001> odate<120617>), total informado: ', len(argv[0:]))
else:

    #SET ARGs
    func = argv[1]#'Funcao'
    table_name = argv[2] #'tabela AEDTGEN'
    odate = argv[3] #'odate  120617'
    print("--> Inicio do processo Funcao:{} Tabela:{} odate:{} em {}".format(func,table_name,odate,datetime.now()))

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

    retention = retention[0]
    hdfs_path_source = hdfs_path_source[0]
    hdfs_path_save = hdfs_path_save[0]
    database = database[0]
    file_count = int(file_count[0])
    os_path_source= os_path_source[0]

    #print(type(retention))
    #validation =''
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

    print('--> retencao: {}'.format(retention))
    print('--> path_source: {}'.format(hdfs_path_source))
    print('--> path_save: {}'.format(hdfs_path_save))
    print('--> database: {}'.format(database))
    print('--> file_count: {}'.format(file_count))
    print('--> os_path_source: {}'.format(os_path_source))

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
    print('--> Inciando waitfile')
    print('--> Pesquinsado {}'.format(os_path_bst))
    count = countFileSystem(os_path_bst)
    print('--> Total de arquivos pesquisado: {}'.format(count))
    if count == 0:
        print('--> Nao existe arquivo')
        sys.exit(-1)

    print('--> {}'.format(os_path_bst))
    if (count < file_count):
        print('--> Quantidade de .BST ({}) menor que a esperada ({})'.format(count,file_count))
        sys.exit(-1)
    else:
        print('--> Total BST: {} Esperado:{}'.format(count, file_count))

    print('--> Inciando checkTotalFile')
    count_csv = countFileSystem(os_path_csv)
    count_bst = countFileSystem(os_path_bst)
    print('PATHs: {} e {}'.format(os_path_csv,os_path_bst))
    if (file_count <> count_csv or file_count <> count_bst):
        setError('ERRO: Quantidade de arquivos divergente do esperado: CSV:{} - BST:{} - Esperao:{}'.format(count_csv,count_bst, file_count))
    else:
        print('--> Total BST: {} CSV:{}'.format(count_bst, count_csv))

#----------------------------------------------------------
#DDDL VALIDATE
#----------------------------------------------------------
def validaddl():
    try:
        print('--> Inciando validaddl')
        print('--> Path Schema Armazenado:{}'.format(hdfs_schema_model))

        file_schema = getFilesHDFS(hdfs_path_ctr)
        print('--> Path Schema:{}'.format(file_schema[0]))

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
                    print('-->Arquivo esta com a estrutura correta')
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
    print('--> Inciando ingestao')
    print('--> Check database e tabela')
    hive = HiveContext(sc)

    cmdDb = "show tables in " + database
    print('--> Pesquisa no Hive {}'.format(cmdDb))
    t = hive.sql(cmdDb)
    result = t.where(t.tableName == table_name.lower()).collect()
    if len(result) == 0:
        setError('ERRO: Tabela ou Db nao existe no Hive')
    else:
        print('--> Check Hive OK')

    files = getFilesHDFS(hdfs_path_csv)
    for f in files:
        try:

            # ----------------------------------------------------------
            # LOAD SCHEMA WITH DAT_REF_CARGA
            # ----------------------------------------------------------
            print('--> Criando schema')
            schema = loadSchema(hdfs_schema_model)
            #print('--> {}'.format(schema))
            structSchema = StructType(schema)

            print ('--> Processando arquivo %s' % f)
            # ----------------------------------------------------------
            # LOAD FILE
            # ----------------------------------------------------------
            print('--> lendo origem de {} '.format(f))
            df = sqlContext.read.format('com.databricks.spark.csv').options(header='false',delimiter=';',quoteMode='NON_NUMERIC',quote='"').load(f,schema = structSchema)


            # ----------------------------------------------------------
            # ADD DAT_REF_CARGA
            # ----------------------------------------------------------
            #print('--> DAT_REF_CARGA {}'.format(dt))
            #newRdd = rddCsv.map(lambda x: x + ';"'+dt+'"')

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

            print('--> Total de linhas lidas: {} '.format(count_read))

            if (count_read == 0 and file_count == 1 ):
                setError('ERRO: Arquivo {} vazio - Arquivo unico'.format(name_file))

            if (count_read == 0):
                print('Nao existe linhas no arquivo {}'.format(name_file))
            else:

                print('--> Schema DF')
                df.printSchema()

                #----------------------------------------------------------
                #SAVE FILE HDFS
                #----------------------------------------------------------
                print('--> gravando parquet em: {}'.format(hdfs_path_parquet))
                df.write.mode('append').parquet(hdfs_path_parquet)

        except ValueError as ve:
            setError('ERRO:{}'.format(ve))

    # ----------------------------------------------------------
    # ADD PARTITION
    # ----------------------------------------------------------
    if (total_line >0):
        print('--> Inciando addPartition')

        cmd_hive = "ALTER TABLE " + database.lower() + "." + table_name.lower() + " ADD PARTITION(DAT_REF_CARGA='" + dt + "') location '" + hdfs_path_parquet + "'"
        try:
            hive.sql(cmd_hive)
            print('---> Particao criada')
        except:
            setError('ERRO: Falha ao criar particao {}'.format(cmd_hive))
    else:
        print('--> Nao foi adicionado particao - Nao existe arquivos')

    # ----------------------------------------------------------
    # LOG
    # ----------------------------------------------------------
    print('--> Inserindo  reports.carga_bdi')

    try:
        insert = hive.sql(
            "select '{}' as db, '{}' as tabela, {} as qtde_arquivos, {} as qtde_linhas, {} as tamanho, '{}' as origem, '{}' as dh_exec, '{}' as arq_carregados, '{}' as dat_ref_carga".format(
                database, table_name, total_file, total_line, size_file, os_path_source, datetime.today(), name_file, dt))
        insert.write.mode("append").saveAsTable("reports.carga_bdi")

        print('---> Log inserido')
    except  ValueError as ve:
        setError(ve)

#----------------------------------------------------------
#DELETE FILES
#----------------------------------------------------------
def limpastage():
    files = getAllFileFileSystem(os_path_all)
    print('--> Inciando deleteFileSystem - {}'.format(os_path_all))
    for filename in files:
        try:
            os.remove(filename)
            print('-->{} removido com sucesso'.format(filename))
        except:
            setError('ERRO: Nao foi possivel excluir o arquivo do file system')

    print('--> Inciando deleteHDFS - {}'.format(hdfs_path_all))
    folderList = getFilesHDFS(hdfs_path_all)
    for filename in folderList:
        try:
            os.popen('hadoop fs -rm ' + filename)
            print('--> {} removido com sucesso'.format(filename))
        except Exception as ex:
            setError('ERRO: Erro ao excluir arquivo do HDFS - {}'.format(ex))

#----------------------------------------------------------
#DROP PARTITION
#----------------------------------------------------------
def expurgo():
    print('--> Inciando expurgo')
    print('--> Retencao:{}'.format(retention))

    data = datetime.today() + timedelta(days=(int(retention)*-1))
    data = data.strftime('%Y/%m/%d').replace('/','-')
    print('--> Removendo data menor que :{}'.format(data))

    hive = HiveContext(sc)

    print('--> Removendo particao Hive')
    delete_part = 'alter table {}.{} drop partition(dat_ref_carga < "{}")'.format(database.lower(),table_name.lower(),data)
    print ('--> Execute: {}'.format(delete_part))
    try:
        hive.sql(delete_part)
    except Exception as ex:
        setError('Falha ao remover particoes. {}'.format(ex))
    else:
        print('--> Particoes removidas com sucesso.')

    print('--> Removendo arquivos HDFS')
    files = getFilesHDFS(hdfs_path_save)
    path=hdfs_path_save+'odate='
    filesName = map(lambda x: x.replace(path,''), files)
    print('--> files:{}'.format(filesName))
    filesData = map(lambda  x:str(datetime(int(str(date.today().year)[:2] + (x[4:])), int(x[2:-2]),int(x[:2])).date().strftime('%Y/%m/%d')).replace('/', '-'),filesName)
    deleteOdate = map(lambda x: x[8:]+x[5:-3]+x[2:-6], filter(lambda x: x < data ,filesData))

    if (len(deleteOdate) <= 0):
        print('--> Nao existe arquivos para excluir')
    else:
        print('--> Inciando deleteHDFS')
        for file in deleteOdate:
            print('--> Exluindo arquivo {}'.format(file))
            try:
                path_del = path+file
                exec_del = ['hdfs', 'dfs', '-rm', '-R', path_del]
                subprocess.check_output(exec_del)
                print('--> {} removido com sucesso'.format(path_del))
            except Exception as ex:
                setError('ERRO: Erro ao excluir arquivo do HDFS - {}'.format(ex))


#----------------------------------------------------------
#PUT HDFS
#----------------------------------------------------------
def puthdfs():

    # Montando comandos a executar no HDFS

    so_path_source_table_name = os_path_source + table_name +'*' + odate +'.*'

    print('--> Arquivos pesquisados {}'.format(so_path_source_table_name))

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
        print('--> Movendo arquivo {} {}'.format(i,str(listaDeArquivos[i])))
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
                print('-->{}'.format(cmdHdfsTest))
                if system(cmdHdfsTest) == 0:
                    setError("ERROR: Arquivo(s) ja existe(em) na pasta: " + hdfs_path_save)
            else:
                setError("ERROR: Erro inesperado.")

#----------------------------------------------------------
#INTERNAL FUNCTIONS
#----------------------------------------------------------

def loadSchemaWithDRC(path):
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
        coluns.append(StructField("DAT_REF_CARGA", StringType(), True))
        return coluns
    except  ValueError as ve:
        setError('ERRO: Erro ao montar schema{}'.format(ve))

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
    print(sType)
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

def countFile(path):
    try:
        cmd = ['hdfs', 'dfs', '-ls', path]
        files = subprocess.check_output(cmd).strip().split('\n')
        return len(files)
    except:
        setError('ERROR: Erro ao consultar arquivos no HDFS:')

def countFileSystem(path):
    try:
        return len(glob.glob(path))
    except:
        setError('ERROR: Erro ao consultar arquivos no file system:')

def getFilesHDFS(path):
    cmd = ['hdfs', 'dfs', '-ls','-C', path]
    files = subprocess.check_output(cmd).strip().split('\n')
    return files

def getFileFileSystem(path):
    cmd = ['ls', path]
    files = glob.glob(path)
    return files[0]

def getAllFileFileSystem(path):
    cmd = ['ls', path]
    files = glob.glob(path)
    return files

def clearLine(l):
    l = list(l)
    i = 0
    while i < len(l) - 1:
        if len(l[i].split(";")) < 5:
            l[i] = l[i] + l[i + 1]
            del l[i + 1]
        i = i + 1
    return l

def splitWithCast(l, schema):
    cols = l.split(";")
    ls = len(schema)
    ll = len(cols)
    #while ll > ls:
    #    cols[4] = cols[4] + ";" + cols[5]
    #    del cols[5]
    #    ll = len(cols)
    rows = []
    for i in range(0, len(cols)):
        try:
            val = cols[i]##[1:len(cols[i]) - 1]
            if schema[i].dataType.typeName() == StringType().typeName():
                if val.startswith('"') and val.endswith('"'):
                    rows.append(val[1:len(val) - 1])
                else:
                    rows.append(val)
            elif schema[i].dataType.typeName() == IntegerType().typeName():
                rows.append(int(val))
            elif schema[i].dataType.typeName() == DecimalType().typeName():
                rows.append(Decimal(cols[i].replace(",", ".")))
            elif schema[i].dataType.typeName() == DateType().typeName():
                dt = val.replace("-", "")
                rows.append(date(int(dt[:4]), int(dt[4:6]), int(dt[6:8])))
            elif schema[i].dataType.typeName() == TimestampType().typeName():
                dt = val.replace("-", "").replace(".", "").replace('"','')
                rows.append(
                    datetime(int(dt[:4]), int(dt[4:6]), int(dt[6:8]), int(dt[8:10]), int(dt[10:12]), int(dt[12:14]),int(dt[14:])).now())
            else:
                rows.append(val)
        except  ValueError as ve:
            setError('ERRO: Registro: {} -  splitWithCast: {}'.format(cols[i],ve))
    #print(rows)
    return rows

def getSchema(file):
		lines = file.collect()
		columns = []
		for i in range(2,len(lines)-2):
			lineText = lines[i].replace("    ( ", "")
			lineText = lineText.replace("    , ", "")
			lineText = lineText.replace(" , ", ",")
			col = lineText.split(" ")
			columns.append(col[0]+","+col[1])
		return columns

def queryFoldersHDFS(pathRoot):
    result = os.popen('hdfs dfs -ls -R ' + pathRoot).read()
    return result.splitlines()

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

print('--> Funcao chamada: {}'.format(func))
functions(func)

print('--> Executado com sucesso')
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

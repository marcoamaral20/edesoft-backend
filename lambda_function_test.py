import os
import boto3
import pandas as pd
import re
import pymysql
import random
from datetime import datetime

def get_file(event):
    bucket_name = os.environ.get('bucket_name')
    object_key = os.environ.get('object_key')
    s3 = boto3.client('s3')
    
    object = s3.get_object(Bucket=bucket_name, Key=object_key)
    data = object['Body'].read().decode("ISO-8859-1").splitlines()
    return data
    
def extracted_data(event):
    list_row = []
    data_csv = get_file(event)
    for row in data_csv:
        row = str(row).replace("b'", '')
        row = row.split(';')
        list_row.append(row)
        
    columns = list_row[0]
    list_row.pop(0)
        
    def remove_digits(column):
        for index,content in enumerate(column):
            column[index] = re.sub(r"[^a-zA-Z0-9 ]", "", content)

    def format_data(column):
        for index,content in enumerate(column):
            column[index] = datetime.strptime(content, "%d/%m/%Y").strftime('%Y-%m-%d')

    df = pd.DataFrame(list_row, columns=columns)
    
    remove_digits(df['Doc Originador'])
    remove_digits(df['CPF/CNPJ'])
    format_data(df['Data de Emissão'])
    format_data(df['Data de Vencimento'])
    format_data(df['Data de Compra CCB'])
    
    return df
    
def connection_mysql():
    rds_endpoint  = os.environ.get('rds-endpoint')
    username = os.environ.get('user')
    password = os.environ.get('password')
    db_name = os.environ.get('database-1')
    conn = None
    try:
        conn = pymysql.connect(host=rds_endpoint, user=username, passwd=password, db=db_name)
    except pymysql.MySQLError as e:
        print("ERROR: Could not connection in database")
        
    return conn
def lambda_handler(event, context):
    connection_db = connection_mysql()
    data = extracted_data(event)

    try:
        cur = connection_db.cursor()
        sql = """CREATE TABLE CESSAO_FUNDO (
          ID_CESSAO INT(22) NOT NULL AUTO_INCREMENT,
          ORIGINADOR VARCHAR(250) NOT NULL,
          DOC_ORIGINADOR INT(50) NOT NULL,
          CEDENTE VARCHAR(250) NOT NULL,
          DOC_CEDENTE INT(50) NOT NULL,
          CCB INT(22) NOT NULL,
          ID_EXTERNO INT(22) NOT NULL,
          CLIENTE VARCHAR(250) NOT NULL,
          CPF_CNPJ VARCHAR(50) NOT NULL,
          ENDERECO VARCHAR(250) NOT NULL,
          CEP VARCHAR(50) NOT NULL,
          CIDADE VARCHAR(250) NOT NULL,
          UF VARCHAR(50) NOT NULL,
          VALOR_DO_EMPRESTIMO DECIMAL(10,2) NOT NULL,
          VALOR_PARCELA DECIMAL(10,2) NOT NULL,
          TOTAL_PARCELAS INT(22) NOT NULL,
          PARCELA INT(22) NOT NULL,
          DATA_DE_EMISSAO DATE NOT NULL,
          DATA_DE_VENCIMENTO DATE NOT NULL,
          PRECO_DE_AQUISICAO DECIMAL(10,2) NOT NULL,
          PRIMARY KEY (ID_CESSAO)) """
        cur.execute(sql)
        connection_db.commit()
    except:
        print("ERROR: Don't create table in database")
        pass

        try:
            sql = """'insert into CESSAO_FUNDO (
                                                  ID_CESSAO,
                                                  ORIGINADOR,
                                                  DOC_ORIGINADOR,
                                                  CEDENTE,
                                                  DOC_CEDENTE,
                                                  CCB INT(22),
                                                  ID_EXTERNO,
                                                  CLIENTE,
                                                  CPF_CNPJ,
                                                  ENDERECO,
                                                  CEP,
                                                  CIDADE,
                                                  UF,
                                                  VALOR_DO_EMPRESTIMO,
                                                  VALOR_PARCELA,
                                                  TOTAL_PARCELAS,
                                                  PARCELA,
                                                  DATA_DE_EMISSAO,
                                                  DATA_DE_VENCIMENTO,
                                                  PRECO_DE_AQUISICAO,) """
            for index in range(len(data.index)):                             
                random_number = random.randint(0,9)
                sql += f"values ({random_number}, {data['Originador'][0]}), {data['Doc Originador'][index]}, {data['Cedente'][index]}, {data['Doc Cedente'][index]}, {data['CCB'][index]}, {data['Id'][index]}, {data['Cliente'][index]}, {data['CPF/CNPJ'][index]}, {data['Endereço'][index]},{data['CEP'][index]}, {data['Cidade'][index]}, {data['UF'][index]},{data['Valor do Empréstimo'][index]}, {data['Parcela R$'][index]}, {data['Total Parcelas'][index]}, {data['Parcela #'][index]}, {data['Data de Emissão'][index]},{data['Data de Vencimento'][index]}, {data['Preço de Aquisição'][index]})"
                cur.execute(sql)
                connection_db.commit()
                
            return "Sucess"    
        except:
            print("ERROR: Don't insert values in database")
            pass
        
    if connection_db:
        connection_db.commit()

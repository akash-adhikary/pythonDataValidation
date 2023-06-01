import pyodbc
import snowflake.connector
import csv
import json

class connection:
    def __init__(self,configFileNm):
        configFileName=configFileNm
        configFile = open(configFileName)
        config=json.load(configFile)
        nzCon = pyodbc.connect("DRIVER={NetezzaSQL}"+";SERVER={SERVER}; PORT={PORT};DATABASE={DATABASE};UID={UID};PWD={PWD};".format(SERVER=config['nzCreds']['SERVER'],PORT=config['nzCreds']['PORT'],DATABASE=config['nzCreds']['DATABASE'],UID=config['nzCreds']['UID'],PWD=config['nzCreds']['PWD']))
        sfCon = snowflake.connector.connect(user=config['sfCreds']['user'],password=config['sfCreds']['password'],account=config['sfCreds']['account']
        ,authenticator=config['sfCreds']['authenticator'],region=config['sfCreds']['region'],warehouse=config['sfCreds']['warehouse'])

        # Define Cursor
        self.nzCur = nzCon.cursor()
        self.sfCur = sfCon.cursor()
    # cur = con.cursor()
    def queryResult(self,db,query):
        if db=='nz':
            opt = self.nzCur.execute(query).fetchall()[0][0]
        elif db=='sf':
            opt = self.sfCur.execute(query).fetchall()[0][0]
        return opt
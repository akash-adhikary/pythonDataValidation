import pyodbc
import snowflake.connector
import csv
import json
from connections import connection as con
connect=con('config.json')
def runCountValidations(layer):
    rows=[]
    with open('meta.csv', mode ='r')as file:
        metaFile = csv.DictReader(file)
        for lines in metaFile:
            if(layer=='ALL'):
                c1=getCounts(lines)
                c2=getCountsWithFilter(lines)
                rows.append([lines["tableName"],c1[0],c1[1],c1[2],lines["nzFilter"],c2[0],lines["sfFilter"],c2[1],c2[2]])
            elif(lines["tableLayer"]==layer):
                c1=getCounts(lines)
                c2=getCountsWithFilter(lines)
                rows.append([lines["tableName"],c1[0],c1[1],c1[2],lines["nzFilter"],c2[0],lines["sfFilter"],c2[1],c2[2]])
            
            
    fields = ['Table Name', 'nz Count', 'sf Count','Diff1', 'nz Filter', 'nz Count with Filter', 'sf FIlter','sf Count with Filter',"Diff2"]
    filename = "countValResult.csv"
    with open(filename, 'w',newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(fields)
        csvwriter.writerows(rows)
    print(rows)
    
def getCounts(metaTuple):
    nzQuery="select count(1) from {}".format(metaTuple["nzObject"])
    sfQuery="select count(1) from {}".format(metaTuple["sfObject"])
    # print(nzQuery,sfQuery)
    nzCount=connect.queryResult('nz',nzQuery)
    sfCount=connect.queryResult('sf',sfQuery)
    diff=nzCount-sfCount
    print(nzCount,sfCount,diff)
    return([nzCount,sfCount,diff])

def getCountsWithFilter(metaTuple):
    nzQuery="select count(1) from {} {}".format(metaTuple["nzObject"],metaTuple["nzFilter"])
    sfQuery="select count(1) from {} {}".format(metaTuple["sfObject"],metaTuple["sfFilter"])
    # print(nzQuery,sfQuery)
    nzCount=connect.queryResult('nz',nzQuery)
    sfCount=connect.queryResult('sf',sfQuery)
    diff=nzCount-sfCount
    #print(nzCount,sfCount,diff)
    return([nzCount,sfCount,diff])

def runDupeValidations(layer):
    rows=[]
    with open('meta.csv', mode ='r')as file:
        metaFile = csv.DictReader(file)
        for lines in metaFile:
            if(layer=='ALL'):
                c1=getDupeCounts(lines)
                rows.append([lines["tableName"],c1[0],c1[1],c1[2]])
            elif(lines["tableLayer"]==layer):
                c1=getDupeCounts(lines)
                rows.append([lines["tableName"],c1[0],c1[1],c1[2]])
           
            
    fields = ['Table Name', 'Snowflake Count', 'Dupes Count','Diff']
    filename = "dupeValResult.csv"
    with open(filename, 'w',newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(fields)
        csvwriter.writerows(rows)
    print(rows)

def getDupeCounts(metaTuple):
    sfQuery="SELECT SUM(DUPE) FROM (SELECT {}_KEY, COUNT(1) AS CNT, (CNT-1) AS DUPE from  {} GROUP BY 1 HAVING CNT > 1)".format(metaTuple["sfObject"].replace("PRD_DATAMART.PRICING.","").replace("HUB_",""),metaTuple["sfObject"])
    sfCount=connect.queryResult('sf',sfQuery)
    sfQuery2="select count(1) from {}".format(metaTuple["sfObject"])
    sfCount2=connect.queryResult('sf',sfQuery2)
    return([sfCount2,sfCount])



def runCountDupeValidations(layer):
    rows=[]
    with open('meta.csv', mode ='r')as file:
        metaFile = csv.DictReader(file)
        for lines in metaFile:
            # print(lines)
            if(layer=='ALL'):
                print("")
                print(lines["tableName"], end =" ")
                c1=getCountsDupes(lines)
                print(c1, end =" ")
                c2=getCountsDupesWithFilter(lines)
                print(c2, end =" ")
                c4=getMaxAuditId(lines)
                print(c4, end =" ")
                # print(lines["tableName"],c1[0],c1[1],c1[2],c2[0],c2[1],c2[2],c1[3],c1[4],c2[3],c2[4],c4)
                rows.append([lines["tableName"],c1[0],c1[1],c1[2],lines["nzFilter"],c2[0],lines["sfFilter"],c2[1],c2[2],c1[3],c1[4],c2[3],c2[4],c4])
            elif(lines["tableLayer"]==layer):
                print("")
                print(lines["tableName"], end =" ")
                c1=getCountsDupes(lines)
                print(c1, end =" ")
                c2=getCountsDupesWithFilter(lines)
                print(c2, end =" ")
                c4=getMaxAuditId(lines)
                print(c4, end =" ")
                # print(lines["tableName"],c1[0],c1[1],c1[2],c2[0],c2[1],c2[2],c1[3],c1[4],c2[3],c2[4],c4)
                rows.append([lines["tableName"],c1[0],c1[1],c1[2],lines["nzFilter"],c2[0],lines["sfFilter"],c2[1],c2[2],c1[3],c1[4],c2[3],c2[4],c4])
            
    
    fields = ['Table Name', 'nz Count', 'sf Count','Diff1', 'nz Filter', 'nz Count with Filter', 'sf FIlter','sf Count with Filter',"Diff2","Dupes in NZ","Dupes in SF","Dupes in NZ with filter","Dupes in SF with","Max Audit ID"]
    filename = "countValResult"
    try:
        with open(filename+".csv", 'w',newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(fields)
            csvwriter.writerows(rows)
            print("file saved"+filename+".csv")
            
    except:
        file='not saved'
        counter=0
        while file != 'saved':
            try:
                counter+=1
                with open(filename+str(counter)+".csv", 'w',newline='') as csvfile:
                    csvwriter = csv.writer(csvfile)
                    csvwriter.writerow(fields)
                    csvwriter.writerows(rows)
                file='saved'
                print(filename+str(counter)+".csv")
            except:
                print("file already open trying to save a copy")

        

    print(rows)

    
def getCountsDupes(metaTuple):
    nzQuery="select count(1) from {}".format(metaTuple["nzObject"])
    nzDupeQuery="SELECT SUM(DUPE) FROM (SELECT {}, COUNT(1) AS CNT, (CNT-1) AS DUPE from  {} GROUP BY {} HAVING CNT > 1) A".format(metaTuple["nzObjectPk"],metaTuple["nzObject"],metaTuple["nzObjectPk"])
    sfQuery="select count(1) from {}".format(metaTuple["sfObject"])
    sfDupeQuery="SELECT SUM(DUPE) FROM (SELECT {}, COUNT(1) AS CNT, (CNT-1) AS DUPE from  {} GROUP BY {} HAVING CNT > 1)".format(metaTuple["sfObjectPk"],metaTuple["sfObject"],metaTuple["sfObjectPk"])
    try:

        nzCount=connect.queryResult('nz',nzQuery)
        sfCount=connect.queryResult('sf',sfQuery)
        diff=nzCount-sfCount
    except:
        nzCount='N/A'
        sfCount='N/A'
        diff='N/A'
        print("error:"+nzQuery)
        print("error:"+sfQuery)

    try:
        nzDupeCount=connect.queryResult('nz',nzDupeQuery)
        sfDupeCount=connect.queryResult('sf',sfDupeQuery)

    except:
        nzDupeCount='N/A'
        sfDupeCount='N/A'
        print("error:"+nzDupeQuery)
        print("error:"+sfDupeQuery)


    return([nzCount,sfCount,diff,nzDupeCount,sfDupeCount])
    
def getCountsDupesWithFilter(metaTuple):
    # print(metaTuple["tableName"])
    nzQuery="select count(1) from {} {}".format(metaTuple["nzObject"],metaTuple["nzFilter"])
    # print(nzQuery)
    nzDupeQuery="SELECT SUM(DUPE) FROM (SELECT {}, COUNT(1) AS CNT, (CNT-1) AS DUPE from  {} {} GROUP BY {} HAVING CNT > 1) A".format(metaTuple["nzObjectPk"],metaTuple["nzObject"],metaTuple["nzFilter"],metaTuple["nzObjectPk"])
    sfQuery="select count(1) from {} {}".format(metaTuple["sfObject"],metaTuple["sfFilter"])
    sfDupeQuery="SELECT SUM(DUPE) FROM (SELECT {}, COUNT(1) AS CNT, (CNT-1) AS DUPE from  {} {} GROUP BY {} HAVING CNT > 1)".format(metaTuple["sfObjectPk"],metaTuple["sfObject"],metaTuple["sfFilter"],metaTuple["sfObjectPk"])
    try:
        nzCount=connect.queryResult('nz',nzQuery)
    except:
        print("error:"+nzQuery)
        nzCount="N/A"
    try:
        sfCount=connect.queryResult('sf',sfQuery)
    except:
        print("error:"+sfQuery)
        sfCount="N/A"
    try:
        diff=nzCount-sfCount
    except:
        diff='N/A'

    # print(metaTuple["tableName"],nzCount,sfCount,diff)
    try:
        nzDupeCount=connect.queryResult('nz',nzDupeQuery)
        sfDupeCount=connect.queryResult('sf',sfDupeQuery)
    except:
        print("error:"+nzDupeQuery)
        print("error:"+sfDupeQuery)
        nzDupeCount='N/A'
        sfDupeCount='N/A'
    
    return([nzCount,sfCount,diff,nzDupeCount,sfDupeCount])


def getMaxAuditId(metaTuple):
    sfQuery="SELECT MAX(AUDIT_ID) FROM {}".format(metaTuple["sfObject"])
    try:
        maxAuditId=connect.queryResult('sf',sfQuery)
    except:
        print("error:"+sfQuery)
        maxAuditId='N/A'
    return(maxAuditId)
# runCountValidations("ALL")


# runDupeValidations("ALL")


runCountDupeValidations("ALL")
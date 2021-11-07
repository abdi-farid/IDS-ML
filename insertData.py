
import csv

# import pandas as pd
from pandas import read_json
from pandas import read_csv
import pyodbc

from os import system

zeekTableQuery = """
    DROP TABLE IF EXISTS zeek;

    CREATE TABLE [dbo].[zeek](
	[column1] [tinyint] NOT NULL,
	[srcip] [varchar](50) NOT NULL,
	[dstip] [varchar](50) NOT NULL,
	[sport] [int] NOT NULL,
	[dport] [int] NOT NULL,
	[proto] [varchar](50) NOT NULL,
	[user] [varchar](50) NULL,
	[password] [varchar](50) NULL,
	[command] [varchar](50) NULL,
	[method] [varchar](50) NULL,
	[service] [varchar](50) NOT NULL,
	[is_sm_ips_ports] [tinyint] NOT NULL,
	[ct_state_ttl] [tinyint] NOT NULL,
	[ct_flw_http_mthd] [tinyint] NOT NULL,
	[is_ftp_login] [tinyint] NOT NULL,
	[ct_ftp_cmd] [tinyint] NOT NULL,
	[ct_srv_src] [tinyint] NOT NULL,
	[ct_srv_dst] [tinyint] NOT NULL,
	[ct_dst_ltm] [tinyint] NOT NULL,
	[ct_src_ltm] [tinyint] NOT NULL,
	[ct_src_dport_ltm] [tinyint] NOT NULL,
	[ct_src_sport_ltm] [tinyint] NOT NULL,
	[ct_dst_src_ltm] [tinyint] NOT NULL,
	[trans_dep] [int]  NULL,
	[res_bdy_len] [int]  NULL
	)
	
"""
argusTablequery = """
    DROP TABLE IF EXISTS argus;

    CREATE TABLE argus(
	[srcip] [varchar](20) NULL,
	[sport] [int] NULL,
	[dstip] [varchar](20) NULL,
	[dport] [int] NULL,
	[proto] [varchar](10) NULL,
	[state] [varchar](10) NULL,
	[dur] [float] NULL,
	[sbytes] [int] NULL,
	[dbytes] [int] NULL,
	[sttl] [int] NULL,
	[dttl] [int] NULL,
	[sloss] [int] NULL,
	[dloss] [int] NULL,
	[sload] [float] NULL,
	[dload] [float] NULL,
	[spkts] [int] NULL,
	[dpkts] [int] NULL,
	[swin] [int] NULL,
	[dwin] [int] NULL,
	[stcpb] [bigint] NULL,
	[dtcpb] [bigint] NULL,
	[smeansz] [float] NULL,
	[dmeansz] [float] NULL,
	[sjit] [float] NULL,
	[djit] [float] NULL,
	[stime] [varchar](50) NULL,
	[ltime] [varchar](50) NULL,
	[sintpkt] [float] NULL,
	[dintpkt] [float] NULL,
	[tcprtt] [float] NULL,
	[synack] [float] NULL,
	[ackdat] [float] NULL
	)
	
"""

flowsTablequery = """
    DROP TABLE IF EXISTS dbo.flows;
	
    CREATE TABLE flows(
	[srcip] [varchar](50) NULL,
	[sport] [int] NULL,
	[dstip] [varchar](50) NULL,
	[dsport] [int] NULL,
	[proto] [varchar](50) NULL,
	[state] [varchar](50) NULL,
	[dur] [float] NULL,
	[sbytes] [int] NULL,
	[dbytes] [int] NULL,
	[sttl] [int] NULL,
	[dttl] [int] NULL,
	[sloss] [int] NULL,
	[dloss] [int] NULL,
	[service] [varchar](50) NULL,
	[sload] [float] NULL,
	[dload] [float] NULL,
	[spkts] [int] NULL,
	[dpkts] [int] NULL,
	[swin] [int] NULL,
	[dwin] [int] NULL,
	[stcpb] [bigint] NULL,
	[dtcpb] [bigint] NULL,
	[smeansz] [float] NULL,
	[dmeansz] [float] NULL,
	[trans_depth] [int] NULL,
	[res_bdy_len] [int] NULL,
	[sjit] [float] NULL,
	[djit] [float] NULL,
	[stime] [varchar](50) NULL,
	[ltime] [varchar](50) NULL,
	[sintpkt] [float] NULL,
	[dintpkt] [float] NULL,
	[tcprtt] [float] NULL,
	[synack] [float] NULL,
	[ackdat] [float] NULL,
	[method] [varchar](50) NULL,
	[user] [varchar](50) NULL,
	[password] [varchar](50) NULL,
	[command] [varchar](50) NULL,
	[is_sm_ips_ports] [tinyint] NULL,
	[ct_state_ttl] [int] NULL,
	[ct_flw_http_mthd] [int] NULL,
	[is_ftp_login] [tinyint] NULL,
	[ct_ftp_cmd] [tinyint] NULL,
	[ct_srv_src] [tinyint] NULL,
	[ct_srv_dst] [tinyint] NULL,
	[ct_dst_ltm] [tinyint] NULL,
	[ct_src_ltm] [tinyint] NULL,
	[ct_src_dport_ltm] [tinyint] NULL,
	[ct_dst_sport_ltm] [tinyint] NULL,
	[ct_dst_src_ltm] [tinyint] NULL
)
"""
insertArgusDataQuery = """insert into argus ([srcip],[sport],[dstip],[dport],[proto],[state],[dur],[sbytes],[dbytes],[sttl],[dttl],
											 [sloss],[dloss],[sload],[dload],[spkts],[dpkts],[swin],[dwin],[stcpb],[dtcpb],[smeansz],
											 [dmeansz],[sjit],[djit],[stime],[ltime],[sintpkt],[dintpkt],[tcprtt],[synack],[ackdat]) 
						  					 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
insertflowsDataQuery = """
		INSERT into flows 
	SELECT dbo.argus.srcip, dbo.argus.sport,dbo.argus.dstip, dbo.argus.dport, dbo.argus.proto, 
			dbo.argus.[state], dbo.argus.dur,dbo.argus.sbytes, dbo.argus.dbytes,dbo.argus.sttl, dbo.argus.dttl, 
			dbo.argus.sloss, dbo.argus.dloss,  dbo.zeek.service,dbo.argus.sload, dbo.argus.dload, dbo.argus.spkts, 
			dbo.argus.dpkts, dbo.argus.swin, dbo.argus.dwin, dbo.argus.stcpb, dbo.argus.dtcpb, 
			dbo.argus.smeansz, dbo.argus.dmeansz ,dbo.zeek.trans_dep, dbo.zeek.res_bdy_len,dbo.argus.sjit, dbo.argus.djit, dbo.argus.stime, 
			dbo.argus.ltime, dbo.argus.sintpkt, dbo.argus.dintpkt, dbo.argus.tcprtt, dbo.argus.synack, 
			dbo.argus.ackdat, dbo.zeek.method,
			dbo.zeek.[user], dbo.zeek.[password], dbo.zeek.command, dbo.zeek.is_sm_ips_ports, dbo.zeek.ct_state_ttl, 
			dbo.zeek.ct_flw_http_mthd, dbo.zeek.is_ftp_login, dbo.zeek.ct_ftp_cmd, dbo.zeek.ct_srv_src, 
			dbo.zeek.ct_srv_dst, dbo.zeek.ct_dst_ltm, dbo.zeek.ct_src_ltm, dbo.zeek.ct_src_dport_ltm, 
			dbo.zeek.ct_src_sport_ltm, dbo.zeek.ct_dst_src_ltm
		from dbo.argus
		inner join dbo.zeek on dbo.argus.srcip = dbo.zeek.srcip and 
				dbo.argus.dstip = dbo.zeek.dstip and 
				dbo.argus.sport = dbo.zeek.sport and 
				dbo.argus.dport = dbo.zeek.dport
"""

def generate_zeek_csv():
    logfile = read_json("custHttpLog.log", lines=True) 
    logfile.to_csv ("zeek.csv", index=[0], encoding="utf-8")
     
# creaate table 
def createTables(cnxn):
	try:
		cnxn.execute(zeekTableQuery)
		cnxn.execute(argusTablequery)
		cnxn.execute(flowsTablequery)
		cnxn.commit()
	except pyodbc.Error as e:
		print("Echec de la creation des tables ")
		cnxn.rollback()	

#insert data in database
def insertData(cnxn):
	cursor = cnxn.cursor()
	#execute command directly from terminal using os module
	try:
		system("bcp zeek in zeek.csv -S localhost -d networkData -U SA -P @123admin -c -q -t, -F 2 ")
		#read csv file
		df = read_csv("./argus.csv")
		columns=['srcip', 'sport','dstip','dport','proto', 'state','dur',  'sbytes', 'dbytes',
				'sttl', 'dttl','sloss', 'dloss','sload', 'dload', 'spkts', 'dpkts','swin','dwin', 'stcpb', 'dtcpb','smeansz', 'dmeansz', 'sjit', 'djit','stime','ltime','sintpkt', 'dintpkt',
				'tcprtt', 'synack', 'ackdat']
		
		#modify column names		 
		df.columns = columns
		#j'ai remplacer les valeurs nulles par -1 pour eviter les problemes d'insertion dans la BDD (problems de types)
		df.fillna(-1, inplace=True)
		#convertir le dataset en une liste utile pour la fonction fetchmany
		records = df.values.tolist()		  

		cursor.executemany(insertArgusDataQuery, records)		  
		# remplissage de la table flows
		cursor.execute(insertflowsDataQuery)
		cursor.commit()
		cursor.close()
	except OSError as e:
		print("erreur de l'execution de la commande")
		
	except pyodbc.Error as e:
		print("Erreur lors de l'insertion dans la base de donnees")
		
		cursor.rollback()		
		 
if __name__ == "__main__":
    #Database information        
	server = 'rayan-SVF15A13CDB' 
	database = 'networkdata' 
	username = 'sa' 
	password = '@123admin'

	# Connect to the database
	try:
		cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
	except pyodbc.DatabaseError as e:
		print("Database Error:")
	except pyodbc.Error as e:
		print("Connection Error")

	generate_zeek_csv()
	createTables(cnxn)
	insertData(cnxn)
	
	#close the connection
	cnxn.close()


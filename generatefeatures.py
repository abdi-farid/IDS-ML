from locale import currency
from os import read
from numpy.core.fromnumeric import cumprod
from pkg_resources import cleanup_resources
import pyodbc
from  pickle import dump
from pandas import read_sql_query
from pandas import DataFrame


global genetated_dataset
global srv_src_data
# Initiliser les colonnes du dataset
df_columns = ['srcip','dstip', 'dur', 'proto', 'service', 'state', 'spkts', 'dpkts', 
              'sbytes', 'dbytes', 'sttl', 'dttl', 'sload', 'dload', 
              'sloss', 'dloss', 'sintpkt', 'dintpkt', 'sjit', 'djit', 'swin', 
              'stcpb', 'dtcpb', 'dwin', 'tcprtt', 'synack', 'ackdat', 'smeansz', 
              'dmeansz', 'trans_depth', 'res_bdy_len', 'ct_srv_src', 'ct_state_ttl', 
              'ct_dst_ltm', 'ct_src_dport_ltm', 'ct_dst_sport_ltm', 'ct_dst_src_ltm', 
              'is_ftp_login', 'ct_ftp_cmd', 'ct_flw_http_mthd', 'ct_src_ltm', 'ct_srv_dst', 'is_sm_ips_ports']



#Pour recuperer les donnees
query = """
    SELECT srcip, dstip, sport, dsport, proto, 
           [state],sbytes, dbytes,sttl, dttl, 
           sloss, dloss, sload, dload, spkts, 
           dpkts, swin, dwin, stcpb, dtcpb, 
           smeansz, dmeansz, sjit, djit, stime, 
           ltime, sintpkt, dintpkt, tcprtt, synack, 
           ackdat,trans_depth, res_bdy_len, dur, method, 
           [user], [password], command
    from dbo.flows

"""
def add_basic_features(cnxn):
    #query for adding basic features to the dataset
    # dur, proto, service, state, spkts, dpkts, 
    #           sbytes, dbytes, sttl, dttl, sload, dload, 
    #           sloss, dloss, sintpkt, dintpkt, sjit, djit, swin, 
    #           stcpb, dtcpb, dwin, tcprtt, synack, ackdat, smeansz, 
    #           dmeansz, trans_depth, res_bdy_len
    query = """
    SELECT  *
    from dbo.flows
    """
    # Read sql query with pandas
    SQL_Query = read_sql_query(query, cnxn)

    #Generate the dataframe with pandas while reading the sql query
    generated_dataset = DataFrame(SQL_Query, columns=df_columns)
    generated_dataset.fillna(-1, inplace=True)

    return generated_dataset

def set_is_sm_ips_ports(data_records):
    is_sm_ips_ports = 0
    sm_port_data = list()
    for data_record in data_records:
        srcip = data_record.srcip
        dstip = data_record.dstip
        sport = data_record.sport
        dport = data_record.dsport
        if(srcip == dstip and sport == dport):
            is_sm_ips_ports = 1
        else:
            is_sm_ips_ports = 0
        sm_port_data.append(is_sm_ips_ports)

    generated_dataset["is_sm_ips_ports"] = sm_port_data
    

def set_ct_state_ttl(data_records):
    ct_state_ttl = 0
    ttl_data = list()

    for data_record in data_records:
        #Recuperer les attributs
        sttl = data_record.sttl
        dttl = data_record.dttl
        state = data_record.state
        if(sttl in [62,63,254,255] and dttl in [252, 253] and state == "FIN"):
            ct_state_ttl = 1
        elif(sttl in [0,62,254] and dttl == 0 and state == "INT"):
            ct_state_ttl = 2
        elif(sttl in [62,254] and dttl in [60,252,253] and state == "CON"):
            ct_state_ttl = 3
        elif(sttl == 254 and dttl == 252 and state == "ACC"):
            ct_state_ttl = 4
        elif(sttl == 254 and dttl == 252 and state == "CLO"):
            ct_state_ttl = 5    
        elif(sttl == 254 and dttl == 0 and state == "REQ"):
            ct_state_ttl = 6
        else:
            ct_state_ttl = 0 
                           
        ttl_data.append(ct_state_ttl)
    generated_dataset["ct_state_ttl"] = ttl_data

def set_ct_flw_http_mthd(cnxn):
    http_data = list()
    ct_flw_http_mthd = 0
    http_query = """select method from dbo.flows"""
    cursor = cnxn.cursor()
    cursor.execute(http_query)
    curr_method = cursor.fetchone()

    #fetchone retourne un tuple donc currmethod[0]
    while curr_method:
        next_method = cursor.fetchone()
        if ((curr_method == next_method) and curr_method[0] is not None):
            ct_flw_http_mthd +=1
            
        else:
            ct_flw_http_mthd = 0 
          
        curr_method = next_method 
            
        http_data.append(ct_flw_http_mthd)
    generated_dataset["ct_flw_http_mthd"] = http_data
    cursor.close()

def set_is_ftp_login(data_records):
    is_ftp_login = 0
    ftp_login_data = list()
    for data_record in data_records:
        service = data_record[13]
        user = data_record.user
        password = data_record.password
        if (service == "ftp" and user != "" and password != ""):
            is_ftp_login = 1
            
        else:
            is_ftp_login = 0 
            
        ftp_login_data.append(is_ftp_login)
    generated_dataset["is_ftp_login"] = ftp_login_data
    
def set_ct_ftp_cmd(cnxn):
    ct_ftp_cmd = 0
    ftp_cmd_data = list()
    
    query = """ select command from flows """
    cursor = cnxn.cursor()
    cursor.execute(query)
    curr_ftp_cmd = cursor.fetchone()
    while curr_ftp_cmd:
        next_ftp_cmd = cursor.fetchone()
        if((curr_ftp_cmd == next_ftp_cmd) and (curr_ftp_cmd != "" and next_ftp_cmd != "")):
            ct_ftp_cmd = 1
        else:
            ct_ftp_cmd = 0 

        curr_ftp_cmd = next_ftp_cmd       
        ftp_cmd_data.append(ct_ftp_cmd)
    generated_dataset["ct_ftp_cmd"] = ftp_cmd_data    
    cursor.close()
  
def set_ct_srv_src(cnxn):
    ct_srv_src = 0
    srv_src_data = list()
    query = """ select dur,srcip, service from flows"""
    cursor = cnxn.cursor()
    cursor.execute(query)
    # data_records = cursor.fetchall()
    record_chunk100 = list()
    record_chunk100 = cursor.fetchmany(100)
    while record_chunk100:
        # fetch 100 records from all the records
        
        for index, data_record in enumerate(record_chunk100):
            #compare current srv_src with the next srv_src
            if index < len(record_chunk100) - 1:
                if (data_record == record_chunk100[index + 1]):
                    ct_srv_src += 1
                elif (data_record != record_chunk100[index + 1]):
                    ct_srv_src = 1
                else:
                    ct_srv_src = 0
                srv_src_data.append(ct_srv_src)
        srv_src_data.append(srv_src_data[-1])
        record_chunk100 = cursor.fetchmany(100)
    generated_dataset["ct_srv_src"] = srv_src_data 
    cursor.close()   

def set_ct_srv_dst(cnxn):
    ct_srv_dst = 0
    srv_dst_data = list()
    
    query = """ select dstip, service from flows"""
    cursor = cnxn.cursor()
    cursor.execute(query)
    record_chunk100 = list()
    record_chunk100 = cursor.fetchmany(100)
    while record_chunk100:
        # fetch 100 records from all the records
        
        for index, data_record in enumerate(record_chunk100):
            #compare current srv_src with the next srv_src
            if index < len(record_chunk100) - 1:
                if (data_record == record_chunk100[index + 1]):
                    ct_srv_dst += 1
                elif (data_record != record_chunk100[index + 1]):
                    ct_srv_dst = 1
                else:
                    ct_srv_dst = 0
                srv_dst_data.append(ct_srv_dst)
        srv_dst_data.append(srv_dst_data[-1])
        record_chunk100 = cursor.fetchmany(100)
    generated_dataset["ct_srv_dst"] = srv_dst_data 
    cursor.close()

def set_ct_dst_ltm(cnxn):
    ct_dst_ltm = 0
    dst_ltm_data = list()
    
    query = """ select dstip, DATEPART(MILLISECOND,ltime) from flows"""
    cursor = cnxn.cursor()
    cursor.execute(query)
    record_chunk100 = list()
    record_chunk100 = cursor.fetchmany(100)
    while record_chunk100:
        # fetch 100 records from all the records
        
        for index, data_record in enumerate(record_chunk100):
            #compare current srv_src with the next srv_src
            if index < len(record_chunk100) - 1:
                if (data_record == record_chunk100[index + 1]):
                    ct_dst_ltm += 1
                elif (data_record != record_chunk100[index + 1]):
                    ct_dst_ltm = 1
                else:
                    ct_dst_ltm = 0
                dst_ltm_data.append(ct_dst_ltm)
        dst_ltm_data.append(dst_ltm_data[-1])
        record_chunk100 = cursor.fetchmany(100)
    generated_dataset["ct_dst_ltm"] = dst_ltm_data 
    cursor.close()


  
    print(len(dst_ltm_data))
    generated_dataset["ct_dst_ltm"] = dst_ltm_data 
            
def set_ct_src_ltm(cnxn):
    ct_src_ltm = 0
    src_ltm_data = list()
    
    query = """ select srcip, DATEPART(MILLISECOND,ltime) from flows"""
    cursor = cnxn.cursor()
    cursor.execute(query)

    record_chunk100 = list()
    record_chunk100 = cursor.fetchmany(100)
    while record_chunk100:
        # fetch 100 records from all the records
        
        for index, data_record in enumerate(record_chunk100):
            #compare current srv_src with the next srv_src
            if index < len(record_chunk100) - 1:
                if (data_record == record_chunk100[index + 1]):
                    ct_src_ltm += 1
                elif (data_record != record_chunk100[index + 1]):
                    ct_src_ltm = 1
                else:
                    ct_src_ltm = 0
                src_ltm_data.append(ct_src_ltm)
        src_ltm_data.append(src_ltm_data[-1])
        record_chunk100 = cursor.fetchmany(100)
    generated_dataset["ct_src_ltm"] = src_ltm_data
    cursor.close()

def set_ct_src_dport_ltm(cnxn):
    ct_src_dport_ltm = 0
    src_dport_ltm_data = list()

    query = """ select srcip ,dsport,DATEPART(MILLISECOND,ltime)  from flows"""
    cursor = cnxn.cursor()
    cursor.execute(query)
    record_chunk100 = list()
    record_chunk100 = cursor.fetchmany(100)
    while record_chunk100:
        # fetch 100 records from all the records
        
        for index, data_record in enumerate(record_chunk100):
            #compare current srv_src with the next srv_src
            if index < len(record_chunk100) - 1:
                if (data_record == record_chunk100[index + 1]):
                    ct_src_dport_ltm += 1
                elif (data_record != record_chunk100[index + 1]):
                    ct_src_dport_ltm = 1
                else:
                    ct_src_dport_ltm = 0
                src_dport_ltm_data.append(ct_src_dport_ltm)
        src_dport_ltm_data.append(src_dport_ltm_data[-1])
        record_chunk100 = cursor.fetchmany(100)
    
    generated_dataset["ct_src_dport_ltm"] = src_dport_ltm_data
    cursor.close()

def set_ct_dst_sport_ltm(cnxn):
    ct_dst_sport_ltm = 0
    dst_sport_ltm_data = list()

    # Pour recuperer l'@ ip et le port et les millisecondes de ltime
    query = """ select dstip ,sport,DATEPART(MILLISECOND,ltime)  from flows"""
    cursor = cnxn.cursor()
    cursor.execute(query)
    record_chunk100 = list()
    record_chunk100 = cursor.fetchmany(100)

    while record_chunk100:
        # fetch 100 records from all the records
        
        for index, data_record in enumerate(record_chunk100):
            #compare current srv_src with the next srv_src
            if index < len(record_chunk100) - 1:
                if (data_record == record_chunk100[index + 1]):
                    ct_dst_sport_ltm += 1
                elif (data_record != record_chunk100[index + 1]):
                    ct_dst_sport_ltm = 1
                else:
                    ct_dst_sport_ltm = 0
                dst_sport_ltm_data.append(ct_dst_sport_ltm)
        dst_sport_ltm_data.append(dst_sport_ltm_data[-1])
        record_chunk100 = cursor.fetchmany(100)

    #Ajouter la colonne dans le dataset
    generated_dataset["ct_dst_sport_ltm"] = dst_sport_ltm_data      
    cursor.close()  

def set_ct_dst_src_ltm(cnxn):
    ct_dst_src_ltm = 0
    dst_src_ltm_data = list()

    # Pour recuperer l'@ ip et le port et les millisecondes de ltime
    query = """ select srcip,dstip,DATEPART(MILLISECOND,ltime)  from flows"""
    cursor = cnxn.cursor()
    cursor.execute(query)
    record_chunk100 = list()
    record_chunk100 = cursor.fetchmany(100)
    while record_chunk100:
        # fetch 100 records from all the records
        
        for index, data_record in enumerate(record_chunk100):
            #compare current srv_src with the next srv_src
            if index < len(record_chunk100) - 1:
                if (data_record == record_chunk100[index + 1]):
                    ct_dst_src_ltm += 1
                elif (data_record != record_chunk100[index + 1]):
                    ct_dst_src_ltm = 1
                else:
                    ct_dst_src_ltm = 0
                dst_src_ltm_data.append(ct_dst_src_ltm)
        dst_src_ltm_data.append(dst_src_ltm_data[-1])
        record_chunk100 = cursor.fetchmany(100)

    #Ajouter la colonne dans le dataset
    generated_dataset["ct_dst_src_ltm"] = dst_src_ltm_data
    cursor.close()


if __name__ == "__main__":  
    # #Database information        
    server = 'localhost' 
    database = 'networkdata' 
    username = 'SA' 
    password = '@123admin'

    #Initialize the dataframe
    generated_dataset = DataFrame(columns=df_columns)
    # Connect to the database
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    
    cursor = cnxn.cursor()

    # recuperer les donnees de la BDD
    data_records = cursor.execute(query).fetchall()
    generated_dataset = add_basic_features(cnxn)
    set_is_sm_ips_ports(data_records)
    set_ct_state_ttl(data_records)
    set_ct_flw_http_mthd(cnxn)
    set_is_ftp_login(data_records)
    set_ct_ftp_cmd(cnxn)
    set_ct_srv_src(cnxn)
    set_ct_srv_dst(cnxn)
    set_ct_dst_ltm(cnxn)
    set_ct_src_ltm(cnxn)
    set_ct_dst_sport_ltm(cnxn)
    set_ct_src_dport_ltm(cnxn)
    set_ct_dst_src_ltm(cnxn)
   
    #Sauvgarder le dataset avec pickle  
    dump(generated_dataset, open("dataset3","wb"))

    cnxn.close()
    #cursor.close()

from re import sub
from pandas.core.reshape.merge import merge
from scipy.sparse import data
import streamlit as st
from pandas import merge
from pandas import DataFrame
from pandas import Series
from pandas import concat
from pandas import read_csv
import subprocess
import pickle

from streamlit.type_util import is_namedtuple
import preprocessing_page as proc
import time
rf = pickle.load(open("models/rfc_dt_17Sep", "rb"))

def analyser(df, model):
    #Recuperer les IPs du dataset genere
    IPs ={
    "id": Series(df.index) , 
    "srcip":df["srcip"],
    "dstip":df["dstip"]
    }
    IPs = concat(IPs,axis=1)
    #supprimer la colonne srcip pour adapter le dataset (42 features)
    df.drop(["srcip", "dstip"], axis=1, inplace=True)
    #predictionss
    pred = model.predict(df)
    #recuperer les IDs
    ipsAfterPred = DataFrame(df[pred == 1].index, columns=["id"])
    #Recuperer les IPs en effectuant un inner join et grouper par IP
    res = merge(IPs, ipsAfterPred, how='inner', on=["id"]).groupby(["srcip", "dstip"])
    #recuperer les IPs de l'objet group
    maliciousIPs = res.groups.keys()
    return DataFrame(maliciousIPs, columns=["Adresse IP source", "Adresse IP destination"])

def generate_dataset_from_pcap(pcap):
    file_name = pcap.name
  
    
    subprocess.run(f"argus -r {file_name} -w traffic.argus", shell=True)
    subprocess.run(f"sudo zeek -r {file_name} CustomHTTPlog.zeek", shell=True)
    subprocess.run("sudo ra -s saddr sport daddr dport proto state service dur sbytes dbytes sttl dttl sloss dloss service sload dload spkts dpkts swin dwin stcpb dtcpb smeansz dmeansz sjit djit stime ltime sintpkt dintpkt tcprtt synack ackdat -r traffic.argus -n -c ,> argus.csv", shell=True)
    subprocess.run("sudo python3 merge.py", shell=True)
    subprocess.run("sudo python3 insertData.py", shell=True)
    subprocess.run("sudo python3 generatefeatures.py", shell=True)   
   
    dataset = pickle.load(open("dataset3", "rb"))
    IPs ={
    
    "srcip":dataset["srcip"],
    "dstip":dataset["dstip"]
    }
    IPs = concat(IPs,axis=1)
    
    dataset.drop(["srcip", "dstip"], axis=1, inplace=True)
    dataset = proc.ordinal_encoding(dataset)
    dataset = proc.Trait_val_vides(dataset)
    dataset = proc.normalisation(dataset)
    dataset.insert(1,"srcip", IPs['srcip'])
    dataset.insert(2,"dstip", IPs['dstip'])
    return dataset, IPs
    
def generate_dataset_from_csv(csv_file):
    dataset = read_csv(csv_file) 
    IPs ={
   
    "srcip":dataset["srcip"]
    }
    # IPs = concat(IPs,axis=1)
    dataset = proc.ordinal_encoding(dataset)
    dataset = proc.Trait_val_vides(dataset)
    dataset = proc.normalisation(dataset)
    return dataset, IPs   

def show_analyser_page():
    st.write("""
        # Analyser un traffic reseau
    """)
    st.subheader("Quel type de fichier souhaitez vous importer")
    file_type = st.selectbox("Type de fichier", ["pcap", "csv"])
    if file_type == "pcap":
        pcap = st.file_uploader("Importer un fichier pcap")
        if pcap is not None:
            dataset, IPs = generate_dataset_from_pcap(pcap)
            
            Ips = analyser(dataset,rf)
            st.text("Resultats de l'analyse")
            if not Ips.empty:
                st.write("Une intrusion  a été détectée")
                st.table(Ips)
            else:
                st.write("Aucune intrusion n'a été détectée")    
    else:
        csv_file = st.file_uploader("Importer un fichier csv")
    
        if csv_file is not None:
            dataset, IPs = generate_dataset_from_csv(csv_file)
            st.text("Les donnees generes a partir du fichier csv:")
          
            Ips = analyser(dataset,rf)
            st.text("Resultats de l'analyse")
            st.table(Ips)


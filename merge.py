import pandas as pd
import csv

def generate_zeek_csv():
    logfile = pd.read_json("custHttpLog.log", lines=True) 
    logfile.to_csv ("traffic.csv", index=[0])

generate_zeek_csv()


'\n\tBULK INSERT dbo.zeek\n\tFROM "/home/rayan/Desktop/matchFeatures/zeek.csv"\n\tWITH(\n\t\tFIELDTERMINATOR = ",",\n\t\tROWTERMINATOR = "\n",\n\t\tFIRSTROW = 2\n\t)\n'
from re import M, S
import streamlit as st
import pandas as pd
import numpy as np
from sklearn.compose import make_column_selector
from sklearn.compose import make_column_transformer
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import MinMaxScaler
from sklearn.impute import SimpleImputer
from streamlit.elements import checkbox
import pickle
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score,precision_score, f1_score, recall_score, f1_score

def make_dataset(dataset):
  # read features file
    features = pd.read_csv('NUSW-NB15_features.csv', sep=',', encoding='cp1252')

    dataset.drop_duplicates(inplace=True)
  
    # to lower case
    features['Name'] = features['Name'].str.lower()
    features['Name'] = features['Name'].str.replace(" ", "")
    
    dataset.columns = features.Name
    
    #encodage 
    dataset = ordinal_encoding(dataset)
    return dataset 

def ordinal_encoding(dataset):
    
    start = 0
    cat_features = ["proto", "service" ,"state"]
    for cat_ft in cat_features:
        ordinal_mapping = {k:i for i, k in enumerate(dataset[cat_ft].unique(), start)}
        dataset[cat_ft] = dataset[cat_ft].map(ordinal_mapping)
        start += len(dataset[cat_ft].unique())
    return dataset


def Trait_val_vides(df, methode="constant"):

    #Sauvgarder les ips avant la normalisation
    columns = df.columns
    # ips = df["srcip"]
    # df.drop(["srcip"], axis=1, inplace=True)
    #*************************************

    categorical_features = df.select_dtypes(include="object").columns
    numerical_features = df.select_dtypes(exclude="object").columns

    
    numerical_pipeline = make_pipeline(SimpleImputer(missing_values=np.nan, strategy=methode, fill_value=-1))
        
    categorical_pipeline = make_pipeline(SimpleImputer(missing_values=np.nan, strategy=methode, fill_value=-1))
    
    preprocessor = make_column_transformer((numerical_pipeline, numerical_features),
                                           (categorical_pipeline,categorical_features))
    df = preprocessor.fit_transform(df)
    
    df =  pd.DataFrame(df)
    #Ajout des ips apres normalisation
    # df.insert(1,"srcip", ips)
    # df.columns = columns
    return df

def normalisation(df):
    #Sauvgarder les ips avant la normalisation
    columns = df.columns
    # ips = df["srcip"]
    # df.drop(["srcip"], axis=1, inplace=True)
    #*************************************
    categorical_features =df.select_dtypes(include="object").columns
    numerical_features = df.select_dtypes(exclude="object").columns
    
    
    numerical_pipeline = make_pipeline(MinMaxScaler())
        
    categorical_pipeline = make_pipeline(MinMaxScaler())
    preprocessor = make_column_transformer((numerical_pipeline, numerical_features),
                                           (categorical_pipeline,categorical_features))
    df = preprocessor.fit_transform(df)
    df =  pd.DataFrame(df)
    #Ajout des ips apres normalisation
    # df.insert(1,"srcip", ips)
    # df.columns = columns
    return df

def choix_methodes(data, trait_vide=False, encodage=False, norm=False,fill_value=None):
    if trait_vide:
       
        if fill_value == "-1":
            methode = "constant"
            data = Trait_val_vides(data, methode)
            
        
    elif encodage:
        data = ordinal_encoding(data)
       
    elif norm:
        data = normalisation(data) 
     
    st.success("Operations effectuees avec success")
    return data       
  
@st.cache(suppress_st_warning=True,allow_output_mutation=True)

def format_data(data_csv):
    return pd.read_csv(data_csv)

def show_preprocessing_page():
    st.write("""
        # Preprocessig des donnees
    """)
    dataframe = pd.DataFrame()
    methodeRemplacemnt = ""
    data_csv = st.file_uploader("Importer des donnees(csv)")
    
    if data_csv is not None:
        dataframe = format_data(data_csv)
    

        st.subheader("Etapes de preprocessing")
        trVid = st.checkbox("Traitement des valeurs vides")
        if trVid:
            methodeRemplacemnt = st.selectbox("Avec quelle valeur voulez vous remplacer les valeurs vides", 
                            ["-1","mean", "median","most_frequent"])
        enc = st.checkbox("Encodage")
        norm = st.checkbox("Normalisation")
        valider = st.button("valider")
        if valider:
            dataframe = choix_methodes(dataframe, trVid, enc, norm, methodeRemplacemnt)
            
            pickle.dump(dataframe, open("./datapreprocessed", "wb"))
            data = pickle.load(open("datapreprocessed", "rb"))
            Y_50000 = data["label"]
            data.drop(["srcip","label"], axis=1, inplace=True)
            x_train50000, x_test50000, y_train50000, y_test50000 = train_test_split(data, Y_50000, test_size = 0.08, shuffle=True)
            scaler = MinMaxScaler()

            x_train50000 = scaler.fit_transform(x_train50000)
            x_test50000 = scaler.transform(x_test50000)
            y_pred = rf.predict(x_test50000)
            st.write(recall_score(y_test50000, y_pred), precision_score(y_test50000, y_pred), f1_score(y_test50000, y_pred))


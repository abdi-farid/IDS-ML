import pickle
import streamlit as st 
import pandas as pd

# PATH TO MODELS -------------------------------------------
# path = 'D:\\M2\\S2\\Farid\\Notebook\\S2\\Memoire\\14 septembre\\ID exclus\\'
path = 'models\\'

# function to read CSv file
def read_dataset(data):
    return pd.read_csv(data, sep=',', encoding='cp1252')

# function to upload CSV file
def file_uploader():
    with st.sidebar.expander('Load data'):
        f = st.file_uploader('Upload your dataset',
                                    type='csv', help='Example : dataset.csv')
        if f : 
            df = read_dataset(f)
            st.success('Successfully uploaded !')

            st.markdown('---')
            return df

# sample the data set with percentage to be used
def percentage_of_data(df):
    if len(df) != 0:
        of_data = st.sidebar.slider('Use % of data', min_value=1, max_value=99, value=20, step=1)
        return of_data            


# get list of models deployed 
def get_models():
    import os 
    return [i for i in os.listdir(path) if len(i.split('.')) == 1]

# display model names 
def display_models():
    with st.sidebar.expander('Choose model'):
        model = st.selectbox('', ['---']+get_models())
        return model 


# ENCODING FUNCTION 
def ordinal_encoding(dataset):
    tmp_df = dataset.copy()
    start = 0
    cat_features = tmp_df.columns[tmp_df.dtypes == 'O']
    for cat_ft in cat_features:
        ordinal_mapping = {k:i for i, k in enumerate(tmp_df[cat_ft].unique(), start)}
        tmp_df[cat_ft] = tmp_df[cat_ft].map(ordinal_mapping)
        start += len(tmp_df[cat_ft].unique())
    return tmp_df


# PREPROCESSIONG FUNCTION 
def preprocessing(df, percetage):
    from sklearn.model_selection import train_test_split
    from sklearn.preprocessing import MinMaxScaler

    for i in ['srcip', 'rate', 'attack_cat']:
        if i in df.columns:
            df.drop([i], axis=1, inplace=True)

    df_encoded = ordinal_encoding(df)
    df_encoded.dropna(axis=0, inplace=True)

    Y = df_encoded.label
    X = df_encoded.drop('label', axis=1)

    train_data, test_data, y_train, y_test = train_test_split(
        X, Y, test_size=percetage/100, shuffle=True, random_state=42)
    print(train_data.shape, y_train.shape, test_data.shape, y_test.shape)

    scaler = MinMaxScaler()

    X_train = scaler.fit_transform(train_data)
    X_test = scaler.transform(test_data)

    return X_train, y_train, X_test, y_test



# get performances table
def performances_table(model, X_test, y_test):
    from sklearn.metrics import accuracy_score, precision_score, f1_score, recall_score

    ypred = model.predict(X_test)
    tmp = {}
    tmp['test_score'] = model.score(X_test, y_test)
    tmp['Accuracy_score'] = accuracy_score(y_test, ypred)
    tmp['Precision_score'] = precision_score(y_test, ypred)
    tmp['Recall_score'] = recall_score(y_test, ypred)
    tmp['f1_score'] = f1_score(y_test, ypred)
    
    with st.expander('Performances table'):
        if st.checkbox('Show'):
            st.dataframe(pd.DataFrame(tmp, index=['values']))


# ROW TO DISPLAY IN PAGE 
def split_page(model, df, percentage_data_to_use, model_name):
    _,_, X_test, y_test = preprocessing(df, percentage_data_to_use)

    import rows_performances as rows

    rows.row1(model, X_test, y_test, df)
    rows.row2(model, X_test, y_test)
    rows.row3(model, X_test, y_test, model_name)
    rows.row4(model, X_test, y_test)
    performances_table(model, X_test, y_test)


    
    
# ------------------------------------------------------ MAIN FUNCTION ------------------------------------------------------
def display_performances():

    df = file_uploader()
    if df is not None:
        percentage_data_to_use = percentage_of_data(df)

    model_name = display_models()
    if model_name != '---':
        model = pickle.load(open(path+model_name, 'rb'))

    if df is not None and model_name != '---':
        split_page(model, df, percentage_data_to_use, model_name)


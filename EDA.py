import streamlit as st 
import pandas as pd 
import numpy as np


# Expoloratory Data Analysis ----------------------------------------------

def file_uploader():
    return st.sidebar.file_uploader('Upload your dataset',
     type='csv', help='Example : dataset.csv')


def read_dataset(data):
    return pd.read_csv(data, sep=',', encoding='cp1252')


def get_shape(df):
    if st.checkbox('Shape'):
        st.write('Lines : ', df.shape[0], 'Columns : ',
         df.shape[1], ' ==> ', df.shape)


def head_line_number(k):
    return st.slider('Lines', min_value=2, max_value=15, value=5, key=k)


def get_head(df):
    if st.checkbox('Head'):
        n = head_line_number('1')
        st.dataframe(df.head(n))


def get_tail(df):
    if st.checkbox('Tail'):
        n = head_line_number('2')
        st.dataframe(df.head(n))


def get_some_columns(df):
    if st.checkbox('Columns to display'):
        cols = st.multiselect('Columns', df.columns, default='dur')
        n = head_line_number('3')
        st.dataframe(df[cols].head(n))


def get_describe(df):
    if st.checkbox('Describe'):
        st.dataframe(df.describe())

def get_columns(df):
    if st.checkbox('Columns name'):
        l, r = st.columns(2)
        for i in df.dtypes.value_counts().to_dict().keys():
            l.markdown('## '+str(i)+'')
            l.write({k: v for k, v in df.dtypes.to_dict().items() if v == i})
            l,r = r,l


def get_isna_null(df):
    if st.checkbox('NaN and null values'):
        isna = pd.DataFrame(df.isna().sum().to_dict(), index=['isna()'])
        isnull = pd.DataFrame(df.isnull().sum().to_dict(), index=['isnull()'])
        na_nul = pd.concat([isna, isnull])
        st.dataframe(na_nul)


def get_duplicated(df):
    if st.checkbox('Duplicated rows'):
        st.write(df.duplicated().sum(), 'row(s) duplicated')

# Visulaization ---------------------------------------------------------------

def bar_distribution(df):
    st.markdown('---')
    st.markdown('#### Categorical feature bar')
    var = st.selectbox('Feature', df.columns[df.dtypes == np.dtype('O')], key='barplot')
    if st.checkbox('Display bar plot'):
        st.bar_chart(df[var].value_counts(), use_container_width=True)


def pie_distribution(df):
    st.markdown('---')
    st.markdown('#### Categorical feature pie')
    var = st.selectbox(
        '', df.columns[df.dtypes == np.dtype('O')], key='pieplot')
    if st.checkbox('Pie plot'):
        st.set_option('deprecation.showPyplotGlobalUse', False)
        st.write(df[var].value_counts().plot.pie())
        st.pyplot()

def area_distribution(df):
    st.markdown('---')
    st.markdown('#### Categorical feature area')
    var = st.selectbox(
        '', df.columns[df.dtypes == np.dtype('O')], key='areaplot')
    if st.checkbox('Area plot'):
        st.set_option('deprecation.showPyplotGlobalUse', False)
        st.area_chart(df[var].value_counts())





def kde_dist(df):
    st.markdown('---')
    st.markdown('#### Numerical features distribution')
    import seaborn as sns 
    var = st.selectbox('', df.columns[df.dtypes != np.dtype('O')], key='distplot')
    if st.checkbox('Distribution plot'):
        st.set_option('deprecation.showPyplotGlobalUse', False)
        sns.displot(df,x=var, kind='kde', hue='label', fill=True)
        st.pyplot()

def bivariate(df):
    st.markdown('---')
    st.markdown('#### Numerical features distribution')
    import seaborn as sns 
    l, m, r = st.columns(3)
    f = l.selectbox('', df.columns[df.dtypes != np.dtype('O')], key='first')
    s = r.selectbox('', df.columns[df.dtypes != np.dtype('O')], key='seconde')
    if st.checkbox('Bivariate distribution'):
        st.set_option('deprecation.showPyplotGlobalUse', False)
        sns.displot(df,x=f, y=s, hue='label')
        st.pyplot()




# MAIN FUNCTION --------------------------------------------------------
def display_explore_data():
    st.title('Exploratory Data Analysis')

    file = file_uploader()
    if file: 
        df = read_dataset(file)
        st.sidebar.success('Successfully uploaded !')

        st.subheader('Explore your data')
        with st.expander(''):
            explore = [get_shape(df), get_head(df), get_tail(df), get_some_columns(df),
                       get_describe(df), get_columns(df), get_isna_null(df), get_duplicated(df)]


        st.subheader('Visualize your categorical data')
        with st.expander(''):
            visualize_cat = [bar_distribution(df), pie_distribution(df),
            area_distribution(df)]

        st.subheader('Visualize your numerical data')
        with st.expander(''):
            visualize_num = [kde_dist(df), bivariate(df)]

        

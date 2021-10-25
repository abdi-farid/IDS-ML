import streamlit as st 
st.set_page_config(layout="wide", page_title='ML-IDS')

menu = ['Home', 'Explore data', 'Analyze', 'Performances']
st.sidebar.markdown('## **OPTIONS**')
choice = st.sidebar.selectbox('', menu)

if choice == 'Home':
    st.title('Intrusion Detection System')
    st.header('Machine Learning')

elif choice == 'Explore data':
    import EDA as eda
    eda.display_explore_data()
elif choice == 'Analyze':
    from analyser import show_analyser_page
    show_analyser_page()
elif choice == 'Performances':
    import Performances as perf
    perf.display_performances()

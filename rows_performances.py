import streamlit as st  
from Performances import preprocessing 

import matplotlib.pyplot as plt
from collections import Counter

from sklearn.metrics import confusion_matrix

from sklearn.metrics import roc_curve, auc
import numpy as np 

from sklearn.metrics import precision_recall_curve
    


# DISPLAY ROW 1 ------------------------------------------------
# CONTIENT : - NOMBRE DE [1,0] DANS Y DE TEST ET Y D'APPRENTISSAGE
def row1(model, X_test, y_test, df):

    l, m = st.columns(2)

    with l.expander('Dataset'):

        if st.checkbox('Show', key='dataset'):
            labels = ['Normal', 'Suspect']
            tmp = Counter(df.label)
            sizes = [tmp[0], tmp[1]]
            explode = (0, 0.1)
            fig1, ax1 = plt.subplots()
            ax1.pie(sizes, explode=explode, labels=labels,
                    autopct='%1.2f%%', shadow=True, startangle=90)
            # Equal aspect ratio ensures that pie is drawn as a circle.
            ax1.axis('equal')
            st.pyplot(fig1)

    with m.expander('Predictions'):
        if st.checkbox('Show', key='predictions'):

            ypred = model.predict(X_test)

            labels = ['Normal', 'Suspect']
            tmp = Counter(ypred)
            sizes, explode = [tmp[0], tmp[1]], (0, 0.1)
            fig1, ax1 = plt.subplots()
            ax1.pie(sizes, explode=explode, labels=labels,
                    autopct='%1.2f%%', shadow=True, startangle=90)
            ax1.axis('equal')
            st.pyplot(fig1)


# DISPLAY ROW 2 ------------------------------------------------
# CONTIENT : - MATRICE DE CONFUSION
def row2(model, X_test, y_test):


    ypred = model.predict(X_test)
    l, r = st.columns(2)

    with l.expander('Confusion matrix'):
        
        if st.checkbox('Show', key='cm'):
            cm = confusion_matrix(y_test, ypred)
            from mlxtend.plotting import plot_confusion_matrix

            fig, ax = plot_confusion_matrix(
                conf_mat=cm, figsize=(4, 4), show_normed=True)
            plt.xlabel('Predictions', fontsize=8)
            plt.ylabel('Actuals', fontsize=8)
            #plt.title('Confusion Matrix', fontsize=18)
            st.pyplot(fig)
    
    with r.expander('Consufion matrix rate'):
        if st.checkbox('Show', key='cmr'):
            cm = confusion_matrix(y_test, ypred)
            from mlxtend.plotting import plot_confusion_matrix

            fig, ax = plot_confusion_matrix(
                conf_mat=cm, figsize=(4, 4), class_names=['Normal', 'Suspect'], show_normed=True)
            plt.xlabel('Predictions', fontsize=8)
            plt.ylabel('Actuals', fontsize=8)
            #plt.title('Confusion Matrix', fontsize=18)
            st.pyplot(fig)



# DISPLAY ROW 3 ------------------------------------------------
# CONTIENT : - ROC CURVE
#            - ROC CURVE THRESHOLD
def row3(model, X_test, y_test, model_name):

    l, r = st.columns(2)

    # # calculate predict_proba
    y_score = model.predict_proba(X_test)[:, 1]

    fpr, tpr, _ = roc_curve(y_test, y_score)

    # la colonne gauche affiche roc curve
    with l.expander('Roc curve'):

        if st.checkbox('Show', key='roc'):
            roc_auc = auc(fpr, tpr)
            fig, ax = plt.subplots()
            ax.plot(fpr, tpr, 'b', label = '{} - AUC = {:.4f}'.format(model_name,roc_auc))
            plt.legend(loc = 'lower right')
            plt.plot([0, 1], [0, 1],'r--')
            plt.xlim([-0.1, 1.1])
            plt.ylim([-0.1, 1.1])
            plt.ylabel('True Positive Rate')
            plt.xlabel('False Positive Rate')
            st.pyplot(fig)

    # la colonne droite affiche roc curve threshold
    with r.expander('Roc curve at various threshold'):

        if st.checkbox('Show', key='various'):
            fig, ax = plt.subplots()
            ax.plot(np.linspace(0, 1, tpr.shape[0]), tpr, 'b',label='TPR')
            ax.plot(np.linspace(0, 1, fpr.shape[0]), fpr, 'r',label='FPR')
            
            plt.legend(loc='lower right')
            plt.xlim([-0.1, 1.1])
            plt.ylim([-0.1, 1.1])
            plt.ylabel('Rate')
            plt.xlabel('Threshold')
            st.pyplot(fig)


# DISPLAY ROW 4 ------------------------------------------------
# CONTIENT : - PRECISION-RECALL CURVE
#            - PRECISION-RECALL THRESHOLD
def row4(model, X_test, y_test):
    
    y_score = model.predict_proba(X_test)[:, 1]
    
    l, r = st.columns(2)
    
    # calculate predict_proba
    precision, recall, _ = precision_recall_curve(y_test, y_score)

    # la colonne gauche affiche precision-recall curve 
    with l.expander('Precision-Recall Curve'):

        if st.checkbox('Show', key='prc'):

            fig, ax = plt.subplots()
            ax.plot(recall, precision, 'b', label='Precision-Recall')

            plt.legend(loc='lower right')
            plt.xlim([-0.1, 1.1])
            plt.ylim([-0.1, 1.1])
            plt.ylabel('Recall')
            plt.xlabel('Precision')
            st.pyplot(fig)

    # la colonne droite affiche precision-recall threshold 
    with r.expander('Precision-Recall curve at various threshold'):

        if st.checkbox('Show', key='prc_thresold'):
            fig, ax = plt.subplots()
            ax.plot(np.linspace(0, 1, precision.shape[0]), precision, 'b',label='Precision')
            ax.plot(np.linspace(0, 1, recall.shape[0]), recall, 'r',label='Recall')
            
            plt.legend(loc='lower right')
            plt.xlim([-0.1, 1.1])
            plt.ylim([-0.1, 1.1])
            plt.ylabel('Rate')
            plt.xlabel('Threshold')
            st.pyplot(fig)
    



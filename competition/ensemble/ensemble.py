from __future__ import print_function
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import RepeatedStratifiedKFold
from sklearn.metrics import roc_auc_score
from sklearn.metrics import log_loss
import pandas as pd
import numpy as np
import graphviz


'''
Convert function for Bank
'''
def str2int_month(month):
    month_str = str(month)
    return {'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
            'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12}[month_str]


'''
Convert function for Crime
'''
def str2int_dayofweek(day):
    day_str = str(day)
    return {'Monday': 1, 'Tuesday': 2, 'Wednesday': 3, 'Thursday': 4,
            'Friday': 5, 'Saturday': 6, 'Sunday': 7}[day_str]


def str2int_time(date):
    date_str = str(date)
    if date_str[12] == ':':
        hour = int(date_str[11:12])
        minute = int(date_str[13:15])
    else:
        hour = int(date_str[11:13])
        minute = int(date_str[14:16])
    return hour * 60 + minute


'''
Function for Crime and Bank
'''
def make_dtype_category(data):
    for col in data.columns:
        if data[col].dtype == 'object':
            data[col] = data[col].astype('category').cat.codes
    return data


def make_xy_data(data, target_col, remove_cols):
    data = make_dtype_category(data)
    y = data[target_col]
    del data[target_col]
    for col in remove_cols:
        del data[col]
    features = list(data.columns)
    x = data[features]
    return x, y


if __name__ == '__main__':
    subject = 'crime'
    
    # Read data.
    data_path = './data/' + subject
    train_data = pd.read_csv(data_path+'/train.csv')
    test_data = pd.read_csv(data_path+'/test.csv')
    
    # Preprocess data.
    if subject == 'bank':
        target_col = 'y'
        remove_cols = []
        del test_data['id']
            
        train_data['month'] = train_data['month'].apply(str2int_month)
        test_data['month'] = test_data['month'].apply(str2int_month)
    
    elif subject == 'crime':
        target_col = 'Category'
        remove_cols = ['Descript', 'Resolution']#, 'Address']
        del test_data['Id']
        #del test_data['Address']
        
        train_data['Times'] = train_data['Dates'].apply(str2int_time)
        test_data['Times'] = test_data['Dates'].apply(str2int_time)
        train_data['DayOfWeek'] = train_data['DayOfWeek'].apply(str2int_dayofweek)
        test_data['DayOfWeek'] = test_data['DayOfWeek'].apply(str2int_dayofweek)
        
        train_category = train_data[target_col].astype('category').cat.categories
        category_dic = dict(enumerate(train_category))

    X, y = make_xy_data(train_data, target_col, remove_cols)
    test_x = make_dtype_category(test_data)

    if subject == 'bank':
        clf = RandomForestClassifier(n_estimators=40,
                                     criterion='gini',
                                     max_features=None,
                                     max_depth=18, #17~19?
                                     min_samples_split=2,
                                     min_samples_leaf=5,
                                     max_leaf_nodes=8000,
                                     min_impurity_decrease=0.,
                                     warm_start=True,
                                     class_weight='balanced') 
    elif subject == 'crime':
        clf = RandomForestClassifier(n_estimators=11,
                                     criterion='entropy', 
                                     max_depth=10, # try more than 10
                                     min_samples_leaf=5,
                                     warm_start=True)
    
    # K-fold cross validation.
    k = 5
    n = 5
    rskf = RepeatedStratifiedKFold(n_splits=k, n_repeats=n)
    _X = np.array(X)
    _y = np.array(y)
    score = 0
    for train_idx, eval_idx in rskf.split(_X, _y):
        train_x, train_y = _X[train_idx], _y[train_idx]
        eval_x, eval_y = _X[eval_idx], _y[eval_idx]

        clf.fit(train_x, train_y)

        if subject == 'bank':
            pred_y = clf.predict(eval_x)
            score += roc_auc_score(eval_y, pred_y)

        elif subject == 'crime':
            pred_y = clf.predict_proba(eval_x)
            pred_df = pd.DataFrame.from_records(pred_y)
            eval_y = pd.get_dummies(eval_y)
            eval_category = eval_y.columns.tolist()
            for i in pred_df.columns.tolist():
                if i not in eval_category:
                    eval_y[i] = pd.Series(np.zeros(len(eval_y)))
            eval_y.sort_index(axis=1, inplace=True)
            score += log_loss(eval_y, pred_df, eps=1e-15)
    print(score / (k * n))
    
    # Train.
    clf.fit(X, y)
    
    # Test and write the result.
    if subject == 'bank':
        predict_y = clf.predict(test_x)
        predict_dic = {'y': predict_y}
        predict_df = pd.DataFrame(predict_dic)
        predict_df.index += 1
        predict_df.to_csv(data_path+'/output.csv', index_label='id')

    elif subject == 'crime':
        predict_y = clf.predict_proba(test_x)
        predict_len = len(predict_y)
        predict_df = pd.DataFrame.from_records(predict_y)
        predict_df.rename(columns=category_dic, inplace=True)
        predict_category = predict_df.columns.tolist()
        for category in train_category:
            if category not in predict_category:
                predict_df[category] = pd.Series(np.zeros(predict_len))
        predict_df.sort_index(axis=1, inplace=True)
        predict_df.to_csv(data_path+'/output.csv', index_label='Id')
    

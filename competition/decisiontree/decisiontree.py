from __future__ import print_function
from sklearn import tree
import pandas as pd
import numpy as np
import graphviz


'''
Convert function for Bank
'''
def str2int_edu(edu):
    edu_str = str(edu)
    return {'primary': 1, 'secondary': 2, 'tertiary': 3, 'unknown': 0}[edu_str]


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


def str2int_date(date):
    date_str = str(date)
    month = int(date_str[5:7])
    day = int(date_str[8:10])
    return month * 30.5 + day


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
    # Read data.
    subject = 'bank'
    data_path = './data/' + subject
    train_data = pd.read_csv(data_path+'/train.csv')
    test_data = pd.read_csv(data_path+'/test.csv')
    
    # Preprocess data.
    if subject == 'bank':
        target_col = 'y'
        remove_cols = []
        del test_data['id']
        
        train_data['education'] = train_data['education'].apply(str2int_edu)
        test_data['education'] = test_data['education'].apply(str2int_edu)
        train_data['month'] = train_data['month'].apply(str2int_month)
        test_data['month'] = test_data['month'].apply(str2int_month)
    
    elif subject == 'crime':
        target_col = 'Category'
        remove_cols = ['Descript', 'Resolution']
        del test_data['Id']
        
        train_data['Times'] = train_data['Dates'].apply(str2int_time)
        test_data['Times'] = test_data['Dates'].apply(str2int_time)
        train_data['Dates'] = train_data['Dates'].apply(str2int_date)
        test_data['Dates'] = test_data['Dates'].apply(str2int_date)
        train_data['DayOfWeek'] = train_data['DayOfWeek'].apply(str2int_dayofweek)
        test_data['DayOfWeek'] = test_data['DayOfWeek'].apply(str2int_dayofweek)
        
        train_category = train_data[target_col].astype('category').cat.categories
        category_dic = dict(enumerate(train_category))

    train_x, train_y = make_xy_data(train_data, target_col, remove_cols)
    test_x = make_dtype_category(test_data)

    # Train.
    if subject == 'bank':
        clf = tree.DecisionTreeClassifier(criterion='entropy', min_samples_leaf=5) 
    
    elif subject == 'crime':
        clf = tree.DecisionTreeClassifier(criterion='entropy', max_depth=10, min_samples_leaf=5)
    
    clf.fit(train_x, train_y)

    # Visualize the graph.
    '''
    dot_data = tree.export_graphviz(clf, out_file=None, feature_names=train_x.columns)
    graph = graphviz.Source(dot_data)
    graph.render('tree_'+subject)
    
    print(clf.tree_.max_depth)
    '''
    
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


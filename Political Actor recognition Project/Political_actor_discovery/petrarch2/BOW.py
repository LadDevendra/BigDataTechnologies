'''
Created on May 6, 2017

@author: devendralad
'''
import wikipedia
import numpy as np
import pandas as pd
from sklearn.cross_validation import train_test_split
from sklearn.naive_bayes import MultinomialNB
from sklearn.feature_extraction.text import TfidfVectorizer
import pickle
from sklearn.externals import joblib
from string import digits

#read training data
df = pd.read_csv('/Users/devendralad/Desktop/TrainActors.txt', sep='\t', names = ['label', 'name'])
df['data'] = pd.Series('default' , index=df.index)
digits = str.maketrans('', '', digits)
#print(df)
#update summary for each actor in training data
for index, row in df.iterrows():
    ag = wikipedia.search(row['name'])
    df.loc[index, 'data'] = str(wikipedia.summary(ag[0])).translate(digits)

#make a TFIDF vertor for model building     
df_x = df["data"]
df_y = df["label"]
# cv = TfidfVectorizer(min_df=1, stop_words='english')
# x_traincv = cv.fit_transform(df_x)


x_train, x_test, y_train, y_test = train_test_split(df_x, df_y, test_size = 0.1, random_state = 4)
cv = TfidfVectorizer(min_df=1, stop_words='english')
x_traincv = cv.fit_transform(x_train)
# a = x_traincv.toarray()
#print(cv.inverse_transform(a[0]))
#print(x_train.iloc[0])
print(len(x_test))
mnb = MultinomialNB()
y_train = y_train.astype('int')
mnb.fit(x_traincv, y_train)

x_testcv = cv.transform(x_test)
pred = mnb.predict(x_testcv)
actual = np.array(y_test)
cnt =0
for i in range (len(pred)):
    if pred[i]==actual[i]:
        cnt = cnt +1
        
print(cnt/len(pred))
print(cnt)
print(len(pred))    
print(len(x_test))



import wikipedia
import random
import numpy as np
import pandas as pd
from sklearn.cross_validation import train_test_split
from sklearn.naive_bayes import MultinomialNB
from sklearn.feature_extraction.text import TfidfVectorizer
import pickle
from sklearn.externals import joblib
from string import digits
import string

df = pd.read_csv('/Users/nikitakothari/Downloads/dataset_new/new_actor_list.csv', sep=',', names = ['id', 'value'])
isPresent = 0
for index, row in df.iterrows():
    print row['id']





# coding: utf-8

# # Collapse utterances

# ## Load data using postgres

# In[1]:

from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import psycopg2
import pandas as pd

# Define a database name
dbname = 'phraze_db'
username = 'elahi' # change this to your username

## 'engine' is a connection to a database
engine = create_engine('postgres://%s@localhost/%s'%(username,dbname))
print(engine.url)

# Connect to make queries using psycopg2
con = None
con = psycopg2.connect(database = dbname, user = username)
con


# In[51]:

# Load data and select relevant columns
sql_query = """SELECT * FROM phraze_data_table;"""
df = pd.read_sql_query(sql_query, con)

# Choose columns to keep
columns_to_keep = ['ID', 'Line', 'UtteranceNumber', 'Role', 
                  'Text', 'Thread_number','note', 'note_cat']

df = df[columns_to_keep]
df.shape


# In[52]:

# Drop duplicates
df2 = df.drop_duplicates()
print df.shape
print df2.shape
df = df.drop_duplicates()


# In[53]:

import pandas as pd
import numpy as np
import re

# To save output
import pickle
from __future__ import division


# In[54]:

# Drop Thread_number or Text values that are None or NA
df.dropna(inplace=True, subset = ['Role','Text','Thread_number','note'])
df.shape
df_original = df


# In[127]:

# Subset the data for testing
df = df_original[:20]
print df.shape
df.head()


# In[128]:

## Function to compare previous cell and return boolean whether the same value
def comp_prev(a):
    return np.concatenate(([False],a[1:] == a[:-1]))

df['Role_match'] = comp_prev(df.Role.values)
df['Note_match'] = comp_prev(df.note.values)


# In[129]:

df[:5]


# In[130]:

## Test if all matches are True, then same number = True
## But if all matches are False, then same number = False

# For each row:
# 1. Extract Line_match, Role_match, Thread_match as array
# 2. Use all() to test whether all are True
# 3. Return this boolean as a new column


# In[131]:

test_row = df[0:1]
test_array = np.array(test_row[['Role_match', 'Note_match']])[0]
print test_array
test_boolean = all(test_array)
print test_boolean


# In[132]:

a = []
for i in range(1, len(df)+1):
    test_row = df[i-1:i]
    test_array = np.array(test_row[['Role_match', 'Note_match']])[0]
    test_boolean = all(test_array)
    a.append(test_boolean)
#print a
print len(a)


# In[133]:

print df.shape
df['same_number'] = a


# In[134]:

df.head()


# In[135]:

# Assign new number based on boolean
b = [1]
i = 1
same_number = df.same_number[i-1:i].values
print(same_number)
print(same_number == True)

if same_number == True:
    print i
else: 
    print i+1

b


# In[144]:

b = [10, 21, 3, 1, 0]
b[-1]


# In[145]:

b = [1]
for i in range(2, len(df)+1):
    same_number = df.same_number[i-1:i].values
    if same_number == True:
        b.append(b[-1])
    else:
        b.append(b[-1]+1)
len(b)


# In[146]:

print b
print df.same_number.values


# In[147]:

df['phrase'] = b


# In[148]:

df.to_csv("data_output/df_test_phrase_number.csv")


# In[149]:

# Prep for the loop
unique_lines = df.phrase.unique()
lines_n = len(unique_lines)
lines_n


# In[154]:

# Initialize the dataset
i = 0
line_i = unique_lines[0]
dat_new = df[df['phrase'] == line_i]
dat_new


# In[155]:

lines_n
range(1, lines_n)
for i in range(1, lines_n):
    line_i = unique_lines[i]
    dat_i = df[df['phrase'] == line_i]
    
    a = []
    for j in dat_i.Text:
        a.append(j)
        a2 = ' '.join(a)
    
    dat_i_slice = dat_i[0:1].reset_index()
    dat_i_slice.drop('Text', axis = 1, inplace=True) # Drop text
    dat_i_slice['Text'] = a2
    
    dat_new = dat_new.append(dat_i_slice)


# In[157]:

# Export testing dataset for visual checks
dat_new.to_csv("data_output/df_sub_new.csv")
df.to_csv("data_output/df_original.csv")


# ## Run on entire dataset

# In[162]:

print df.shape


# In[160]:

df = df_original

## Function to compare previous cell and return boolean whether the same value
def comp_prev(a):
    return np.concatenate(([False],a[1:] == a[:-1]))
df['Role_match'] = comp_prev(df.Role.values)
df['Note_match'] = comp_prev(df.note.values)

## Does the speaker role and note of the current row match the previous row?
## (i.e., should this be the same phrase number? True/False)
a = []
for i in range(1, len(df)+1):
    test_row = df[i-1:i]
    test_array = np.array(test_row[['Role_match', 'Note_match']])[0]
    test_boolean = all(test_array)
    a.append(test_boolean)
df['same_number'] = a

## Assign phrase numbers, starting with 1. 
## If same_number == True, use previous number; otherwise use previous number + 1
b = [1]
for i in range(2, len(df)+1):
    same_number = df.same_number[i-1:i].values
    if same_number == True:
        b.append(b[-1])
    else:
        b.append(b[-1]+1)        
df['phrase'] = b


# In[166]:

print df.tail()


# In[167]:

## Concatenate the Text, by phrase number 
# Prep for the loop
unique_lines = df.phrase.unique()
lines_n = len(unique_lines)

# Initialize the dataset
i = 0
line_i = unique_lines[0]
dat_new = df[df['phrase'] == line_i]

# Run the loop
for i in range(1, lines_n):
    line_i = unique_lines[i]
    dat_i = df[df['phrase'] == line_i]
    
    a = []
    for j in dat_i.Text:
        a.append(j)
        a2 = ' '.join(a)
    
    dat_i_slice = dat_i[0:1].reset_index()
    dat_i_slice.drop('Text', axis = 1, inplace=True) # Drop text
    dat_i_slice['Text'] = a2
    
    dat_new = dat_new.append(dat_i_slice)


# In[168]:

# started at 2:07pm
# finished at 2:59pm
dat_new.shape


# In[169]:

dat_new.to_csv("data_output/phraze_data_collapsed.csv")


# In[ ]:




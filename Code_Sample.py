# -*- coding: utf-8 -*-
import requests
import pandas as pd
import json
import MySQLdb
import csv
import glob
import time
# Read total nubmer of pages from Json file
TOTAL_PAGES = 64
FILENAME = "AaronWorks"
FOLDER_PATH = "C:/Users/sshsjj/.spyder/AaronWorks_Assignment/AaronWorks/"
OUTPUT_TABLE_PATH = "C:/Users/sshsjj/.spyder/AaronWorks_Assignment/AaronWorks_Tables/"
OUTPUT_TABLE_PATH_MYSQL = "C:/Users/sshsjj/.spyder/AaronWorks_Assignment/AaronWorks_Tables_MySQL/"
AWS_EC2_PATH = "http://XXXXXXXXXXXXXXXXXXX.us-east-1.elb.amazonaws.com:8000/records?page="
AWS_MYSQL_PATH = "data-engineer-rds.XXXXX.us-east-1.rds.amazonaws.com"



# 1. Read Data by doing restful call to get small JSONs and merge into chunks that will fit into PC memory 
     Read Data by from Mysql database by establishing AWS rds connections and do define the CHUNK_SIZE that will fit into PC memory
	 (CHUNK_SIZE and jsons size are user-defined values)
# Loop all urls to get 64 jsons
for i in range(1, TOTAL_PAGES+1):
    r = requests.get(AWS_EC2_PATH + str(i))
    f = open('AaronWorks/AaronWorks'+str(i)+'.json', 'w')
    f.write(r.text)
    # It will only finish writing after closing the IO stream
    f.close()

# Merge every 10 files in the systems
# Params:
# url: folder path
# i: nth JSON
def readFromJSON(path, filename ,i, msg = 'Read JSON and put into pd''s dataframe'):
    with open(path + filename + str(i)+'.json', 'r') as json_file:
        f = json.load(json_file)
    return json.dumps(f['rows']);

# Usage: df = readFromJSON(FOLDER_PATH, FILENAME, 1)

# JSON File has been created 7 chunks
for i in range(1, TOTAL_PAGES+1):
    json_chunk = readFromJSON(FOLDER_PATH, FILENAME, i)
    f = open(OUTPUT_TABLE_PATH+str(i/10)+'.json', 'a')
    f.write(json_chunk)
    f.close()


# Read from MySQL
CHUNK_SIZE = 100000
db = MySQLdb.connect(host=AWS_MYSQL_PATH,    # your host, usually localhost
                     user="AaronWorks_read_only",         # your username
                     passwd="N&f#vSq9",  # your password
                     db="Aaron_Lab",
                     port=3306)        # name of the data base

cursor = db.cursor()
cursor.execute('select count(*) from AaronWorks_list_v1')
count_tuple = cursor.fetchone()
# fetchone() will return one tuple, fetchall() will return tuple of tuple
count = count_tuple[0]


#MySql total counts: 637150
# MySQL: Data has been splited into 7 chunks
# Table Schema
cursor.execute('desc AaronWorks_list_v1')
s = cursor.fetchall()
# (('id', 'int(11)', 'NO', 'PRI', None, 'auto_increment'),
#  ('name', 'varchar(64)', 'NO', '', None, ''),
#  ('address', 'varchar(128)', 'NO', '', None, ''),
#  ('birthdate', 'varchar(10)', 'NO', '', None, ''),
#  ('sex', 'varchar(1)', 'NO', '', None, ''),
#  ('job', 'varchar(64)', 'NO', '', None, ''),
#  ('company', 'varchar(64)', 'NO', '', None, ''),
#  ('emd5', 'varchar(32)', 'NO', 'MUL', None, ''))

# Load MYSQL table to files
for i in range(1, count, CHUNK_SIZE):
    cursor.execute('select * from AaronWorks_list_v1 where id between %(start)s and %(end)s', {'start': str(i), 'end' : str(i+CHUNK_SIZE-1)})
    with open(OUTPUT_TABLE_PATH_MYSQL+str(i/CHUNK_SIZE)+'.csv', 'wb') as f:
        csv_out = csv.writer(f)
        csv_out.writerow(['id', 'name', 'address', 'birthdate', 'sex', 'job', 'company', 'emd5'])
        for row in cursor.fetchall():
            csv_out.writerow(row)
        f.close()


start = time.clock()

# Pandas: Read All JSON and MySQL files from folder
# Read JSONS
json_file_names = glob.glob(OUTPUT_TABLE_PATH + "*.json")
# Read csvs
csv_file_names = glob.glob(OUTPUT_TABLE_PATH_MYSQL + "*.csv")

total_intersection = pd.DataFrame({})
total_complement_AaronWorks= pd.DataFrame({})
total_complement_AaronLab= pd.DataFrame({})
total_intersection1 = pd.DataFrame({})

# i = 0
# j = 0



2.	How many users in the AaronWorks dataset are also in AaronLab's dataset? How many are just in AaronLab's? How many are just in AaronWorks'?
	For the users within that intersection, what percent have different job titles between the two sets?

# Left join function (AaronWorks, AaronLab)
# Loop and join with each chunks between JSONs and Mysqls
for json1 in json_file_names:
    # i = i+1     [ key:value, key:value, ... ]
    # Merged JSONs look like [jsons][jsons]... Replace ][ with ,
    data = open(json1).read().replace('][',',')
    data_list = json.loads(data)
    df1 = pd.read_json(data, orient='records')[['emd5', 'job', 'company']]  # [] Series [[]] Dataframe
    for csv in csv_file_names:
        # j = j+1
        df2 = pd.read_csv(csv)[['emd5', 'job', 'company']]
        # Intersection
        intersection = pd.merge(df1, df2, on=['emd5'])[['emd5', 'job_x', 'company_x', 'job_y', 'company_y']].rename(
            columns={'job_x': 'job_AaronWorks', 'company_x': 'company_AaronWorks',
                     'job_y': 'job_AaronLab', 'company_y': 'company_AaronLab'})
        total_intersection = pd.concat([total_intersection, intersection]) # <= per CHUNK_SIZE  #SQL Union All might have duplicates
######## Additional Task: total_intersection will be printed and save to csv sequentially
        # Percentage: Assume our output can't fit the memory also
        # Grouped counts/ Total counts
        # float64 to String and append '%'
        # After aggregation, it should fit into memory
        # Similar to Map-Reduce
    # j=0
    r_AaronWorks = pd.merge(df1, intersection, on=['emd5'], how='left')
    complement_AaronWorks = r_AaronWorks[r_AaronWorks['job_AaronWorks'].isna() == True ][['emd5', 'job', 'company']].rename(
        columns={'job': 'job_AaronWorks', 'company': 'company_AaronWorks'})
    total_complement_AaronWorks = pd.concat([total_complement_AaronWorks, complement_AaronWorks])

# i = 0
# j = 0

 # Left join function (AaronWorks, AaronLab)
 # Loop and join with each chunks between JSONs and Mysqls
for csv in csv_file_names:
    # i = i + 1
    df1 = pd.read_csv(csv)[['emd5', 'job', 'company']]
    for json1 in json_file_names:
        # j = j + 1
        # Merged JSONs look like [jsons][jsons]... Replace ][ with ,
        data = open(json1).read().replace('][', ',')
        data_list = json.loads(data)
        df2 = pd.read_json(data, orient='records')[['emd5', 'job', 'company']]
        # Intersection
        intersection = pd.merge(df1, df2, on=['emd5'])[['emd5', 'job_x', 'company_x', 'job_y', 'company_y']].rename(
            columns={'job_x': 'job_AaronWorks', 'company_x': 'company_AaronWorks',
                     'job_y': 'job_AaronLab', 'company_y': 'company_AaronLab'})
        total_intersection1 = pd.concat([total_intersection1, intersection])  # <= per CHUNK_SIZE
    #     print i, j
    #     print 'Intersection: ', total_intersection1.shape[0]
    # j = 0
    r_AaronLab = pd.merge(df1, intersection, on=['emd5'], how='left')
    complement_AaronLab = r_AaronLab[r_AaronLab['job_AaronWorks'].isna() == True][['emd5', 'job', 'company']].rename(
              columns={'job': 'job_AaronLab', 'company': 'company_AaronLab'})
    total_complement_AaronLab = pd.concat([total_complement_AaronLab, complement_AaronLab])


output = pd.DataFrame(total_intersection)
output['AaronWorks_JSON'] = '[{"job": '+'"'+output['job_AaronWorks']+ '",'+  ' "company": '+ '"'+output['company_AaronWorks']+ '"}]'
output['AaronLab_JSON'] = '[{"job": '+'"'+output['job_AaronLab']+ '",'+  ' "company": '+ '"'+output['company_AaronLab']+ '"}]'
output[['emd5','AaronWorks_JSON','AaronLab_JSON']].to_csv("AaronWorks_intersection.csv", index = False, encoding = 'utf8')


# 3. Total Percentage with different job titles between 2 datasets
total_intersection.loc[(total_intersection['job_AaronWorks']==total_intersection['job_AaronLab']),'Match'] = 1
number_different_users = total_intersection[total_intersection['Match'] != 1].shape[0]
percentage_different_users = str(float(number_different_users)/float(total_intersection.shape[0])*100) + '%'


end = time.clock()
print end-start


# 4. Write Output to CSV File
total_intersection1 = pd.DataFrame({})
for csv in csv_file_names:
    # i = i + 1
    df1 = pd.read_csv(csv)[['emd5', 'job', 'company']]
    for i in range(0, total_intersection.shape[0], CHUNK_SIZE ):
        if (i + CHUNK_SIZE < total_intersection.shape[0]):
            df2 = total_intersection[i:i+ CHUNK_SIZE]
        else:
            df2 = total_intersection[i:total_intersection.shape[0]]
        # Intersection
        intersection = pd.merge(df1, df2, on=['emd5'])[['emd5', 'job', 'company']].rename(
            columns={'job': 'job_AaronLab', 'company': 'company_AaronLab'})
        total_intersection1 = pd.concat([total_intersection1, intersection])  # <= per CHUNK_SIZE
    #     print i, j
    #     print 'Intersection: ', total_intersection1.shape[0]
    # j = 0
    r_AaronLab = pd.merge(df1, intersection, on=['emd5'], how='left')
    complement_AaronLab = r_AaronLab[r_AaronLab['job_AaronWorks'].isna() == True][['emd5', 'job', 'company']].rename(
              columns={'job': 'job_AaronLab', 'company': 'company_AaronLab'})
    total_complement_AaronLab = pd.concat([total_complement_AaronLab, complement_AaronLab])










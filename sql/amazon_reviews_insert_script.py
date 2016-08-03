# -*- coding: utf-8 -*-
"""
Created on Thu Jul  7 18:32:15 2016

@author: cvint
"""

import psycopg2
import pandas
import numpy as np
from datetime import datetime

#Connect to database
conn = psycopg2.connect(database="RecallsReviews", user="unsafefoods", password="Password1", host="unsafefoods.csya4zsfb6y4.us-east-1.rds.amazonaws.com", port="5432")

print("Opened database successfully")

#upload review csv file
review_data = pandas.read_csv("C://Users/cvint/all_reviews.csv")
print('read data')

#create a cursor to execute SQL
cur = conn.cursor()

#This was used during the debugging and initial writing of the script.
#cur.execute("DELETE FROM Review;")

#Track reviewers already added
cur.execute('SELECT amazon_reviewer_id from reviewer')
reviewer_ids = pandas.DataFrame(cur.fetchall())
reviewer_ids = list(reviewer_ids.iloc[:,0])
print(reviewer_ids[:10])

#get dataframe of all reviewers and their review times
cur.execute('SELECT reviewer.amazon_reviewer_id, review.unix_review_time\
            from reviewer join review on reviewer.reviewer_id = review.reviewer_id')
            
reviewer_data = pandas.DataFrame(cur.fetchall())

count = 0

val_string = ''

reviewer_string = ''

for row in range(246850+68300+159800+140100+48800+9900+17600+2600+21500,review_data.shape[0]):
    count += 1
    print(count)
    review_date = review_data.reviewTime[row]
    review_text = review_data.reviewText[row]
    summary = review_data.summary[row]
    overall = int(review_data.overall[row])
    unix_review_time = int(review_data.unixReviewTime[row])
    amazon_reviewer_fk = review_data.reviewerID[row]
    review_asin = review_data.asin[row].strip()
    
    review_text = str(review_text).replace('\'', '')
    summary = str(summary).replace('\'', '')
    reviewer_name = str(review_data.reviewerName[row]).replace('\'', '')
	#check to see if reviewer already exists
    if amazon_reviewer_fk not in reviewer_ids:
        reviewer_string+= ', (nextval(\'reviewer_serial\'),  \'%s\', \'%s\')' % (amazon_reviewer_fk, reviewer_name)
        reviewer_ids.append(amazon_reviewer_fk)
	
    #check if review already exists by checking unix review time and amazon reviewer id
    #cur.execute('Select * from review where unix_review_time = %d and reviewer_id in (select reviewer_id from reviewer\
    #				where amazon_reviewer_id = \'%s\');' % (unix_review_time, amazon_reviewer_fk))
    exists = False
    reviewer_rows = reviewer_data[reviewer_data[0] == amazon_reviewer_fk].index.tolist()
    for rrow in reviewer_rows:
        if reviewer_data.iloc[rrow,1] == int(unix_review_time):
            exists = True
            print(reviewer_data.iloc[rrow,1])
            break
	
    if not exists:
        val_string += ',(nextval(\'review_serial\'), \
                        (SELECT reviewer_id from reviewer where amazon_reviewer_id = \'%s\' limit 1), \
                        (SELECT product_id from product where asin = \'%s\' limit 1), \'%s\', \'%s\', \
                        %d, %d, to_timestamp(\'%s\', \'dd mm yyyy\'))' % (amazon_reviewer_fk, \
                        review_asin, review_text, summary, overall, unix_review_time, review_date)
        reviewer_data.append([amazon_reviewer_fk,unix_review_time])
        
        if count % 100 == 0:
            cur.execute('INSERT INTO REVIEWER (reviewer_id, amazon_reviewer_id, reviewer_name)\
                        VALUES %s' % reviewer_string[1:])
            reviewer_string = ''
            cur.execute('INSERT INTO Review (review_id, reviewer_id, product_id, review_text, \
                summary, overall, unix_review_time, review_time) VALUES %s' % val_string[1:])
            conn.commit()
            print('committed')
            print(overall)
            val_string = ''

#commit and close
conn.commit()
print("Records created successfully")
conn.close()

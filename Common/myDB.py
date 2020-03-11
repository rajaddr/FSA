'''

*   Version : 1.0
*   User : Dharmaraj
*   EMail : rajaddr@gmail.com

'''
import pymysql.cursors
import pymysql
import logging

import pandas as pd

class myDB:

    def getConnectionmySQL(self):
        db = pymysql.connect(host = "remotemysql.com", db = "jTwkfX19tK", user = "jTwkfX19tK", passwd = "NnGQ8Sd5tl")

        #return db
        return db.cursor()

    def exec(self, SQL):

        try:
            #connection = pymysql.connect("remotemysql.com", "T7QMNssx0u", "XgUVn1K6WD", "T7QMNssx0u")
            connection = self.getConnectionmySQL()
            connection.execute(SQL)
            field_names = [i[0] for i in connection.description]
            result = pd.DataFrame(connection.fetchall(), columns=[i[0] for i in connection.description])
        except Exception as e:
            print("Error While execution in DB")
            print(e)
            temp=""
            result = pd.DataFrame(temp)

        finally:
            logging.info("SQL Commend Executed - " + SQL)
            connection.close()

        return result


'''

*   Version : 1.0
*   User : Dharmaraj
*   EMail : rajaddr@gmail.com

'''
import pandas as pd, numpy as np
from Common.myDB import myDB

class school:
    def apiSchool(self):
        dbConn = myDB()
        df_school = pd.DataFrame(dbConn.exec("SELECT * FROM school_tbl"))
        df_school['IS_US'] = df_school['IS_US'].astype(int)
        df_school['Code'] = df_school['Code'].astype(int)
        df_school['Zip'] = df_school['Zip'].fillna(-1)
        df_school['Zip'] = df_school['Zip'].astype(int)
        df_school['Name'] = df_school["School"] + " [ " + df_school["Code"].map(str) + " ]"
        df_school.drop(['Code', 'School', 'Zip_Code'], axis=1, inplace = True)
        return df_school

    def apiisUs(self):
        dbConn = myDB()
        df_school = pd.DataFrame(dbConn.exec("SELECT distinct(IS_US) as IS_US FROM school_tbl"))
        return df_school['IS_US'].astype(int)

    def apistate(self, args):
        dbConn = myDB()
        print(args)
        Where = "SELECT distinct(state) as state FROM school_tbl s WHERE  s.Code IN (SELECT DISTINCT(Code) FROM aid_tbl WHERE YEAR > (SELECT MAX(YEAR) - 1 FROM aid_tbl)) "
        if (len(args['IS_US']) != 0):
            Where += " AND s.IS_US = " + args['IS_US']
        Where += " ORDER BY IS_US"
        df_school = pd.DataFrame(dbConn.exec(Where))
        df_school.sort_values(by=['state'], inplace=True)
        return df_school['state']

    def apizip(self, args):
        dbConn = myDB()
        Where = "SELECT distinct(zip) as Zip FROM school_tbl s WHERE  s.Code IN (SELECT DISTINCT(Code) FROM aid_tbl WHERE YEAR > (SELECT MAX(YEAR) - 1 FROM aid_tbl)) AND zip != '-1' "

        if (len(args['IS_US']) != 0):
            Where += " AND s.IS_US = " + args['IS_US']
        if (len(args['State']) != 0):
            Where += " AND s.State = '" + args['State'] + "'"

        Where += " ORDER BY zip"

        df_school = pd.DataFrame(dbConn.exec(Where))
        df_school['Zip'] = df_school['Zip'].fillna(-1)
        return df_school['Zip'].astype(str)

    def apiSchool_Type(self, args):
        dbConn = myDB()
        Where = ""

        if (len(args['IS_US']) != 0):
            Where += " AND s.IS_US = " + args['IS_US']
        if (len(args['State']) != 0):
            Where += " AND s.State = '" + args['State'] + "'"
        if (len(args['Zip']) != 0):
            Where += " AND s.Zip = '" + args['Zip'] + "'"
        Where += " ORDER BY School_Type"
        df_school = pd.DataFrame(dbConn.exec("SELECT distinct(School_Type) as School_Type FROM school_tbl s WHERE  s.Code IN (SELECT DISTINCT(Code) FROM aid_tbl WHERE YEAR > (SELECT MAX(YEAR) - 1 FROM aid_tbl)) " + Where))
        df_school.sort_values(by=['School_Type'], inplace=True)
        return df_school['School_Type']

    def apiname(self, args):
        dbConn = myDB()
        Where = ""
        if (len(args['IS_US']) != 0):
            Where += " AND s.IS_US = " + args['IS_US']
        if (len(args['State']) != 0):
            Where += " AND s.State = '" + args['State'] + "'"
        if (len(args['Zip']) != 0):
            Where += " AND s.Zip = '" + args['Zip'] + "'"
        if (len(args['School_Type']) != 0):
            Where += " AND s.School_Type = '" + args['School_Type'] + "'"

        df_school = pd.DataFrame(dbConn.exec("SELECT * FROM school_tbl s WHERE  s.Code IN (SELECT DISTINCT(Code) FROM aid_tbl WHERE YEAR > (SELECT MAX(YEAR) - 1 FROM aid_tbl)) " + Where))
        df_school['Name'] = df_school["School"] + " [ " + df_school["Code"].map(str) + " ]"
        df_school.sort_values(by=['Name'], inplace=True)
        return df_school['Name']

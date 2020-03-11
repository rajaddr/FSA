'''

*   Version : 1.0
*   User : Dharmaraj
*   EMail : rajaddr@gmail.com

'''
import pandas as pd, numpy as np
from Common.myDB import myDB
from model import model

class aid:
    def apiYear(self):
        dbConn = myDB()
        modelObj = model()
        SQL = 'SELECT Year, Quarter, SUM(Recipients) as Recipients, SUM(No_Originated) as No_Originated, SUM(Value_Originated) as Value_Originated, SUM(No_Disbursements) as No_Disbursements, SUM(Value_Disbursements) as Value_Disbursements from aid_tbl a WHERE Loan_Type NOT IN ("DL UNSUBSIDIZED") AND Code IN (SELECT DISTINCT(Code) FROM aid_tbl WHERE YEAR > (SELECT MAX(YEAR) - 1 FROM aid_tbl)) GROUP BY Year, Quarter'
        data, forecast, accy, accy1, point = modelObj.model_date(pd.DataFrame(dbConn.exec(SQL)))
        return data, forecast, accy, accy1, point

    def forcast(self,args):
        Where = ""

        if (len(args['IS_US']) != 0):
            Where += " AND s.IS_US = " + args['IS_US']
        if (len(args['State']) != 0):
            Where += " AND s.State = '" + args['State'] + "'"
        if (len(args['Zip']) != 0):
            Where += " AND s.Zip = '" + args['Zip'] + "'"
        if (len(args['School_Type']) != 0):
            Where += " AND s.School_Type = '" + args['School_Type'] + "'"
        if (len(args['Name']) != 0):
            Where += " AND s.Code = " + args['Name'].split('[ ')[1].split(' ]')[0]
        if (len(args['Aid_Type']) != 0):
            Where += " AND a.Loan_Type = '" + args['Aid_Type'] + "'"

        dbConn = myDB()
        modelObj = model()
        SQL = 'SELECT a.Year, a.Quarter, SUM(a.Recipients) as Recipients, SUM(a.No_Originated) as No_Originated, SUM(a.Value_Originated) as Value_Originated, SUM(a.No_Disbursements) as No_Disbursements, SUM(a.Value_Disbursements) as Value_Disbursements from aid_tbl a, school_tbl s WHERE a.Loan_Type NOT IN ("DL UNSUBSIDIZED") AND a.Code IN (SELECT DISTINCT(Code) FROM aid_tbl WHERE YEAR > (SELECT MAX(YEAR) - 1 FROM aid_tbl)) AND s.Code = a.Code ' + Where + ' GROUP BY a.Year, a.Quarter'
        data, forecast, accy, accy1, point = modelObj.model_date(pd.DataFrame(dbConn.exec(SQL)))

        return data, forecast, accy, accy1, point

    def aidtype(self,args):
        Where = ""

        if (len(args['IS_US']) != 0):
            Where += " AND s.IS_US = " + args['IS_US']
        if (len(args['State']) != 0):
            Where += " AND s.State = '" + args['State'] + "'"
        if (len(args['Zip']) != 0):
            Where += " AND s.Zip = '" + args['Zip'] + "'"
        if (len(args['School_Type']) != 0):
            Where += " AND s.School_Type = '" + args['School_Type'] + "'"
        if (len(args['Name']) != 0):
            Where += " AND s.Code = " + args['Name'].split('[ ')[1].split(' ]')[0]

        dbConn = myDB()
        SQL = 'SELECT a.Loan_Type from aid_tbl a, school_tbl s WHERE a.Loan_Type NOT IN ("DL UNSUBSIDIZED") AND a.Code IN (SELECT DISTINCT(Code) FROM aid_tbl WHERE YEAR > (SELECT MAX(YEAR) - 1 FROM aid_tbl)) AND s.Code = a.Code ' + Where + ' GROUP BY a.Loan_Type ORDER BY a.Loan_Type'
        data = pd.DataFrame(dbConn.exec(SQL))
        data['aidtype'] = data['Loan_Type'].replace(['DL SUBSIDIZED- UNDERGRADUATE', 'DL SUBSIDIZED- GRADUATE'],
                                                  'DL SUBSIDIZED')
        data.drop(['Loan_Type'], axis=1, inplace=True)
        return data

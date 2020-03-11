'''

*   Version : 1.0
*   User : Dharmaraj
*   EMail : rajaddr@gmail.com

'''
import pandas as pd, numpy as np
from Common.myDB import myDB

import time, itertools, logging

from statsmodels.tsa.statespace.sarimax import SARIMAX
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from pmdarima.arima import auto_arima
from sklearn.metrics import mean_squared_error, mean_absolute_error

import warnings
warnings.filterwarnings('ignore')

class model:
    def model1(self):
        dbConn = myDB()
        return 'df_year'

    def model_eval(self, y, predictions):
        mae = mean_absolute_error(y, predictions)
        mse = mean_squared_error(y, predictions)
        SMAPE = np.mean(np.abs((y - predictions) / ((y + predictions) / 2))) * 100
        rmse = np.sqrt(mean_squared_error(y, predictions))
        MAPE = np.mean(np.abs((y - predictions) / y)) * 100
        mfe = np.mean(y - predictions)
        NMSE = mse / (np.sum((y - np.mean(y)) ** 2) / (len(y) - 1))
        error = y - predictions
        mfe = np.sqrt(np.mean(predictions ** 2))
        mse = np.sqrt(np.mean(y ** 2))
        rmse = np.sqrt(np.mean(error ** 2))
        theil_u_statistic = rmse / (mfe * mse)

        return [round(mae, 3), round(mse, 3), round(rmse, 3), round(MAPE, 3), round(SMAPE, 3), round(mfe, 3),
                round(NMSE, 3), round(theil_u_statistic, 3)]

    def arima_model_value(self,y):
        p = d = q = range(0, 2);
        c3 = [];c4 = []
        for param in list(itertools.product(p, d, q)):
            for param_seasonal in [(x[0], x[1], x[2], 4) for x in list(itertools.product(p, d, q))]:
                try:
                    results = SARIMAX(y, order=param, seasonal_order=param_seasonal, enforce_stationarity=False,
                                      enforce_invertibility=False).fit()
                    if np.isnan(results.aic) == False:
                        c3.append(results.aic)
                        c4.append([results.aic, param, param_seasonal])
                except:
                    continue

        return list(c4[np.argmin(c3)][1]), list(c4[np.argmin(c3)][2])

    def f_Holt_Wint(self,O_Train, O_Test):
        model = ExponentialSmoothing(O_Train, trend='add', seasonal='add', seasonal_periods=4, damped=True)
        hw_model = model.fit(optimized=True, use_boxcox=False, remove_bias=False)
        p1 = O_Test.reset_index()
        pre = pd.DataFrame(hw_model.forecast(len(O_Test)))
        pre.reset_index(drop=True, inplace=True)
        pred1 = pd.concat([p1['Date'], pre], axis=1)
        pred1.columns = ['Date', 'pred']
        pred1 = pred1.set_index('Date')
        pred = pred1['pred']
        return pred

    def f_ARIMA(self,O_Train, O_Test, order1, seasonal_order1):
        ar_model = SARIMAX(O_Train, order=order1, seasonal_order=seasonal_order1).fit()
        #pred = ar_model.predict(start=O_Test.index[0], end=O_Test.index[-1])
        p1 = O_Test.reset_index()
        pre = pd.DataFrame(ar_model.forecast(len(O_Test)))
        pre.reset_index(drop=True, inplace=True)
        pred1 = pd.concat([p1['Date'], pre], axis=1)
        pred1.columns = ['Date', 'pred']
        pred1 = pred1.set_index('Date')
        pred = pred1['pred']
        return pred

    def f_AutoARIMA(self,O_Train, O_Test):
        model = auto_arima(O_Train, trace=True, error_action='ignore', suppress_warnings=True)
        model.fit(O_Train)

        forecast = model.predict(n_periods=len(O_Test))
        forecast = pd.DataFrame(forecast, index=O_Test.index, columns=['Prediction'])
        return forecast

    def get_train_test(self,r_df):
        train_size = int(len(r_df) * 0.80)
        return r_df[0:train_size], r_df[train_size:]

    def model_date(self,df_r):
        Col = ['Recipients', 'No_Originated', 'Value_Originated', 'No_Disbursements', 'Value_Disbursements']
        a_Col = ['Model', 'Type', 'Mean Absolute Error', 'Mean Squared Error', 'Root Mean Squared Error',
                 'Mean absolute percentage error', 'Scaled Mean absolute percentage error', 'Mean forecast error',
                 'Normalised mean squared error', 'Theil_u_statistic']
        columns = ['h_Recipients', 'h_No_Originated', 'h_Value_Originated', 'h_No_Disbursements',
                   'h_Value_Disbursements', 'A_Recipients', 'A_No_Originated', 'A_Value_Originated',
                   'A_No_Disbursements', 'A_Value_Disbursements', 'S_Recipients', 'S_No_Originated',
                   'S_Value_Originated', 'S_No_Disbursements', 'S_Value_Disbursements']
        df_rt = pd.DataFrame(columns=columns)
        accy = []
        accy1 = []

        df_r.fillna(0)
        df_r.insert(0, "Date", pd.to_datetime(['-'.join(x.split()[::-1]) for x in ("Q" + df_r['Quarter'].astype(str) + " " + df_r['Year'].astype(str))]))
        df_r.reset_index(drop=True, inplace=True)
        df_r.set_index("Date", inplace=True)
        df_r.drop(['Year','Quarter'], axis = 1, inplace=True)
        df_r.sort_index(inplace=True)

        df_r['Recipients'] = df_r['Recipients'].fillna(0).astype('int64')
        df_r['No_Originated'] = df_r['No_Originated'].fillna(0).astype('int64')
        df_r['Value_Originated'] = df_r['Value_Originated'].fillna(0).astype('int64')
        df_r['No_Disbursements'] = df_r['No_Disbursements'].fillna(0).astype('int64')
        df_r['Value_Disbursements'] = df_r['Value_Disbursements'].fillna(0).astype('int64')

        train, test = self.get_train_test(df_r)
        testfull = pd.DataFrame(pd.date_range(df_r.index.max() + pd.DateOffset(1), periods=8, freq='QS'),
                                columns=['Date']).reset_index(drop=True).set_index("Date")
        for loop_a in train.columns[0:]:
            print("Model Running for Consolidated data : " + loop_a)
            order1, seasonal_order1 = self.arima_model_value(df_r[loop_a])
            print("(p,d,q)x(P,D,Q,s) : ", order1, seasonal_order1)

            test["y_holt_" + loop_a] = pd.DataFrame(self.f_Holt_Wint(train[loop_a], test[loop_a]))
            test["y_sarimax_" + loop_a] = pd.DataFrame(self.f_ARIMA(train[loop_a], test[loop_a], order1, seasonal_order1))
            test["y_autoarmia_" + loop_a] = pd.DataFrame(self.f_AutoARIMA(train[loop_a], test[loop_a]))

            accy.append(['HOLT', loop_a] + self.model_eval(test[loop_a], test["y_holt_" + loop_a].fillna(0)))
            accy.append(['SARIMAX', loop_a] + self.model_eval(test[loop_a], test["y_sarimax_" + loop_a].fillna(0)))
            accy.append(['AUTOARMIA', loop_a] + self.model_eval(test[loop_a], test["y_autoarmia_" + loop_a].fillna(0)))

            accy1.append([loop_a, self.model_eval(test[loop_a], test["y_holt_" + loop_a].fillna(0))[3], self.model_eval(test[loop_a], test["y_sarimax_" + loop_a].fillna(0))[3], self.model_eval(test[loop_a], test["y_autoarmia_" + loop_a].fillna(0))[3]])

            df_rt['h_' + loop_a] = pd.concat([df_r[loop_a], self.f_Holt_Wint(df_r[loop_a], testfull)])
            df_rt['A_' + loop_a] = self.f_AutoARIMA(df_r[loop_a], testfull)
            df_rt['S_' + loop_a] = pd.concat([df_r[loop_a], self.f_ARIMA(df_r[loop_a], testfull, order1, seasonal_order1)])

        df_rt.reset_index(drop=False, inplace=True)
        df_rt.columns = ['Date'] + columns

        for l in columns:
            df_rt[l] = df_rt[l].fillna(0).astype('int64')

        #for i in Col:
        #    df_rt['A_' + i] = df_rt['S_' + i] = df_rt['h_' + i]

        df_rt[df_rt['Date'] == df_r.index.max()]

        for i in Col:
            df_rt['A_' + i][df_rt['Date'] < df_r.index.max() + pd.DateOffset(1)] = df_rt['S_' + i][df_rt['Date'] < df_r.index.max() + pd.DateOffset(1)]

        data = df_rt[df_rt['Date'] < df_r.index.max() + pd.DateOffset(1)]#.reset_index(drop=True).set_index("Date")
        forecast = df_rt[df_rt['Date'] > df_r.index.max() - pd.DateOffset(1)]#.reset_index(drop=True).set_index("Date")
        data.drop_duplicates(keep='first',inplace=True)

        forecast = forecast.drop_duplicates()

        point = df_r.describe().transpose()
        point = point.reset_index(drop=False)
        point.columns = ['Type', 'Quarters', 'mean', 'std', 'minval', 'val25', 'val50', 'val75', 'maxval']
        point['Quarters'] = point['Quarters'].fillna(0).astype('int64')
        point['minval'] = point['minval'].fillna(0).astype('int64')
        point['maxval'] = point['maxval'].fillna(0).astype('int64')
        return data, forecast, pd.DataFrame(accy, columns=a_Col).fillna(0), pd.DataFrame(accy1, columns=['Name','HOLT','SARIMAX','AUTOARMIA']).fillna(0), point
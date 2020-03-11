'''

*   Version : 1.0
*   User : Dharmaraj
*   EMail : rajaddr@gmail.com

'''
from flask import Flask, jsonify, json, make_response, render_template, request, Response
from flask_cors import CORS

import logging.config
import yaml, datetime

import numpy as np
import pandas as pd

from school import school
from aid import aid

import warnings

warnings.filterwarnings('ignore')

app = application = Flask(__name__, template_folder='UI', static_folder='UI')
CORS(application)


@application.errorhandler(404)
def resource_not_found(e):
    return jsonify(error=str(e)), 404


@application.route('/main')
def Main():
    return 'pyFSAEngine ' + str(datetime.datetime.now()), 200


@application.route('/')
def Index():
    return application.send_static_file('index.html')

@application.route('/index')
def Index1():
    return application.send_static_file('index.html')

@application.route('/models')
def models():
    return application.send_static_file('model.html')
    #return Response('model.html', mimetype="text/html")

@application.route('/eda')
def EDA():
    return application.send_static_file('EDA.html')

@application.route('/status')
def status():
    return "pyFSAEngine " + str(datetime.datetime.now()), 200


@application.route('/school', methods=['GET', 'POST'])
def School():
    start_time = datetime.datetime.now()
    sc = school()
    rt = sc.apiSchool()
    logging.info('Duration: {}'.format(datetime.datetime.now() - start_time))
    return Response(response=rt.to_json(orient="values"), status=200, mimetype="application/json")

@application.route('/isUS', methods=['GET', 'POST'])
def isUS():
    start_time = datetime.datetime.now()
    sc = school()
    rt = sc.apiisUs()
    logging.info('Duration: {}'.format(datetime.datetime.now() - start_time))
    return Response(response=rt.to_json(orient="values"), status=200, mimetype="application/json")

@application.route('/state', methods=['GET', 'POST'])
def state():
    start_time = datetime.datetime.now()
    sc = school()
    rt = sc.apistate(request.get_json())
    logging.info('Duration: {}'.format(datetime.datetime.now() - start_time))
    return Response(response=rt.to_json(orient="values"), status=200, mimetype="application/json")

@application.route('/zip', methods=['GET', 'POST'])
def zip():
    start_time = datetime.datetime.now()
    sc = school()
    rt = sc.apizip(request.get_json())
    logging.info('Duration: {}'.format(datetime.datetime.now() - start_time))
    return Response(response=rt.to_json(orient="values"), status=200, mimetype="application/json")

@application.route('/School_Type', methods=['GET', 'POST'])
def School_Type():
    start_time = datetime.datetime.now()
    sc = school()
    rt = sc.apiSchool_Type(request.get_json())
    logging.info('Duration: {}'.format(datetime.datetime.now() - start_time))
    return Response(response=rt.to_json(orient="values"), status=200, mimetype="application/json")

@application.route('/name', methods=['GET', 'POST'])
def name():
    start_time = datetime.datetime.now()
    sc = school()
    rt = sc.apiname(request.get_json())
    logging.info('Duration: {}'.format(datetime.datetime.now() - start_time))
    return Response(response=rt.to_json(orient="values"), status=200, mimetype="application/json")

@application.route('/aidtype', methods=['GET', 'POST'])
def aidtype():
    start_time = datetime.datetime.now()
    ai = aid()
    rt = ai.aidtype(request.get_json())
    logging.info('Duration: {}'.format(datetime.datetime.now() - start_time))
    return Response(response=rt.to_json(orient="values"), status=200, mimetype="application/json")


@application.route('/forecast', methods=['GET', 'POST'])
def forecast():
    start_time = datetime.datetime.now()
    ai = aid()
    data, forecast, accy, accy1, point = ai.forcast(request.get_json())
    data['Date'] = data['Date'].astype('str')
    forecast['Date'] = forecast['Date'].astype('str')

    point['HOLT'] = accy1['HOLT']
    point['SARIMAX'] = accy1['SARIMAX']
    point['AUTOARMIA'] = accy1['AUTOARMIA']

    point['HOLT'] = point['HOLT'].map('{:,.2f}%'.format)
    point['SARIMAX'] = point['SARIMAX'].map('{:,.2f}%'.format)
    point['AUTOARMIA'] = point['AUTOARMIA'].map('{:,.2f}%'.format)
    point['minval'] = point['minval'].map('{:,d}'.format)
    point['maxval'] = point['maxval'].map('{:,d}'.format)
    point['mean'] = point['mean'].map('{:,.2f}'.format)
    point['std'] = point['std'].map('{:,.2f}'.format)
    point['val25'] = point['val25'].map('{:,.2f}'.format)
    point['val50'] = point['val50'].map('{:,.2f}'.format)
    point['val75'] = point['val75'].map('{:,.2f}'.format)

    t1 = pd.concat([data, forecast], ignore_index=True)
    t1['Date'] = t1['Date'].astype('datetime64').drop_duplicates()
    t1['Date'] = t1['Date'].astype('str').drop_duplicates()
    t1 = t1.loc[t1['Date'] != 'NaT']
    t1['h_Recipients'].loc[t1.index < (data['h_Recipients'].count() - 1)] = np.NaN
    t1['A_Recipients'].loc[t1.index < (data['A_Recipients'].count() - 1)] = np.NaN
    t1['S_Recipients'].loc[t1.index < (data['S_Recipients'].count() - 1)] = np.NaN

    t1['h_No_Originated'].loc[t1.index < (data['h_No_Originated'].count() - 1)] = np.NaN
    t1['A_No_Originated'].loc[t1.index < (data['A_No_Originated'].count() - 1)] = np.NaN
    t1['S_No_Originated'].loc[t1.index < (data['S_No_Originated'].count() - 1)] = np.NaN

    t1['h_No_Disbursements'].loc[t1.index < (data['h_No_Disbursements'].count() - 1)] = np.NaN
    t1['A_No_Disbursements'].loc[t1.index < (data['A_No_Disbursements'].count() - 1)] = np.NaN
    t1['S_No_Disbursements'].loc[t1.index < (data['S_No_Disbursements'].count() - 1)] = np.NaN

    t1['h_Value_Originated'].loc[t1.index < (data['h_Value_Originated'].count() - 1)] = np.NaN
    t1['A_Value_Originated'].loc[t1.index < (data['A_Value_Originated'].count() - 1)] = np.NaN
    t1['S_Value_Originated'].loc[t1.index < (data['S_Value_Originated'].count() - 1)] = np.NaN

    t1['h_Value_Disbursements'].loc[t1.index < (data['h_Value_Disbursements'].count() - 1)] = np.NaN
    t1['A_Value_Disbursements'].loc[t1.index < (data['A_Value_Disbursements'].count() - 1)] = np.NaN
    t1['S_Value_Disbursements'].loc[t1.index < (data['S_Value_Disbursements'].count() - 1)] = np.NaN

    resp = make_response(json.dumps(
        {'data': data.to_json(orient='values'), 'forecast': forecast.to_json(orient='values'),
         'accy': accy.to_json(orient='values'), 'accuracy': accy1.to_json(orient='values'),
         'point': point.to_json(orient='records'),
         'date': t1['Date'].to_json(orient='values'), 'd_Recipients': data['h_Recipients'].to_json(orient='values'),
         'd_No_Originated': data['h_No_Originated'].to_json(orient='values'),
         'd_Value_Originated': data['h_Value_Originated'].to_json(orient='values'),
         'd_No_Disbursements': data['h_No_Disbursements'].to_json(orient='values'),
         'd_Value_Disbursements': data['h_Value_Disbursements'].to_json(orient='values'),
         'd_H_No_Originated': t1['h_No_Originated'].to_json(orient='values'),
         'd_A_No_Originated': t1['A_No_Originated'].to_json(orient='values'),
         'd_S_No_Originated': t1['S_No_Originated'].to_json(orient='values'),
         'd_H_Recipients': t1['h_Recipients'].to_json(orient='values'),
         'd_A_Recipients': t1['A_Recipients'].to_json(orient='values'),
         'd_S_Recipients': t1['S_Recipients'].to_json(orient='values'),
         'd_H_No_Disbursements': t1['h_No_Disbursements'].to_json(orient='values'),
         'd_A_No_Disbursements': t1['A_No_Disbursements'].to_json(orient='values'),
         'd_S_No_Disbursements': t1['S_No_Disbursements'].to_json(orient='values'),
         'd_H_Value_Originated': t1['h_Value_Originated'].to_json(orient='values'),
         'd_A_Value_Originated': t1['A_Value_Originated'].to_json(orient='values'),
         'd_S_Value_Originated': t1['S_Value_Originated'].to_json(orient='values'),
         'd_H_Value_Disbursements': t1['h_Value_Disbursements'].to_json(orient='values'),
         'd_A_Value_Disbursements': t1['A_Value_Disbursements'].to_json(orient='values'),
         'd_S_Value_Disbursements': t1['S_Value_Disbursements'].to_json(orient='values'),

         }), 200)
    resp.mimetype = "application/json"
    logging.info('Duration: {}'.format(datetime.datetime.now() - start_time))
    return resp


@application.route('/aidYear')
def apiYear():
    start_time = datetime.datetime.now()
    ai = aid()
    data, forecast, accy, accy1, point = ai.apiYear()
    data['Date'] = data['Date'].astype('str')
    forecast['Date'] = forecast['Date'].astype('str')

    #data.to_csv("data.csv", index=False)
    #forecast.to_csv("forecast.csv", index=False)
    #accy.to_csv("accy.csv", index=False)
    #accy1.to_csv("accy1.csv", index=False)
    #point.to_csv("point.csv", index=False)
    #

    #data = pd.read_csv("data.csv")
    #forecast = pd.read_csv("forecast.csv")
    #accy = pd.read_csv("accy.csv")
    #accy1 = pd.read_csv("accy1.csv")
    #point = pd.read_csv("point.csv")

    point['HOLT'] = accy1['HOLT']
    point['SARIMAX'] = accy1['SARIMAX']
    point['AUTOARMIA'] = accy1['AUTOARMIA']

    point['HOLT'] = point['HOLT'].map('{:,.2f}%'.format)
    point['SARIMAX'] = point['SARIMAX'].map('{:,.2f}%'.format)
    point['AUTOARMIA'] = point['AUTOARMIA'].map('{:,.2f}%'.format)
    point['minval'] = point['minval'].map('{:,d}'.format)
    point['maxval'] = point['maxval'].map('{:,d}'.format)
    point['mean'] = point['mean'].map('{:,.2f}'.format)
    point['std'] = point['std'].map('{:,.2f}'.format)
    point['val25'] = point['val25'].map('{:,.2f}'.format)
    point['val50'] = point['val50'].map('{:,.2f}'.format)
    point['val75'] = point['val75'].map('{:,.2f}'.format)


    t1 = pd.concat([data,forecast], ignore_index=True)

    t1['Date'] = t1['Date'].astype('datetime64').drop_duplicates()
    t1['Date'] = t1['Date'].astype('str').drop_duplicates()
    t1 = t1.loc[t1['Date'] != 'NaT']

    t1['h_Recipients'].loc[t1.index < (data['h_Recipients'].count()-1)] = np.NaN
    t1['A_Recipients'].loc[t1.index < (data['A_Recipients'].count()-1)] = np.NaN
    t1['S_Recipients'].loc[t1.index < (data['S_Recipients'].count()-1)] = np.NaN

    t1['h_No_Originated'].loc[t1.index < (data['h_No_Originated'].count() - 1)] = np.NaN
    t1['A_No_Originated'].loc[t1.index < (data['A_No_Originated'].count() - 1)] = np.NaN
    t1['S_No_Originated'].loc[t1.index < (data['S_No_Originated'].count() - 1)] = np.NaN

    t1['h_No_Disbursements'].loc[t1.index < (data['h_No_Disbursements'].count() - 1)] = np.NaN
    t1['A_No_Disbursements'].loc[t1.index < (data['A_No_Disbursements'].count() - 1)] = np.NaN
    t1['S_No_Disbursements'].loc[t1.index < (data['S_No_Disbursements'].count() - 1)] = np.NaN

    t1['h_Value_Originated'].loc[t1.index < (data['h_Value_Originated'].count() - 1)] = np.NaN
    t1['A_Value_Originated'].loc[t1.index < (data['A_Value_Originated'].count() - 1)] = np.NaN
    t1['S_Value_Originated'].loc[t1.index < (data['S_Value_Originated'].count() - 1)] = np.NaN

    t1['h_Value_Disbursements'].loc[t1.index < (data['h_Value_Disbursements'].count() - 1)] = np.NaN
    t1['A_Value_Disbursements'].loc[t1.index < (data['A_Value_Disbursements'].count() - 1)] = np.NaN
    t1['S_Value_Disbursements'].loc[t1.index < (data['S_Value_Disbursements'].count() - 1)] = np.NaN


    resp = make_response(json.dumps(
        {'data': data.to_json(orient='values'), 'forecast': forecast.to_json(orient='values'),
         'accy': accy.to_json(orient='values'), 'accuracy': accy1.to_json(orient='values'), 'point': point.to_json(orient='records'),
         'date': t1['Date'].to_json(orient='values'), 'd_Recipients': data['h_Recipients'].to_json(orient='values'),
         'd_No_Originated': data['h_No_Originated'].to_json(orient='values'), 'd_Value_Originated': data['h_Value_Originated'].to_json(orient='values'),
         'd_No_Disbursements': data['h_No_Disbursements'].to_json(orient='values'), 'd_Value_Disbursements': data['h_Value_Disbursements'].to_json(orient='values'),
         'd_H_No_Originated': t1['h_No_Originated'].to_json(orient='values'),'d_A_No_Originated': t1['A_No_Originated'].to_json(orient='values'),'d_S_No_Originated': t1['S_No_Originated'].to_json(orient='values'),
         'd_H_Recipients': t1['h_Recipients'].to_json(orient='values'),'d_A_Recipients': t1['A_Recipients'].to_json(orient='values'),'d_S_Recipients': t1['S_Recipients'].to_json(orient='values'),
         'd_H_No_Disbursements': t1['h_No_Disbursements'].to_json(orient='values'),'d_A_No_Disbursements': t1['A_No_Disbursements'].to_json(orient='values'),'d_S_No_Disbursements': t1['S_No_Disbursements'].to_json(orient='values'),
         'd_H_Value_Originated': t1['h_Value_Originated'].to_json(orient='values'),'d_A_Value_Originated': t1['A_Value_Originated'].to_json(orient='values'),'d_S_Value_Originated': t1['S_Value_Originated'].to_json(orient='values'),
         'd_H_Value_Disbursements': t1['h_Value_Disbursements'].to_json(orient='values'),'d_A_Value_Disbursements': t1['A_Value_Disbursements'].to_json(orient='values'),'d_S_Value_Disbursements': t1['S_Value_Disbursements'].to_json(orient='values'),

         }), 200)
    resp.mimetype = "application/json"
    logging.info('Duration: {}'.format(datetime.datetime.now() - start_time))

    return resp


if __name__ == '__main__':
    with open('logger.yaml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)

    logging.info("Server Going to Start")
    application.run(debug=False, threaded=True)
    logging.info("Server Started")

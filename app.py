import pandas as pd

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask,jsonify

import datetime


###### Database Setup ########
engine = create_engine('sqlite:///Resources/hawaii.sqlite')

Base = automap_base()
Base.prepare(engine, reflect = True)
Station = Base.classes.station
Measurement = Base.classes.measurement 

app = Flask(__name__)

##### FLASK ROUTES ######
@app.route('/')
def home():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/start/<br/>"
        f"/api/v1.0/start/end/<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs\n"
        'for query dates, please provide in format: "yyyy-mm-dd"'
    )
    print('you have access to: \n')
    print('precipitation\nstations\ntobs')
    print('and search using start and end dates')
    print('for query dates, please provide in format: "yyyy-mm-dd"')

@app.route('/api/v1.0/precipitation/')
def precipitation():
    session = Session(engine)
    
    year_prcp = session.query(Measurement.date,Measurement.prcp).\
                order_by(Measurement.date.desc()).all()
    session.close()
    prcp_dict = {}
    for date, prcp in year_prcp:
        prcp_dict[date] = prcp
    return jsonify(prcp_dict)

@app.route('/api/v1.0/stations/')
def stations():
    session = Session(engine)

    list_stations = session.query(Station.station).all()
    session.close()
    x = 0
    for station in list_stations:
        list_stations[x] = list_stations[x][0]
        x += 1
    return jsonify(list_stations)

@app.route('/api/v1.0/tobs/')
def tobs():
    session = Session(engine)

    ########### Query for Most Frequent Station #########
    common_stations = pd.DataFrame(session.query(Measurement.station, Station.id))

    common_stations_count = common_stations['station'].value_counts()
    common_stations_count = pd.DataFrame(common_stations_count)
    common_stations_count.reset_index(inplace = True)
    top_station = common_stations_count.iloc[0]['index']

    ######## Station Query ########
    station_data = pd.DataFrame(session.query(Measurement.tobs, Measurement.date,Measurement.station))

    top_station_data = station_data.loc[station_data['station'] == top_station]

    session.close()

    return jsonify(top_station_data.to_dict())

@app.route('/api/v1.0/<start>/')
def only_start(start):
    session = Session(engine)
    start = start.split("-")

    ######### Query ########
    tobs_data = pd.DataFrame(session.query(Measurement.station, Measurement.tobs, Measurement.date))
    
    session.close()

    for x in range(0,len(start)):
        if start[x][0] == '0':
            start[x] = start[x].strip('0')


    tobs_data = tobs_data.loc[tobs_data['date'] > datetime.date(int(start[0]),int(start[1]),int(start[2])).strftime("%y-%m-%d")]
    tobs_data_describe = tobs_data.describe()
    tobs_data_min = tobs_data_describe.iloc[3]
    tobs_data_mean = tobs_data_describe.iloc[1]
    tobs_data_max = tobs_data_describe.iloc[-1]

    tobs_dict = {}
    tobs_dict['min'] = tobs_data_min
    tobs_dict['max'] = tobs_data_max
    tobs_dict['mean'] = tobs_data_mean

    tobs_dict = pd.DataFrame(tobs_dict)

    return jsonify(tobs_dict.to_dict())

@app.route('/api/v1.0/<start>/<end>/')
def start_and_end(start,end):
    session = Session(engine)
    start = start.split("-")
    end = end.split("-")

    ######### Query ########
    tobs_data = pd.DataFrame(session.query(Measurement.station, Measurement.tobs, Measurement.date))
    
    session.close()

    for x in range(0,len(start)):
        if start[x][0] == '0':
            start[x] = start[x].strip('0')
        if end[x][0] == '0':
            end[x] = end[x].strip('0')


    tobs_data = tobs_data.loc[(tobs_data['date'] > datetime.date(int(start[0]),int(start[1]),int(start[2])).strftime("%y-%m-%d")) & (tobs_data['date'] < datetime.date(int(end[0]),int(end[1]),int(end[2])).strftime("%y-%m-%d"))]
    tobs_data_describe = tobs_data.describe()
    tobs_data_min = tobs_data_describe.iloc[3]
    tobs_data_mean = tobs_data_describe.iloc[1]
    tobs_data_max = tobs_data_describe.iloc[-1]

    tobs_dict = {}
    tobs_dict['min'] = tobs_data_min
    tobs_dict['max'] = tobs_data_max
    tobs_dict['mean'] = tobs_data_mean

    tobs_dict = pd.DataFrame(tobs_dict)

    return jsonify(tobs_dict.to_dict())


if __name__ == '__main__':
    app.run(debug=True)
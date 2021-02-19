import json
from flask import Flask, render_template, request, redirect
from flask_sqlalchemy import SQLAlchemy
import pandas as pd
import numpy as np
from bokeh.models import ColumnDataSource, Div, Select, Slider, TextInput, HoverTool
from bokeh.resources import INLINE
from bokeh.embed import components
from bokeh.plotting import figure, output_file
from bokeh.palettes import Greys6, Set1
#from bokeh.transform import factor_cmap
from datetime import datetime, timedelta
from forecast_position import simple_forecast, advanced_forecast
from data_fetch import fetch_by_buoyid
from utils import format_timedelta

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///database.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)


class Buoy(db.Model):
    # Database for storing buoy data
    # Data from multiple buoys can be stored by specifying buoy column
    id = db.Column(db.Integer, primary_key=True)
    buoy = db.Column(db.Text)  # should match ID in active_buoys.json
    lat = db.Column(db.Float)
    lon = db.Column(db.Float)
    date = db.Column(db.DateTime)

    def __repr__(self):
        return '<Name %r>' % self.id


class Forecast(db.Model):
    # Database for storing forecasted positins
    id = db.Column(db.Integer, primary_key=True)
    lat = db.Column(db.Float)
    lon = db.Column(db.Float)
    date = db.Column(db.DateTime)
    method = db.Column(db.Text(5))  # Either simple ('s') or advanced ('a')

    def __repr__(self):
        return '<Name %r>' % self.id


class Variables(db.Model):
    # Database to store misc persistant date variables
    #   e.g. time since last update
    id = db.Column(db.Integer, primary_key=True)
    key_string = db.Column(db.Text)
    value = db.Column(db.DateTime)
    value_txt = db.Column(db.Text)

    def __repr__(self):
        return '<Name %r>' % self.id


@app.template_filter()
def timezone_conversion(value, tz='akst', format='%Y-%m-%d %H:%M'):
    if tz == 'akst':
        return (value - timedelta(hours=9)).strftime(format)
    elif tz == 'utc':
        return value.strftime(format)
    else:
        return value.strftime(format)


@app.before_first_request
def init_tables():
    default_date = datetime(year=1900, month=1, day=1)
    exists = Variables.query.filter_by(key_string="last_update").first()
    if not exists:
        new_row = Variables(key_string="last_update", value=default_date)
        db.session.add(new_row)
    # Last update of simple forecast
    exists = Variables.query.filter_by(key_string="s_update").first()
    if not exists:
        new_row = Variables(key_string="s_update", value=default_date)
        db.session.add(new_row)
    # Last update of advanced forecast
    exists = Variables.query.filter_by(key_string="a_update").first()
    if not exists:
        new_row = Variables(key_string="a_update", value=default_date)
        db.session.add(new_row)
    # Filename of stored topaz data
    exists = Variables.query.filter_by(key_string="topaz_start").first()
    if not exists:
        new_row = Variables(key_string="topaz_start", value=default_date)
        db.session.add(new_row)
        new_row = Variables(key_string="topaz_end", value=default_date)
        db.session.add(new_row)
        new_row = Variables(key_string="topaz_update", value=default_date)
        db.session.add(new_row)
    exists = Variables.query.filter_by(key_string="primary_buoy").first()
    if not exists:
        new_primary = Variables(key_string="primary_buoy", value_txt="443910")
        db.session.add(new_primary)
    
    db.session.commit()


@app.route("/")
@app.route("/overview")
def overview():
    current_time = datetime.utcnow()

    # Update the buoy data record
    update_record(current_time)

    last_update = Variables.query.filter_by(key_string="last_update").first()
    time_since_update = current_time - last_update.value

    time_since_update = format_timedelta(time_since_update)

    # last_known_point = Buoy.query.order_by(Buoy.date.desc()).limit(1)[0]
    last_known_point = query_buoy_data(n_pts=1)

    obs_age = current_time - last_known_point.date
    obs_age = format_timedelta(obs_age, show_seconds=False)

    p = make_plot()

    script, div = components(p)
    return render_template(
        'overview.html',
        current_time=current_time,
        last_sync=time_since_update,
        lkp=last_known_point,
        obs_age=obs_age,
        plot_script=script,
        plot_div=div,
        js_resources=INLINE.render_js(),
        css_resources=INLINE.render_css(),
    ).encode(encoding='UTF-8')


@app.route("/pilot")
def pilot():
    current_time = datetime.utcnow()

    # Update the buoy data record
    update_record(current_time)

    last_fc_update = Variables.query.filter_by(key_string="s_update").first()

    # Update the forecast if its age is greater than the last data sync
    if current_time - last_fc_update.value > timedelta(hours=.1):
        update_forecast(forecast_method='s')

    # last_known_point = Buoy.query.order_by(Buoy.date.desc()).limit(1)[0]
    last_known_point = query_buoy_data(n_pts=1)
    fc_pos = Forecast.query.filter_by(method='s').order_by(Forecast.date)

    p = make_plot(forecast='s', size=(600, 600), record='partial')

    script, div = components(p)
    return render_template(
            'pilot.html',
            fc_points=fc_pos,
            lkp=last_known_point,
            plot_script=script,
            plot_div=div,
            js_resources=INLINE.render_js(),
            css_resources=INLINE.render_css(),
        ).encode(encoding='UTF-8')


@app.route("/satellite", methods=['POST', 'GET'])
def satellite():
    current_time = datetime.utcnow()

    # Update the buoy data record
    update_record(current_time)

    # Update the forecast on user request if its age is greater than the last data sync
    if request.method == "POST":
        last_fc_update = Variables.query.filter_by(key_string="a_update").first()
        if current_time - last_fc_update.value > timedelta(hours=0.25):
            update_forecast(forecast_method='a')

    # Calculate the time since the forecast was run
    last_fc_update = Variables.query.filter_by(key_string="a_update").first()
    fc_age = datetime.utcnow() - last_fc_update.value
    fc_age = format_timedelta(fc_age, show_seconds=False)

    # Calculate the time since the topaz variables were downloaded
    last_topaz_update = Variables.query.filter_by(key_string="topaz_update").first()
    topaz_age = datetime.utcnow() - last_topaz_update.value
    topaz_age = format_timedelta(topaz_age, show_seconds=False)

    #last_known_point = Buoy.query.order_by(Buoy.date.desc()).limit(1)[0]
    last_known_point = query_buoy_data(n_pts=1)

    fc_pos = Forecast.query.filter_by(method='a').order_by(Forecast.date)

    active_buoy = Variables.query.filter_by(key_string='primary_buoy').first().value_txt

    p = make_plot(forecast='a', size=(600, 600), record='full')

    script, div = components(p)
    return render_template(
            'satellite.html',
            current_time=current_time,
            fc_points=fc_pos,
            fc_age=fc_age,
            topaz_age=topaz_age,
            lkp=last_known_point,
            active_buoy=active_buoy,
            plot_script=script,
            plot_div=div,
            js_resources=INLINE.render_js(),
            css_resources=INLINE.render_css(),
        ).encode(encoding='UTF-8')


def query_buoy_data(n_pts=1):
    '''
    Wrapper around Buoy.query to always query for the primary buoy
    Limits query to most recent n_pts
    '''
    primary_buoy = Variables.query.filter_by(key_string="primary_buoy").first().value_txt
    # If only 1 point was requested, return as an item instead of a list with len=1
    if n_pts == 1:
        return Buoy.query.filter_by(buoy=primary_buoy).order_by(Buoy.date.desc()).limit(n_pts)[0]
    else:
        return Buoy.query.filter_by(buoy=primary_buoy).order_by(Buoy.date.desc()).limit(n_pts)


def update_primary(new_primary):
    '''
    Updates the primary buoy for tracking / forecasting
    This method forces the update of the forecasts
    :new_primary: ID of new buoy to track. Must be a valid ID in active_buoys.json
    '''
    # Find the existing value, then set it to the new value
    primary_buoy = Variables.query.filter_by(key_string="primary_buoy").first()
    primary_buoy.value_txt = new_primary

    # Commit the change to the database
    db.session.commit()

    # Force update both methods to run from the new primary buoy
    update_forecast(forecast_method='s')
    update_forecast(forecast_method='a')


def update_record(current_time, throttle=15):
    '''
    :current_time: datetime object
    :throttle: Minimum time elapsed, in minutes, before new data is downloaded
    '''
    last_update = Variables.query.filter_by(key_string="last_update").first()
    time_since_update = current_time - last_update.value

    # Return without doing anything if not enough time has elapsed
    if time_since_update < timedelta(minutes=throttle):
        return
   
    # Read in each active buoy
    with open("static/active_buoys.json", 'r') as fhandle:
        active_buoys = json.load(fhandle)

    # Update the database with new data for each active buoy
    for buoy in active_buoys:
        update_bouy(current_time, time_since_update, buoy)

    last_known_point = query_buoy_data()
    obs_age = current_time - last_known_point.date

    # Set a new primary buoy if the current one has not reported in >5 hours
    if obs_age > timedelta(hours=5):
        # Set the age threshold to be beaten
        primary_age = obs_age
        # Set the current primary buoy
        primary_buoy = Variables.query.filter_by(key_string="primary_buoy").first().value
        # Search for a bouy that has the lowest obs_age
        for buoy in active_buoys:
            last_known_point = Buoy.query.filter_by(buoy=buoy).order_by(Buoy.date.desc()).limit(1)[0]
            obs_age = current_time - last_known_point.date
            if obs_age < primary_age:
                primary_age = obs_age
                primary_buoy = buoy
        
        update_primary(primary_buoy)

    # Update the last update time in the Variables db
    last_update.value = current_time
    db.session.commit()


def update_bouy(current_time, time_since_update, buoy_id):
    """
    Updates the database with new buoy positions for a single target.
    """
    # The number of positions requested is
    #   the number of hours since the last update + 2
    n_pos = int(time_since_update.total_seconds()/(3600)) + 2
    if n_pos > 200:
        n_pos = 200

    # Download the most recent data from the buoy
    new_points = fetch_by_buoyid(buoy_id, n_pos=n_pos + 2)

    # Add any buoy points to the DB if they do not already exist
    for pos_time, lat, lon in new_points:
        exists = Buoy.query.filter_by(date=pos_time, buoy=buoy_id).first()
        if not exists:
            new_point_entry = Buoy(buoy=buoy_id, date=pos_time, lat=lat, lon=lon)
            db.session.add(new_point_entry)

    # Commit all changes to the db
    db.session.commit()


def update_forecast(forecast_method='s'):

    if forecast_method == 's':
        # Select the last 8 buoy points
        n_pts = 8
        drift_track = [(0, 0, 0) for _ in range(n_pts)]
        
        points = query_buoy_data(n_pts=n_pts)
        # points = Buoy.query.order_by(Buoy.date.desc()).limit(n_pts)

        # Need place these in drift track in reverse order
        i = n_pts-1
        for point in points:
            drift_track[i] = (point.date, point.lat, point.lon)
            i -= 1

        init_time = datetime.utcnow()
        forecast_position = simple_forecast(init_time+timedelta(hours=6), drift_track, full_forecast=True)
    
    elif forecast_method == 'a':
        topaz_start_entry = Variables.query.filter_by(key_string="topaz_start").first()
        topaz_end_entry = Variables.query.filter_by(key_string="topaz_end").first()

        #lkp = Buoy.query.order_by(Buoy.date.desc()).limit(1)[0]
        lkp = query_buoy_data(n_pts=1)

        #lkp = Buoy.query.order_by(Buoy.date.desc()).limit(25)[24]
        last_known_point = (lkp.date, lkp.lat, lkp.lon)

        # Check the age of the existing model data
        last_topaz_update = Variables.query.filter_by(key_string="topaz_update").first()
        topaz_age = datetime.utcnow() - last_topaz_update.value

        [topaz_update, topaz_start, 
        topaz_end, forecast_position] = advanced_forecast(last_known_point,
                                                          lkp.date+timedelta(hours=96),
                                                          topaz_age,
                                                          topaz_start_entry.value,
                                                          topaz_end_entry.value,
                                                          full_forecast=True)

        if topaz_update:
            tu = Variables.query.filter_by(key_string="topaz_update").first()
            tu.value = datetime.utcnow()
        topaz_start_entry.value = topaz_start
        topaz_end_entry.value = topaz_end
        db.session.commit()
        
    else:
        return

    # Delete the existing records for this forecast method
    db.session.query(Forecast).filter(Forecast.method == forecast_method).delete()
    db.session.commit()

    # Add the new forecast data to the database
    for pos_time, lat, lon in forecast_position:
        new_point_entry = Forecast(date=pos_time, lat=lat, lon=lon, 
                                   method=forecast_method)
        try:
            db.session.add(new_point_entry)
        except:
            pass

    # Update the last forecast update value
    last_update = Variables.query.filter_by(key_string='{}_update'.format(forecast_method)).first()
    last_update.value = datetime.utcnow()
    db.session.commit()
    # Set time since update to 0
    #time_since_update = timedelta(hours=0)
    #return time_since_update



def make_plot(forecast=None, size=(800, 800), record='full'):
    
    buoy_id = Variables.query.filter_by(key_string="primary_buoy").first().value_txt
    if record == 'partial':
        drift_history = pd.read_sql("select * from buoy where buoy='{}'".format(buoy_id), db.session.bind)
        #drift_history = pd.read_sql("buoy", db.session.bind)
        last_idx = drift_history.last_valid_index()
        drift_history.sort_values(by='date', inplace=True)
        drift_history = drift_history.iloc[last_idx-20:]
    else:
        drift_history = pd.read_sql("select * from buoy where buoy='{}'".format(buoy_id), db.session.bind)
        #drift_history = pd.read_sql("buoy", db.session.bind)

    # If requested to show the forecast, read that data and plot it in red.
    # Otherwise just plot the latest point in red. 
    if forecast is not None:
        drift_forecast = pd.read_sql("select * from forecast where method='{}'".format(forecast), db.session.bind)
        lat_fc = drift_forecast.lat
        lon_fc = drift_forecast.lon
        date_fc = drift_forecast.date
        method_fc = drift_forecast.method
        forecast_legend = 'Forecast Track'
        color = Set1[3][1]
    else:
        index = drift_history.id.idxmax()
        lat_fc = [drift_history.lat[index]]
        lon_fc = [drift_history.lon[index]]
        date_fc = [drift_history.date[index]]
        method_fc = ['s']   # placeholder value
        forecast_legend = 'Last Known'
        color = 'green'

    ht = HoverTool(tooltips=[("time", "@date{%F %H:%M}"),
                             ("(lat, lon)", "(@y, @x)")],
                   formatters={'@date': 'datetime'})

    data_source = ColumnDataSource(data=dict(
                                   x=drift_history.lon,
                                   y=drift_history.lat,
                                   date=drift_history.date
                                   ))

    data_source_forecast = ColumnDataSource(data=dict(
                                            x=lon_fc,
                                            y=lat_fc,
                                            date=date_fc,
                                            method=method_fc
                                            ))

    p = figure(title="Drift History", sizing_mode="fixed", 
               plot_width=size[0], plot_height=size[1], tools=["pan,wheel_zoom,box_zoom,reset", ht])
    p.xaxis.axis_label = "Longitude"
    p.yaxis.axis_label = "Latitude"
    p.circle('x', 'y', source=data_source, color='black', fill_alpha=0.4, size=10, legend_label='Drift History')
    p.circle('x', 'y', source=data_source_forecast, color=color, fill_alpha=0.8, size=10, legend_label=forecast_legend)
    return p

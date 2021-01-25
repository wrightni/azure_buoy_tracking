from flask import Flask, render_template, request, redirect
from flask_sqlalchemy import SQLAlchemy
import json
import pandas as pd
import numpy as np
from bokeh.models import ColumnDataSource, Div, Select, Slider, TextInput, HoverTool
from bokeh.io import curdoc
from bokeh.resources import INLINE
from bokeh.embed import components
from bokeh.plotting import figure, output_file, show
from bokeh.palettes import Greys6
from bokeh.transform import linear_cmap

from datetime import datetime, timedelta
from forecast_position import simple_forecast
from data_fetch import fetch_by_buoyid
from utils import format_timedelta

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///database.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

# Create db model
class Buoy(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    lat = db.Column(db.Float)
    lon = db.Column(db.Float)
    date = db.Column(db.DateTime)

    def __repr__(self):
        return '<Name %r>' % self.id

# Create db model
class Forecast(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    lat = db.Column(db.Float)
    lon = db.Column(db.Float)
    date = db.Column(db.DateTime)
    method = db.Column(db.Text(5)) # Either simple ('s') or advanced ('a')

    def __repr__(self):
        return '<Name %r>' % self.id

# Create db to store last update: 
#   Could be extended to store misc persistant variables
class Variables(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    key_string = db.Column(db.Text)
    value = db.Column(db.DateTime)

    def __repr__(self):
        return '<Name %r>' % self.id


@app.before_first_request
def init_tables():
    default_date = datetime(year=1900, month=1, day=1)
    exists = Variables.query.filter_by(key_string="last_update").first()
    if not exists:
        new_row = Variables(key_string="last_update", value=default_date)
        db.session.add(new_row)
        db.session.commit()

    exists = Variables.query.filter_by(key_string="s_update").first()
    if not exists:
        new_row = Variables(key_string="s_update", value=default_date)
        db.session.add(new_row)
        db.session.commit()


@app.route("/")
@app.route("/overview")
def overview():
    current_time = datetime.utcnow()

    last_update = Variables.query.filter_by(key_string="last_update").first()
    time_since_update = current_time - last_update.value

    if time_since_update > timedelta(minutes=15):
        time_since_update = update_record(current_time, last_update)

    time_since_update = format_timedelta(time_since_update)

    last_known_point = Buoy.query.order_by(Buoy.date.desc()).limit(1)[0]

    obs_age = current_time - last_known_point.date
    obs_age = format_timedelta(obs_age, show_seconds=False)

    p = make_plot()
    
    script, div = components(p)
    return render_template(
        'overview.html',
        current_time=current_time.strftime("%Y-%m-%d %H:%M:%S"),
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
    last_fc_update = Variables.query.filter_by(key_string="s_update").first()

    # Update the forecast if its age is greater than the last data sync
    if current_time - last_fc_update.value > timedelta(hours=1):
        update_forecast(last_fc_update)

    last_known_point = Buoy.query.order_by(Buoy.date.desc()).limit(1)[0]
    fc_pos = Forecast.query.order_by(Forecast.date).limit(5)

    p = make_plot(forecast=True, size=(600,600), record='partial')

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
    if request.method == "POST":
        return "Thanks for clicking the button."
    else:

        last_fc_update = Variables.query.filter_by(key_string="s_update").first()

        # Update the forecast if its age is greater than the last data sync
        if current_time - last_fc_update.value > timedelta(hours=1):
            update_forecast(last_fc_update)

        last_known_point = Buoy.query.order_by(Buoy.date.desc()).limit(1)[0]
        fc_pos = Forecast.query.order_by(Forecast.date)

        p = make_plot(forecast=True, size=(600,600), record='partial')
    
        script, div = components(p)
        return render_template(
                'satellite.html',
                fc_points=fc_pos,
                lkp=last_known_point,
                plot_script=script,
                plot_div=div,
                js_resources=INLINE.render_js(),
                css_resources=INLINE.render_css(),
            ).encode(encoding='UTF-8')



def update_record(current_time, last_update):
    """
    Updates the database with new buoy positions.
    Also updates the time_si
    """

    # The number of positions requested is the number of hours since the last update + 2
    time_since_update = current_time - last_update.value
    n_pos = int(time_since_update.total_seconds()/(3600)) + 2
    print(n_pos)
    # Download the most recent data from the buoy
    new_points = fetch_by_buoyid("443910", n_pos=n_pos+2)
    print(len(new_points))
    # Add any buoy points to the DB if they do not already exist
    for pos_time, lat, lon in new_points:
        exists = Buoy.query.filter_by(date=pos_time).first()
        if not exists:
            new_point_entry = Buoy(date=pos_time, lat=lat, lon=lon)
            print("New point added")
            try:
                db.session.add(new_point_entry)
            except:
                pass
    try:
        # Update the last update time in the Variables db
        last_update.value = current_time
        # Commit all changes to the db
        db.session.commit()
        # Set time since update to 0
        time_since_update = timedelta(hours=0)
        return time_since_update
    except:
        # Return last update as given if this update fails
        return last_update
    

def update_forecast(last_update, forecast_method='s'):

    # Select the last 8 buoy points
    drift_track = [(0,0,0) for _ in range(8)]
    points = Buoy.query.order_by(Buoy.date.desc()).limit(8)

    # Need place these in drift track in reverse order
    i=7
    for point in points:
        drift_track[i] = (point.date, point.lat, point.lon)
        i-=1

    init_time = datetime.utcnow()
    forecast_position = simple_forecast(init_time+timedelta(hours=24), drift_track, full_forecast=True)

    # Delete the existing records for this forecast method
    try:
        db.session.query(Forecast).filter(Forecast.method == 's').delete()
        db.session.commit()
    except:
        pass

    # Add the new forecast data to the database
    for pos_time, lat, lon in forecast_position:
        new_point_entry = Forecast(date=pos_time, lat=lat, lon=lon, 
                                   method=forecast_method)
        try:
            db.session.add(new_point_entry)
        except:
            pass
    try:
        last_update.value = datetime.utcnow()
        db.session.commit()
        # Set time since update to 0
        time_since_update = timedelta(hours=0)
        return time_since_update
    except:
        pass


def make_plot(forecast=False, size=(800, 800), record='full'):

    if record == 'partial':
        drift_history = pd.read_sql("buoy", db.session.bind)
        last_idx = drift_history.last_valid_index()
        drift_history.sort_values(by='date', inplace=True)
        drift_history = drift_history.iloc[last_idx-10:]
    else:
        drift_history = pd.read_sql("buoy", db.session.bind)

    # If requested to show the forecast, read that data and plot it in red.
    # Otherwise just plot the latest point in red. 
    if forecast:
        drift_forecast = pd.read_sql("forecast", db.session.bind)
        lat_fc = drift_forecast.lat
        lon_fc = drift_forecast.lon
        date_fc = drift_forecast.date
        forecast_legend = 'Forecast Track'
    else:
        index = drift_history.date.idxmax()
        lat_fc = [drift_history.lat[index]]
        lon_fc = [drift_history.lon[index]]
        date_fc = [drift_history.date[index]]
        forecast_legend = 'Last Known'
    
    ht = HoverTool(tooltips=[("time", "@date{%F %H:%M}"),
                             ("(lat, lon)", "(@y, @x)")],
                   formatters={'@date': 'datetime'})

    data_source = ColumnDataSource(data=dict(
                                   x = drift_history.lon,
                                   y = drift_history.lat,
                                   date=drift_history.date
                                   ))

    data_source_forecast = ColumnDataSource(data=dict(
                                            x=lon_fc,
                                            y=lat_fc,
                                            date=date_fc
                                            ))

    p = figure(title = "Drift History", sizing_mode="fixed", 
               plot_width=size[0], plot_height=size[1], tools=["pan,wheel_zoom,box_zoom,reset", ht])
    p.xaxis.axis_label = "Longitude"
    p.yaxis.axis_label = "Latitude"
    p.circle('x', 'y', source=data_source, color='black', fill_alpha=0.4, size=10, legend_label='Drift History')
    p.circle('x', 'y', source=data_source_forecast, color='green', fill_alpha=0.8, size=10, legend_label=forecast_legend)
    return p
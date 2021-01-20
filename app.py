from flask import Flask, render_template, request, redirect
from flask_sqlalchemy import SQLAlchemy
import json
import pandas as pd
from bokeh.models import ColumnDataSource, Div, Select, Slider, TextInput
from bokeh.io import curdoc
from bokeh.resources import INLINE
from bokeh.embed import components
from bokeh.plotting import figure, output_file, show
from bokeh.sampledata.iris import flowers

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
@app.route("/hourly")
def hourly():

    current_time = datetime.utcnow()

    last_update = Variables.query.filter_by(key_string="last_update").first()
    time_since_update = current_time - last_update.value

    if time_since_update > timedelta(minutes=15):
        time_since_update = update_record(last_update)

    last_fc_update = Variables.query.filter_by(key_string="s_update").first()

    # Update the forecast if its age is greater than the last data sync
    if current_time - last_fc_update.value > time_since_update:
        update_forecast(last_fc_update)

    time_since_update = format_timedelta(time_since_update)

    points = Buoy.query.order_by(Buoy.date.desc()).limit(5)
    fc_pos = Forecast.query.order_by(Forecast.date.desc())

    obs_age = current_time - points[0].date
    obs_age = format_timedelta(obs_age)

    return render_template("hourly.html", points=points, fc_points=fc_pos, current_time=current_time.strftime("%Y-%m-%d %H:%M:%S"), 
                           last_update=time_since_update, last_known_pos=obs_age)


@app.route("/custom", methods=['POST', 'GET'])
def custom():
    if request.method == "POST":
        return render_template("custom.html")
    else:
        return render_template("custom.html")


@app.route("/plot")
def plot():
    p = make_plot()
    
    script, div = components(p)
    return render_template(
        'plot.html',
        plot_script=script,
        plot_div=div,
        js_resources=INLINE.render_js(),
        css_resources=INLINE.render_css(),
    ).encode(encoding='UTF-8')


def update_record(last_update):
    """
    Updates the database with new buoy positions.
    Also updates the time_si
    """

    # Download the most recent data from the buoy
    new_points = fetch_by_buoyid("443910", n_pos=5)

    # Add any buoy points to the DB if they do not already exist
    for pos_time, lat, lon in new_points:
        exists = Buoy.query.filter_by(date=pos_time).first()
        if not exists:
            new_point_entry = Buoy(date=pos_time, lat=lat, lon=lon)
            try:
                db.session.add(new_point_entry)
            except:
                pass
    try:
        # Update the last update time in the Variables db
        last_update.value = datetime.utcnow()
        # Commit all changes to the db
        db.session.commit()
        # Set time since update to 0
        time_since_update = timedelta(hours=0)
        return time_since_update
    except:
        pass
    

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


def make_plot():

    # colormap = {'setosa': 'red', 'versicolor': 'green', 'virginica': 'blue'}

    drift_history = pd.read_sql("buoy", db.session.bind)
    drift_forecast = pd.read_sql("forecast", db.session.bind)

    p = figure(title = "Drift Track", sizing_mode="fixed", plot_width=800, plot_height=800)
    p.xaxis.axis_label = "Latitude"
    p.yaxis.axis_label = "Longitude"
    p.circle(drift_history.lat, drift_history.lon, color='green', fill_alpha=0.5, size=10)
    p.circle(drift_forecast.lat, drift_forecast.lon, color='red', fill_alpha=0.5, size=10)
    return p
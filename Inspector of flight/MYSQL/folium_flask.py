
import requests
from flask import Flask
import time
import folium
from folium import plugins
import time

from flask import render_template

#from app import app

# @app.route('/')
# def index():
#     return render_template("index.html")

app = Flask(__name__)
url = "https://opensky-network.org/api/states/all?lamin=55&lomin=25&lamax=65&lomax=35"

@app.route('/')
def index():
    m = folium.Map(location=[59, 30], control_scale=True, zoom_start=8, tiles="Stamen Terrain")
    for i in range(1):
        #time.sleep(10)
        tooltip = "Click me!"
        resp = requests.get(url)
        result = resp.json()
        for flight in result['states']:
            label = f'<i>{flight[1]} h={flight[7]} v={round(flight[9] * 3.6, 0)}</i>'
            folium.Marker([flight[6], flight[5]], popup=label, tooltip=tooltip).add_to(m)
            #folium.ColorLine()

    #start_coords = (46.9540700, 142.7360300)
    #folium_map = folium.Map(location=start_coords, zoom_start=14)
    return m._repr_html_()

if __name__ == '__main__':
    app.run(debug=True)
    for i in range(100):
        index()
        time.sleep(10)
        print(i)

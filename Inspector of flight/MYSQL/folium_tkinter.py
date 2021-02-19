from tkinter import *
#import map
import folium
import os
import requests
import io
from PIL import Image, ImageTk
from folium import plugins

lon = 0 #55.7530 # 59
lat = 0 #37.6215 # 30
dlt = 179

url = "https://opensky-network.org/api/states/all?lamin="+str(lon-dlt)+"&lomin="+str(lat-1*dlt)+"&lamax="+str(lon+dlt)+"&lomax="+str(lat+1*dlt)


def start(zoom):
    try:
        map = folium.Map(location=[lon, lat], control_scale=True, zoom_start=zoom, tiles="Stamen Terrain")  # 59 ,30
        plugins.LocateControl().add_to(map)
        map.add_child(folium.LatLngPopup())
        url_rose = 'https://raw.githubusercontent.com/SECOORA/static_assets/master/maps/img/rose.png'
        szt = plugins.FloatImage(url_rose, bottom=60, left=70)
        map.add_child(szt)
        tooltip = "Click me!"
        for i in range(1):
            resp = requests.get(url)
            result = resp.json()
            print(len(result['states']))
            k=0
            for flight in result['states']:
                try:
                    label = f'<i>{flight[1]} h={flight[7]} v={round(flight[9] * 3.6, 0)}</i>'
                    #folium.Marker([flight[6], flight[5]], popup=label, tooltip=tooltip).add_to(map)
                    col = '#ff0'
                    if flight[7] < 1000 : col = '#f06'
                    if flight[7] < 200: col = '#606'
                    if flight[7] > 5000: col = '#3f6'
                    plugins.BoatMarker(
                        location=(flight[6], flight[5]),
                        heading=flight[10],
                        speed= round(flight[9]*3.6,0),
                        wind_heading=flight[10],
                        wind_speed=round(flight[9]*3.6,0)/10,
                        color=col,
                        popup=label
                    ).add_to(map)
                except Exception as e:
                    #print(e)
                    #print(flight)
                    k+=1
                #folium.ColorLine()
        #map.add_child(folium.LatLngPopup()) # показывает координаты точки
        img_data = map._to_png(1)
        img = Image.open(io.BytesIO(img_data))
        img.save('image.png')
        map.save("map.html")
        print('Save')
        print(k)
    except Exception as e:
        print(e)

class MainApp(Frame):#object):#Frame):
    zoom = 6

    def __init__(self, gui, parent=None,**kw):
        Frame.__init__(self,  master=parent, **kw)
        self.fl = 0
        self.main_frame(gui)
        self.zoom = 6

    def main_frame(self, gui):
        gui.title("Test flight map")
        gui.config(background="black")
        mainmenu = Menu(gui)
        gui.config(menu=mainmenu)

        filemenu = Menu(mainmenu, tearoff=0)
        filemenu.add_command(label="Exit", command=self.exit_app)

        mainmenu.add_cascade(label="File", menu=filemenu)
        # Graphics window
        self.Frame_1a = Frame(gui, width=640, height=5, bg="black")
        self.Frame_1b = Frame(gui, width=640, height=20, bg="black")
        self.Frame_1 = Frame(gui, width=640, height=360, bg="black")

        self.Frame_1a.grid(row=0, column=0)
        self.Frame_1b.grid(row=1, column=0)
        self.Frame_1.grid(row=2, column=0) #, columnspan=2)

        self.btn = Button(self.Frame_1b, text='Start!', bg="#555", fg="#ccc", command=self.click_button)
        self.btn.grid(row=0, column=1) # sticky=N+S+W+E)
        self.btn2 = Button(self.Frame_1b, text='ZIN', bg="#555", fg="#ccc", command=self.z_in)
        self.btn2.grid(row=0, column=5) # sticky=N+S+W+E)
        self.btn2 = Button(self.Frame_1b, text='ZOUT', bg="#555", fg="#ccc", command=self.z_out)
        self.btn2.grid(row=0, column=3) # sticky=N+S+W+E)

        self.lmain = Label(self.Frame_1)
        self.lmain.grid(row=0, column=0)
        self.lmain.bind("<Enter>")  # , self.enter_lmain)
        self.lmain.bind("<Leave>")  # , self.leave_lmain)
        self.lmain.after(10000, self.show_new_map())

    def show_new_map(self):
        self.img =  ImageTk.PhotoImage(image=Image.open('image.png').resize((640, 360), Image.ANTIALIAS))
        self.lmain.configure(image=self.img)
        print('Show_new_map', self.fl, self.zoom)
        if self.fl == 1:
            start(self.zoom)
        self.lmain.after(10000, self.show_new_map)

    def exit_app(self):
        sys.exit()

    def click_button(self):
        print('Start', self.zoom)
        if self.fl == 0:
            start(self.zoom)
            os.startfile("map.html")
            self.fl = 1
        else:
            start()

    def z_in(self):
        self.zoom = self.zoom + 1
        print('Zoom', self.zoom)

    def z_out(self):
        self.zoom = self.zoom - 1
        print('Zoom', self.zoom)





if __name__ == '__main__':
    gui = Tk()
    gui.geometry("650x400")
    app = MainApp(gui)
    gui.mainloop()

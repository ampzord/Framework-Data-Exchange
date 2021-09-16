FOR /L %%A IN (1,1,10) DO (
  start py.exe "C:\Users\work\Documents\FEUP\MQTT_Python\client.py" %%A
)
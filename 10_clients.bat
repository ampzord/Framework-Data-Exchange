FOR /L %%A IN (1,1,10) DO (
  start py.exe "C:\Users\work\Documents\FEUP\MQTT_Project\MQTT_python\client.py" %%A
)
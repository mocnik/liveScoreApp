# LiveScoreApp

## Instal dependacies

```
pip install -r requirements.txt
```

## Sh.Radio controller

http://radio.jsh.de/jsh.radio_controller.zip

Change WebClient settings at the bottom of `jSh_Radio.ini` with:
```
URL=http://127.0.0.1:8000/punch
Header=Content-Type: application/json
Body={"chipNumber": %ChipNr%, "time": "%PunchTime%", "stationCode": %CodeNr%}
```

## OEvent

* Required software can be downloaded from: http://www.oevent.org/Downloads.aspx
* Firebird database software needs to be installed as `Super Classic Server Binary`, more on this in OEvent help.
* Based on location of served and `gdb` files put together connection string and fix it in `app.py`. Example of valid connection string:
```
DB_CONNECTION_STRING='localhost:C:\\Users\\<user>\\AppData\\Roaming\\OEvent\\Data\\Competition1.gdb'
```

## Test
 * Start `run.bat`
 * Open `templates/index.html`
 * Open `jsh_Radio`, connect WebClient and send test punch

## Available endpoints
* http://localhost:8000/competition
* http://localhost:8000/categories
* http://localhost:8000/category/CATEGORY/runners
* http://localhost:8000/runner/START_NUMBER

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

## Test
 * Start `run.bat`
 * Open `templates/index.html`
 * Open `jsh_Radio`, connect WebClient and send test punch

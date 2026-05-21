# Fluidra Blue Connect to MQTT

Home Assistant add-on that reads Blue Connect (Fluidra Connect) water
measurements and publishes them to MQTT with auto-discovery.

Sensors created (device family "Data collectors", Blue Connect v3, salt
pool by default):

- pH (component 13)
- Water temperature, °C (component 12)
- ORP / redox, mV (component 14)
- Salinity, g/L (component 16)
- Status (`ok` / `degraded` / `error`)
- Last bridge update (timestamp of the last successful API call)
- Last sensor reading (timestamp from the probe itself, diagnostic)

## Why this bridge

The HACS integration `foXaCe/Fluidra-pool` retrieves these components
but does not expose them as entities for devices belonging to the
"Data collectors" family (Blue Connect). This bridge queries the
component endpoint of the Fluidra Connect API directly and publishes
the result over MQTT.

## How it works

- Authenticates against AWS Cognito with your Fluidra Pool credentials
  (the same ones you use in the Fluidra Pool mobile app).
- Reads the configured components from the Fluidra EMEA API every
  `poll_interval_seconds`.
- Publishes each measurement on its own MQTT topic and announces them
  via Home Assistant MQTT discovery so they appear as native sensors.
- Reads the per-component `ts` field (Unix epoch seconds) to track when
  the probe itself last produced data, independently of when the bridge
  last polled the API.
- Combines API health and sensor freshness into a single `status`
  sensor so you can tell at a glance whether values are fresh, whether
  the API is rate-limiting, or whether the probe is silent.
- On HTTP 429 (rate limit) or transient errors, **keeps the last
  retained values** and applies exponential backoff (15 / 30 / 60 /
  120 minutes) instead of hammering the API.

## Installation

1. Home Assistant: Settings > Add-ons > Add-on Store.
2. Three-dot menu > Repositories > paste the repository URL > Add.
3. Refresh, open the "Fluidra Blue Connect to MQTT" add-on, click
   Install.

## Configuration

| Option | Description |
| --- | --- |
| `fluidra_email` | Email of your Fluidra Pool account |
| `fluidra_password` | Fluidra Pool password |
| `pool_id` | Pool UUID (from the Fluidra account) |
| `device_id` | Blue Connect device id (e.g. `QXxxxxxxxx`) |
| `poll_interval_seconds` | Poll interval in seconds (default 7200, i.e. 2 h) |
| `mqtt_host` | `core-mosquitto` when using the official Mosquitto add-on |
| `mqtt_port` | 1883 |
| `mqtt_username` / `mqtt_password` | MQTT broker credentials |
| `mqtt_discovery_prefix` | `homeassistant` (leave as is) |
| `log_level` | `INFO` (set to `DEBUG` to troubleshoot) |

## Status sensor logic

The `Status` sensor combines API health and probe freshness. The worst
of the two wins:

- `ok` — API call succeeded **and** last probe reading is less than 6 h
  old.
- `degraded` — API is rate-limited (429), partial cycle, **or** last
  probe reading is between 6 h and 24 h old.
- `error` — Full API failure **or** last probe reading is more than
  24 h old.

This lets you distinguish between an API issue (transient, values are
still recent on the probe) and a probe issue (battery, Sigfox reach,
extender unplugged…) directly from a single sensor.

## Notes

- The Fluidra account must not have MFA enabled (the bridge does not
  handle the MFA challenge; rare on standard Fluidra Pool accounts).
- Salinity (component 16) is mapped as `g/L`. If your probe reports a
  different unit, adjust the `SENSORS` mapping in `bridge.py`.
- The first poll runs immediately at start-up, subsequent polls follow
  the configured interval.
- A polling interval of 2 h is recommended and is the default. The
  probe pushes a few readings per day at best, so shorter intervals
  do not yield more data and increase the risk of getting rate-limited.

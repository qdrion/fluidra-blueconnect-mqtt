#!/usr/bin/env python3
"""Fluidra Blue Connect -> MQTT bridge (v1.1.0).

v1.1.0 changes:
  - Exponential backoff on HTTP 429 (rate limit).
  - Throttle (delay) between component requests within a cycle.
  - On error: KEEP the last good value (no overwrite, no 'unavailable'),
    and expose a status sensor + last-success timestamp for a visual
    freshness indicator on the dashboard.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any

import requests
import paho.mqtt.client as mqtt

COGNITO_ENDPOINT = "https://cognito-idp.eu-west-1.amazonaws.com/"
COGNITO_CLIENT_ID = "g3njunelkcbtefosqm9bdhhq1"
FLUIDRA_BASE = "https://api.fluidra-emea.com"
USER_AGENT = (
    "com.fluidra.iaqualinkplus/1741857021 "
    "(Linux; U; Android 14; fr_FR; MI PAD 4; "
    "Build/UQ1A.240205.004; Cronet/140.0.7289.0)"
)

INTER_REQUEST_DELAY = 8
BACKOFF_MINUTES = [15, 30, 60, 120]

SENSORS: dict[int, dict[str, Any]] = {
    13: {"key": "ph", "name": "pH", "unit": None,
         "device_class": None, "icon": "mdi:ph", "precision": 2},
    12: {"key": "water_temperature", "name": "Temperature eau",
         "unit": "\u00b0C", "device_class": "temperature",
         "icon": "mdi:pool-thermometer", "precision": 1},
    14: {"key": "orp", "name": "ORP", "unit": "mV",
         "device_class": None, "icon": "mdi:flash", "precision": 0},
    16: {"key": "salinity", "name": "Salinite", "unit": "g/L",
         "device_class": None, "icon": "mdi:shaker-outline",
         "precision": 2},
}

EMAIL = os.environ["FLUIDRA_EMAIL"]
PASSWORD = os.environ["FLUIDRA_PASSWORD"]
POOL_ID = os.environ.get("POOL_ID", "")
DEVICE_ID = os.environ["DEVICE_ID"]
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL_SECONDS", "7200"))
MQTT_HOST = os.environ.get("MQTT_HOST", "core-mosquitto")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_USERNAME = os.environ.get("MQTT_USERNAME", "") or None
MQTT_PASSWORD = os.environ.get("MQTT_PASSWORD", "") or None
DISCOVERY_PREFIX = os.environ.get("MQTT_DISCOVERY_PREFIX", "homeassistant")
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("fluidra-bridge")

NODE_ID = "blueconnect"
BASE_TOPIC = f"fluidra/{NODE_ID}"
AVAILABILITY_TOPIC = f"{BASE_TOPIC}/availability"
STATUS_TOPIC = f"{BASE_TOPIC}/status"
LASTUPDATE_TOPIC = f"{BASE_TOPIC}/last_update"

_running = True


def _stop(signum, frame):
    global _running
    log.info("Signal %s received, shutting down.", signum)
    _running = False


signal.signal(signal.SIGTERM, _stop)
signal.signal(signal.SIGINT, _stop)


class RateLimited(Exception):
    """Raised when the Fluidra API answers HTTP 429."""


class FluidraClient:
    def __init__(self, email: str, password: str) -> None:
        self._email = email
        self._password = password
        self._access_token: str | None = None
        self._refresh_token: str | None = None
        self._expires_at: float = 0.0
        self._s = requests.Session()

    def _cognito(self, payload: dict[str, Any]) -> dict[str, Any]:
        headers = {
            "Content-Type": "application/x-amz-json-1.1; charset=utf-8",
            "X-Amz-Target":
                "AWSCognitoIdentityProviderService.InitiateAuth",
            "User-Agent": USER_AGENT,
        }
        r = self._s.post(COGNITO_ENDPOINT, headers=headers,
                         json=payload, timeout=30)
        if r.status_code == 429:
            raise RateLimited("Cognito rate limited (429)")
        if r.status_code != 200:
            raise RuntimeError(
                f"Cognito auth failed {r.status_code}: {r.text[:300]}")
        return r.json()

    def _store(self, auth_result: dict[str, Any]) -> None:
        self._access_token = auth_result.get("AccessToken")
        new_refresh = auth_result.get("RefreshToken")
        if new_refresh:
            self._refresh_token = new_refresh
        expires_in = int(auth_result.get("ExpiresIn", 3600))
        self._expires_at = time.time() + expires_in - 120
        if not self._access_token:
            raise RuntimeError("No AccessToken returned by Cognito")

    def login(self) -> None:
        if self._refresh_token:
            try:
                data = self._cognito({
                    "AuthFlow": "REFRESH_TOKEN_AUTH",
                    "ClientId": COGNITO_CLIENT_ID,
                    "AuthParameters": {
                        "REFRESH_TOKEN": self._refresh_token},
                })
                self._store(data.get("AuthenticationResult", {}))
                log.debug("Refreshed access token via refresh token.")
                return
            except RateLimited:
                raise
            except Exception as exc:
                log.warning("Refresh token failed (%s), full re-auth.",
                            exc)

        data = self._cognito({
            "AuthFlow": "USER_PASSWORD_AUTH",
            "ClientId": COGNITO_CLIENT_ID,
            "AuthParameters": {
                "USERNAME": self._email, "PASSWORD": self._password},
        })
        auth_result = data.get("AuthenticationResult")
        if not auth_result:
            challenge = data.get("ChallengeName", "none")
            raise RuntimeError(
                f"Login requires challenge '{challenge}'. "
                "MFA is not supported by this bridge.")
        self._store(auth_result)
        log.info("Authenticated against Fluidra Connect.")

    def _ensure_token(self) -> None:
        if not self._access_token or time.time() >= self._expires_at:
            self.login()

    def get_component(self, device_id: str, component_id: int) -> Any:
        self._ensure_token()
        url = (f"{FLUIDRA_BASE}/generic/devices/{device_id}"
               f"/components/{component_id}")
        headers = {
            "Authorization": f"Bearer {self._access_token}",
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
        }
        r = self._s.get(url, headers=headers,
                        params={"deviceType": "connected"}, timeout=30)
        if r.status_code == 429:
            raise RateLimited(
                f"Component {component_id} rate limited (429)")
        if r.status_code == 401:
            log.debug("401 on component %s, re-login.", component_id)
            self.login()
            headers["Authorization"] = f"Bearer {self._access_token}"
            r = self._s.get(url, headers=headers,
                            params={"deviceType": "connected"},
                            timeout=30)
            if r.status_code == 429:
                raise RateLimited(
                    f"Component {component_id} rate limited (429)")
        if r.status_code != 200:
            raise RuntimeError(
                f"Component {component_id} HTTP {r.status_code}: "
                f"{r.text[:200]}")
        return r.json()


def build_mqtt() -> mqtt.Client:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                         client_id="fluidra-blueconnect-bridge")
    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.will_set(AVAILABILITY_TOPIC, "offline", retain=True)
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()
    return client


def publish_discovery(client: mqtt.Client) -> None:
    device_block = {
        "identifiers": [f"fluidra_{DEVICE_ID}"],
        "name": "Blue Connect Gold",
        "manufacturer": "Fluidra",
        "model": "Blue Connect (blueconnectv3)",
    }
    for comp_id, meta in SENSORS.items():
        key = meta["key"]
        cfg_topic = f"{DISCOVERY_PREFIX}/sensor/{NODE_ID}_{key}/config"
        payload: dict[str, Any] = {
            "name": meta["name"],
            "unique_id": f"fluidra_{DEVICE_ID}_{key}",
            "state_topic": f"{BASE_TOPIC}/{key}",
            "availability_topic": AVAILABILITY_TOPIC,
            "state_class": "measurement",
            "device": device_block,
        }
        if meta["unit"]:
            payload["unit_of_measurement"] = meta["unit"]
        if meta["device_class"]:
            payload["device_class"] = meta["device_class"]
        if meta["icon"]:
            payload["icon"] = meta["icon"]
        client.publish(cfg_topic, json.dumps(payload), retain=True)

    client.publish(
        f"{DISCOVERY_PREFIX}/sensor/{NODE_ID}_etat/config",
        json.dumps({
            "name": "Etat",
            "unique_id": f"fluidra_{DEVICE_ID}_etat",
            "state_topic": STATUS_TOPIC,
            "availability_topic": AVAILABILITY_TOPIC,
            "icon": "mdi:check-network",
            "device": device_block,
        }), retain=True)

    client.publish(
        f"{DISCOVERY_PREFIX}/sensor/{NODE_ID}_last_update/config",
        json.dumps({
            "name": "Derniere maj",
            "unique_id": f"fluidra_{DEVICE_ID}_last_update",
            "state_topic": LASTUPDATE_TOPIC,
            "availability_topic": AVAILABILITY_TOPIC,
            "device_class": "timestamp",
            "icon": "mdi:clock-check",
            "device": device_block,
        }), retain=True)

    client.publish(AVAILABILITY_TOPIC, "online", retain=True)


def extract_value(raw: Any) -> Any:
    if isinstance(raw, dict):
        for field in ("reportedValue", "value", "desiredValue"):
            if field in raw and raw[field] is not None:
                return raw[field]
        data = raw.get("data")
        if isinstance(data, dict):
            return extract_value(data)
    return raw


def poll_once(fc: FluidraClient, client: mqtt.Client) -> str:
    ok_count = 0
    total = len(SENSORS)
    items = list(SENSORS.items())

    for idx, (comp_id, meta) in enumerate(items):
        key = meta["key"]
        try:
            raw = fc.get_component(DEVICE_ID, comp_id)
            value = extract_value(raw)
            if value is None:
                log.warning("Component %s (%s) returned no value",
                            comp_id, key)
            else:
                try:
                    num = float(value)
                    prec = meta.get("precision")
                    if prec is not None:
                        num = round(num, prec)
                        if prec == 0:
                            num = int(num)
                    out: Any = num
                except (TypeError, ValueError):
                    out = value
                client.publish(f"{BASE_TOPIC}/{key}", out, retain=True)
                log.info("%s = %s", key, out)
                ok_count += 1
        except RateLimited as exc:
            log.error("%s -> aborting cycle, will back off.", exc)
            raise
        except Exception as exc:
            log.error("Failed reading component %s (%s): %s "
                      "(keeping last value)", comp_id, key, exc)

        if idx < len(items) - 1:
            for _ in range(INTER_REQUEST_DELAY):
                if not _running:
                    break
                time.sleep(1)

    if ok_count == total:
        return "ok"
    if ok_count == 0:
        return "error"
    return "degraded"


def publish_status(client: mqtt.Client, status: str,
                   touch_last_update: bool) -> None:
    client.publish(STATUS_TOPIC, status, retain=True)
    if touch_last_update:
        now_iso = datetime.now(timezone.utc).isoformat()
        client.publish(LASTUPDATE_TOPIC, now_iso, retain=True)


def interruptible_sleep(seconds: int) -> None:
    for _ in range(max(1, int(seconds))):
        if not _running:
            return
        time.sleep(1)


def main() -> None:
    log.info("Bridge v1.1.0 starting (device=%s, pool=%s, every %ss).",
             DEVICE_ID, POOL_ID or "n/a", POLL_INTERVAL)
    fc = FluidraClient(EMAIL, PASSWORD)

    while _running:
        try:
            fc.login()
            break
        except RateLimited:
            log.error("Rate limited during initial login; wait 15 min.")
            interruptible_sleep(15 * 60)
        except Exception as exc:
            log.error("Initial login failed: %s (retry in 60s)", exc)
            interruptible_sleep(60)

    client = build_mqtt()
    publish_discovery(client)
    publish_status(client, "ok", touch_last_update=False)

    backoff_idx = 0

    while _running:
        start = time.time()
        try:
            status = poll_once(fc, client)
            publish_status(client, status,
                           touch_last_update=(status != "error"))
            client.publish(AVAILABILITY_TOPIC, "online", retain=True)
            backoff_idx = 0
            elapsed = int(time.time() - start)
            interruptible_sleep(max(5, POLL_INTERVAL - elapsed))
        except RateLimited:
            publish_status(client, "degraded", touch_last_update=False)
            client.publish(AVAILABILITY_TOPIC, "online", retain=True)
            wait_min = BACKOFF_MINUTES[
                min(backoff_idx, len(BACKOFF_MINUTES) - 1)]
            backoff_idx += 1
            log.warning("Rate limited (429). Backing off %d min "
                        "(values kept).", wait_min)
            interruptible_sleep(wait_min * 60)
        except Exception as exc:
            log.error("Poll cycle error: %s", exc)
            publish_status(client, "error", touch_last_update=False)
            interruptible_sleep(300)

    log.info("Stopping: marking offline and disconnecting.")
    client.publish(AVAILABILITY_TOPIC, "offline", retain=True)
    time.sleep(1)
    client.loop_stop()
    client.disconnect()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)

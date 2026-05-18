#!/usr/bin/env python3
"""Fluidra Blue Connect -> MQTT bridge.

Authenticates against the Fluidra Connect API (AWS Cognito), reads the Blue
Connect water-quality components, and publishes them to MQTT using Home
Assistant discovery so they appear as native sensors.

Reverse-engineered endpoints (same as the foXaCe/Fluidra-pool integration):
  - Cognito : https://cognito-idp.eu-west-1.amazonaws.com/
  - API     : https://api.fluidra-emea.com
  - Component: GET /generic/devices/{device_id}/components/{id}?deviceType=connected
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
from typing import Any

import requests
import paho.mqtt.client as mqtt

# --------------------------------------------------------------------------- #
# Constants (extracted from the Fluidra mobile app / foXaCe integration)
# --------------------------------------------------------------------------- #
COGNITO_ENDPOINT = "https://cognito-idp.eu-west-1.amazonaws.com/"
COGNITO_CLIENT_ID = "g3njunelkcbtefosqm9bdhhq1"
FLUIDRA_BASE = "https://api.fluidra-emea.com"
USER_AGENT = (
    "com.fluidra.iaqualinkplus/1741857021 "
    "(Linux; U; Android 14; fr_FR; MI PAD 4; "
    "Build/UQ1A.240205.004; Cronet/140.0.7289.0)"
)

# Component id -> sensor definition.
# Confirmed from the diagnostics dump of device QX25002505 (Blue Connect v3,
# salt pool): comp 12 = 22.4 (water temp), comp 13 = 7.3 (pH),
# comp 14 = 700 (ORP mV), comp 16 = 4.52 (salinity g/L, salt pool).
SENSORS: dict[int, dict[str, Any]] = {
    13: {
        "key": "ph",
        "name": "pH",
        "unit": None,
        "device_class": None,
        "icon": "mdi:ph",
        "precision": 2,
    },
    12: {
        "key": "water_temperature",
        "name": "Temperature eau",
        "unit": "\u00b0C",
        "device_class": "temperature",
        "icon": "mdi:pool-thermometer",
        "precision": 1,
    },
    14: {
        "key": "orp",
        "name": "ORP",
        "unit": "mV",
        "device_class": None,
        "icon": "mdi:flash",
        "precision": 0,
    },
    16: {
        "key": "salinity",
        "name": "Salinite",
        "unit": "g/L",
        "device_class": None,
        "icon": "mdi:shaker-outline",
        "precision": 2,
    },
}

# --------------------------------------------------------------------------- #
# Configuration from environment (set by run.sh from add-on options)
# --------------------------------------------------------------------------- #
EMAIL = os.environ["FLUIDRA_EMAIL"]
PASSWORD = os.environ["FLUIDRA_PASSWORD"]
POOL_ID = os.environ.get("POOL_ID", "")
DEVICE_ID = os.environ["DEVICE_ID"]
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL_SECONDS", "3600"))
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

_running = True


def _stop(signum, frame):  # noqa: ANN001, D401
    global _running
    log.info("Signal %s received, shutting down.", signum)
    _running = False


signal.signal(signal.SIGTERM, _stop)
signal.signal(signal.SIGINT, _stop)


# --------------------------------------------------------------------------- #
# Fluidra API client
# --------------------------------------------------------------------------- #
class FluidraClient:
    """Minimal Fluidra Connect client (auth + component read)."""

    def __init__(self, email: str, password: str) -> None:
        self._email = email
        self._password = password
        self._access_token: str | None = None
        self._refresh_token: str | None = None
        self._expires_at: float = 0.0
        self._s = requests.Session()

    # ---- authentication -------------------------------------------------- #
    def _cognito(self, payload: dict[str, Any]) -> dict[str, Any]:
        headers = {
            "Content-Type": "application/x-amz-json-1.1; charset=utf-8",
            "X-Amz-Target": "AWSCognitoIdentityProviderService.InitiateAuth",
            "User-Agent": USER_AGENT,
        }
        r = self._s.post(
            COGNITO_ENDPOINT, headers=headers, json=payload, timeout=30
        )
        if r.status_code != 200:
            raise RuntimeError(
                f"Cognito auth failed {r.status_code}: {r.text[:300]}"
            )
        return r.json()

    def _store(self, auth_result: dict[str, Any]) -> None:
        self._access_token = auth_result.get("AccessToken")
        new_refresh = auth_result.get("RefreshToken")
        if new_refresh:
            self._refresh_token = new_refresh
        expires_in = int(auth_result.get("ExpiresIn", 3600))
        # refresh a bit early
        self._expires_at = time.time() + expires_in - 120
        if not self._access_token:
            raise RuntimeError("No AccessToken returned by Cognito")

    def login(self) -> None:
        if self._refresh_token:
            try:
                data = self._cognito(
                    {
                        "AuthFlow": "REFRESH_TOKEN_AUTH",
                        "ClientId": COGNITO_CLIENT_ID,
                        "AuthParameters": {
                            "REFRESH_TOKEN": self._refresh_token
                        },
                    }
                )
                self._store(data.get("AuthenticationResult", {}))
                log.debug("Refreshed access token via refresh token.")
                return
            except Exception as exc:  # noqa: BLE001
                log.warning(
                    "Refresh token failed (%s), full re-auth.", exc
                )

        data = self._cognito(
            {
                "AuthFlow": "USER_PASSWORD_AUTH",
                "ClientId": COGNITO_CLIENT_ID,
                "AuthParameters": {
                    "USERNAME": self._email,
                    "PASSWORD": self._password,
                },
            }
        )
        auth_result = data.get("AuthenticationResult")
        if not auth_result:
            challenge = data.get("ChallengeName", "none")
            raise RuntimeError(
                f"Login requires challenge '{challenge}'. "
                "MFA is not supported by this bridge; disable MFA on the "
                "Fluidra account or use an account without it."
            )
        self._store(auth_result)
        log.info("Authenticated against Fluidra Connect.")

    def _ensure_token(self) -> None:
        if not self._access_token or time.time() >= self._expires_at:
            self.login()

    # ---- component read -------------------------------------------------- #
    def get_component(self, device_id: str, component_id: int) -> Any:
        self._ensure_token()
        url = (
            f"{FLUIDRA_BASE}/generic/devices/{device_id}"
            f"/components/{component_id}"
        )
        headers = {
            "Authorization": f"Bearer {self._access_token}",
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
        }
        r = self._s.get(
            url,
            headers=headers,
            params={"deviceType": "connected"},
            timeout=30,
        )
        if r.status_code == 401:
            # token might be stale despite expiry math; force re-auth once
            log.debug("401 on component %s, retrying after re-login.",
                      component_id)
            self.login()
            headers["Authorization"] = f"Bearer {self._access_token}"
            r = self._s.get(
                url,
                headers=headers,
                params={"deviceType": "connected"},
                timeout=30,
            )
        if r.status_code != 200:
            raise RuntimeError(
                f"Component {component_id} HTTP {r.status_code}: "
                f"{r.text[:200]}"
            )
        return r.json()


# --------------------------------------------------------------------------- #
# MQTT helpers
# --------------------------------------------------------------------------- #
def build_mqtt() -> mqtt.Client:
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="fluidra-blueconnect-bridge",
    )
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
        cfg_topic = (
            f"{DISCOVERY_PREFIX}/sensor/{NODE_ID}_{key}/config"
        )
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
        log.debug("Published discovery for %s", key)
    client.publish(AVAILABILITY_TOPIC, "online", retain=True)


def extract_value(raw: Any) -> Any:
    """Pull the numeric reading out of the component response."""
    if isinstance(raw, dict):
        for field in ("reportedValue", "value", "desiredValue"):
            if field in raw and raw[field] is not None:
                return raw[field]
        # some endpoints nest under 'data'
        data = raw.get("data")
        if isinstance(data, dict):
            return extract_value(data)
    return raw


# --------------------------------------------------------------------------- #
# Main loop
# --------------------------------------------------------------------------- #
def poll_once(fc: FluidraClient, client: mqtt.Client) -> None:
    any_ok = False
    for comp_id, meta in SENSORS.items():
        key = meta["key"]
        try:
            raw = fc.get_component(DEVICE_ID, comp_id)
            value = extract_value(raw)
            if value is None:
                log.warning("Component %s (%s) returned no value",
                            comp_id, key)
                continue
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
            any_ok = True
        except Exception as exc:  # noqa: BLE001
            log.error("Failed reading component %s (%s): %s",
                      comp_id, key, exc)
    client.publish(
        AVAILABILITY_TOPIC,
        "online" if any_ok else "offline",
        retain=True,
    )


def main() -> None:
    log.info(
        "Bridge starting (device=%s, pool=%s, every %ss).",
        DEVICE_ID,
        POOL_ID or "n/a",
        POLL_INTERVAL,
    )
    fc = FluidraClient(EMAIL, PASSWORD)

    # initial auth with retry
    while _running:
        try:
            fc.login()
            break
        except Exception as exc:  # noqa: BLE001
            log.error("Initial login failed: %s (retry in 60s)", exc)
            time.sleep(60)

    client = build_mqtt()
    publish_discovery(client)

    while _running:
        start = time.time()
        try:
            poll_once(fc, client)
        except Exception as exc:  # noqa: BLE001
            log.error("Poll cycle error: %s", exc)
        # sleep in 1s slices so SIGTERM is responsive
        elapsed = time.time() - start
        remaining = max(5, POLL_INTERVAL - int(elapsed))
        for _ in range(remaining):
            if not _running:
                break
            time.sleep(1)

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

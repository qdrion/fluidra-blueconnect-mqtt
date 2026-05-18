#!/usr/bin/with-contenv bashio
# shellcheck shell=bash

export FLUIDRA_EMAIL="$(bashio::config 'fluidra_email')"
export FLUIDRA_PASSWORD="$(bashio::config 'fluidra_password')"
export POOL_ID="$(bashio::config 'pool_id')"
export DEVICE_ID="$(bashio::config 'device_id')"
export POLL_INTERVAL_SECONDS="$(bashio::config 'poll_interval_seconds')"
export MQTT_HOST="$(bashio::config 'mqtt_host')"
export MQTT_PORT="$(bashio::config 'mqtt_port')"
export MQTT_USERNAME="$(bashio::config 'mqtt_username')"
export MQTT_PASSWORD="$(bashio::config 'mqtt_password')"
export MQTT_DISCOVERY_PREFIX="$(bashio::config 'mqtt_discovery_prefix')"
export LOG_LEVEL="$(bashio::config 'log_level')"

bashio::log.info "Starting Fluidra Blue Connect to MQTT bridge..."
exec python3 /bridge.py

# Fluidra Blue Connect to MQTT

Add-on Home Assistant qui lit les mesures du Blue Connect (Fluidra Connect)
et les publie via MQTT avec auto-discovery.

Capteurs créés (device QX25002505, Blue Connect v3, piscine au sel) :

- pH (composant 13)
- Température eau, °C (composant 12)
- ORP / redox, mV (composant 14)
- Salinité, g/L (composant 16)

## Pourquoi ce bridge

L'intégration HACS `foXaCe/Fluidra-pool` récupère bien ces composants mais
ne les expose pas en entités pour les appareils de la famille
"Data collectors" (le Blue Connect). Ce bridge interroge directement
l'endpoint composant de l'API Fluidra et publie le résultat sur MQTT.

## Installation

1. Pousser ce dépôt sur GitHub (par ex. `github.com/qdrion/fluidra-blueconnect-mqtt`).
2. Home Assistant : Paramètres > Modules complémentaires > Boutique.
3. Menu (3 points) > Dépôts > coller l'URL du dépôt > Ajouter.
4. Rafraîchir, ouvrir l'add-on "Fluidra Blue Connect to MQTT", Installer.

## Configuration

| Option | Description |
| --- | --- |
| `fluidra_email` | Email du compte Fluidra Pool |
| `fluidra_password` | Mot de passe Fluidra Pool |
| `pool_id` | Pré-rempli : `308b19be-ae03-5f98-a48a-42ed05xxxxxx` |
| `device_id` | Pré-rempli : `QX2500xxxx` |
| `poll_interval_seconds` | 3600 (1 h) par défaut |
| `mqtt_host` | `core-mosquitto` si add-on Mosquitto officiel |
| `mqtt_port` | 1883 |
| `mqtt_username` / `mqtt_password` | Identifiants du broker MQTT |
| `mqtt_discovery_prefix` | `homeassistant` (à laisser tel quel) |
| `log_level` | INFO (passer en DEBUG pour diagnostiquer) |

## Notes

- Le compte Fluidra ne doit pas avoir le MFA activé (le bridge ne gère pas
  le challenge MFA ; rare sur les comptes Fluidra Pool standard).
- Première remontée immédiate au démarrage, puis toutes les heures.

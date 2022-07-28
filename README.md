# High Altitude Balloon APRS Tracker
Python-based APRS-IS data collector of High Altitude Balloon APRS Tracker. Requires [PHP-based API](https://github.com/mkbodanu4/high-altitude-balloon-aprs-api) to access saved data from other applications.

## Requirements

* aprslib=0.7.2
* pyaml=6.0
* mysql-connector-python=8.0.29

## Installation

1. Upload code to your VPS or server
2. Update *configuration.yaml* file with your own configuration
3. Update *habat.service* file with the proper path to the installation folder.
4. Copy *habat.service* file to systemd folder (*/etc/systemd/system/*)
5. Enable and start a service named *habat*.

## Configuration

* aprs
  * call_sign - unique call sign and SSID, used to identify you at APRS-IS server
  * host - hostname of APRS IS server
  * filter - filter, applied to APRS-IS stream. Keep this as it is
* history
  * keep - how many rows keep for every balloon. Number or string 'all' is only allowed.
* logging
  * level - use ERROR for production and DEBUG for debugging
* mysql - MySQL/MariaDB server configuration, use TCP-IP or Unix Socket, depends on your system.

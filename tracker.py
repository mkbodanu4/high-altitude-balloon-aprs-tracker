import mysql.connector
import aprslib
import yaml
import logging
import time
import re

with open("configuration.yaml", 'r') as stream:
    configuration = yaml.safe_load(stream)

logging.basicConfig(level=configuration['logging']['level'])

if configuration['mysql']['unix_socket']:
    db = mysql.connector.connect(
        unix_socket=configuration['mysql']['unix_socket'],
        user=configuration['mysql']['username'],
        password=configuration['mysql']['password'],
        database=configuration['mysql']['database'],
    )
else:
    db = mysql.connector.connect(
        host=configuration['mysql']['hostname'],
        user=configuration['mysql']['username'],
        password=configuration['mysql']['password'],
        database=configuration['mysql']['database'],
    )

last_flush = 0


def flush_history(keep=500):
    global last_flush

    if (time.time() - last_flush) > 60:
        crs = db.cursor()
        # Keep only last *keep* rows for each call sign
        crs.execute("""
        DELETE `h3`
        FROM `history` `h3`
        WHERE `h3`.`date` < (
        SELECT `t`.`date`
        FROM (SELECT `h1`.`call_sign`,
                     (SELECT `h2`.`date`
                      FROM `history` `h2`
                      WHERE `h2`.`call_sign` = `h1`.`call_sign`
                      ORDER BY `h2`.`date` DESC
                      LIMIT %s, 1) AS `date`
              FROM `history` `h1`
              GROUP BY `h1`.`call_sign`) `t`
        WHERE `h3`.`call_sign` = `t`.`call_sign` AND `t`.`date` IS NOT NULL
        );
        """, (keep - 1,))
        crs.close()

        logging.info("History flushed")
        last_flush = time.time()


def callback(packet):
    if configuration['history']['keep'] != 'all':
        flush_history(keep=configuration['history']['keep'])

    try:
        parsed = aprslib.parse(packet)
    except (aprslib.ParseError, aprslib.UnknownFormat) as exp:
        return

    # Packed must be routed to APRS-IS by IGate only, so app will ignore Radiosonde traffic etc
    q = parsed.get('path')[-2]
    if q not in ['qAR', 'qAO', 'qAo']:
        logging.info("Packet from " + parsed.get('from') + " (" + q + "," + parsed.get('via') + "; " + parsed.get(
            "comment") + ") ignored")
        return

    if not (parsed.get('latitude') and parsed.get('longitude')):
        logging.info("Packet from " + parsed.get('from') + " has empty coordinates, ignored")
        return

    if parsed.get('altitude') is not None and parsed.get('altitude') < 0:
        logging.info(
            "Packet from " + parsed.get('from') + " ignored for wrong altitude (" + str(parsed.get("altitude")) + " m)")
        return

    if parsed.get('latitude') is None and parsed.get('longitude') is None and parsed.get('latitude') is None and parsed.get(
            'longitude') is None:
        logging.info("Packet from " + parsed.get(
            'from') + " ignored for missing coordinates.")
        return

    if parsed.get('latitude') < 0.1 and parsed.get('longitude') < 0.1 and parsed.get('latitude') > -0.1 and parsed.get(
            'longitude') > -0.1:
        logging.info("Packet from " + parsed.get(
            'from') + " ignored for wrong coordinates: very close to 0,0 , looks like GPS positioning error")
        return

    regexp = re.compile(r'(' + '|'.join(configuration['aprs']['ignore_comment']) + ')')
    if regexp.search(parsed.get('comment')):
        logging.info("Packet from " + parsed.get('from') + " (" + parsed.get(
            "comment") + ") ignored for being Radiosonde, not amateur radio balloon")
        return

    if parsed.get('from') in configuration['aprs']['ignore_call_sign']:
        logging.info("Packet from " + parsed.get('from') + " ignored, reason: ignore list")
        return

    query = """INSERT INTO `history` SET
        `call_sign` = %s,
        `date` = UTC_TIMESTAMP(),
        `timestamp` = %s,
        `latitude` = %s,
        `longitude` = %s,
        `course` = %s,
        `speed` = %s,
        `altitude` = %s,
        `daodatumbyte` = %s,
        `comment` = %s,
        `raw` = %s
    ON DUPLICATE KEY
        UPDATE
            `call_sign` = %s,
            `date` = UTC_TIMESTAMP(),
            `timestamp` = %s,
            `latitude` = %s,
            `longitude` = %s,
            `course` = %s,
            `speed` = %s,
            `altitude` = %s,
            `daodatumbyte` = %s,
            `comment` = %s,
            `raw` = %s
    ;"""
    params = (
        parsed.get('from'),
        parsed.get('timestamp'),
        parsed.get('latitude'),
        parsed.get('longitude'),
        parsed.get('course'),
        parsed.get('speed'),
        parsed.get('altitude'),
        parsed.get('daodatumbyte'),
        parsed.get('comment'),
        parsed.get('raw'),

        parsed.get('from'),
        parsed.get('timestamp'),
        parsed.get('latitude'),
        parsed.get('longitude'),
        parsed.get('course'),
        parsed.get('speed'),
        parsed.get('altitude'),
        parsed.get('daodatumbyte'),
        parsed.get('comment'),
        parsed.get('raw')
    )

    crs = db.cursor()
    crs.execute(query, params)
    db.commit()
    crs.close()

    logging.info("Packet from " + parsed.get('from') + " saved to history")


AIS = aprslib.IS(configuration['aprs']['callsign'], passwd="-1", host=configuration['aprs']['host'], port=14580)
AIS.set_filter(configuration['aprs']['filter'])
AIS.connect()
AIS.consumer(callback, raw=True)

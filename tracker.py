import MySQLdb
import aprslib
import yaml
import logging
import time
import re

with open("configuration.yaml", 'r') as stream:
    configuration = yaml.safe_load(stream)

formatter = logging.Formatter(fmt='<%(asctime)s> [%(levelname)s]: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

logger = logging.getLogger()
logger.setLevel(configuration['logging']['level'])

sh = logging.StreamHandler()
sh.setFormatter(formatter)
logger.addHandler(sh)

fh = logging.FileHandler(filename='logs/' + str(configuration['logging']['level']).lower() + '.log', mode='a')
fh.setFormatter(formatter)
logger.addHandler(fh)

if configuration['mysql']['unix_socket']:
    db = MySQLdb.connect(
        unix_socket=configuration['mysql']['unix_socket'],
        user=configuration['mysql']['username'],
        password=configuration['mysql']['password'],
        database=configuration['mysql']['database'],
    )
else:
    db = MySQLdb.connect(
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
        try:
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
        except Exception as exception:
            logging.error("MySQL Error: " + str(exception))
        finally:
            crs.close()

        logging.info("History flushed")
        last_flush = time.time()


def callback(packet):
    if configuration['history']['keep'] != 'all':
        flush_history(keep=configuration['history']['keep'])

    try:
        parsed = aprslib.parse(packet)
    except (aprslib.ParseError, aprslib.UnknownFormat) as exception:
        logging.warning(packet + " ignored: can't be parsed (" + exception + ")")
        return

    if configuration['aprs']['allowed_q_construct'] is not None:
        q = parsed.get('path')[-2]
        if q not in configuration['aprs']['allowed_q_construct']:
            logging.debug(parsed.get('from') + " ignored: q construct prohibited (" + q + "," + parsed.get('via') + "; " + parsed.get("comment") + ")")
            return

    if parsed.get('latitude') is None or parsed.get('longitude') is None:
        logging.info(parsed.get('from') + " ignored: empty coordinates")
        return

    if 0.1 > parsed.get('latitude') > -0.1 and 0.1 > parsed.get('longitude') > -0.1:
        logging.info(parsed.get('from') + " ignored: GPS positioning error (" + str(parsed.get('latitude')) + str(parsed.get('longitude')) + ")")
        return

    if parsed.get('altitude') is None or parsed.get('altitude') < 0.3:  # 0.3 meters is 1 feet and ballot must be at least 1 feet above ground
        logging.info(parsed.get('from') + " ignored: wrong altitude (" + str(parsed.get("altitude")) + ")")
        return

    if configuration['aprs']['ignore_comment'] is not None and len(configuration['aprs']['ignore_comment']) > 0:
        regexp = re.compile(r'(' + '|'.join(configuration['aprs']['ignore_comment']) + ')')
        if regexp.search(parsed.get('comment')):
            logging.debug(parsed.get('from') + " ignored: comment has prohibited phrase in it (" + parsed.get("comment") + ")")
            return

    if configuration['aprs']['ignore_call_sign'] is not None and parsed.get('from') in configuration['aprs']['ignore_call_sign']:
        logging.info(parsed.get('from') + " ignored: call sign in ignore list")
        return

    check_duplicate_query = """SELECT
        COUNT(*) AS `duplicates`
    FROM
        `history`
    WHERE
        `date` > UTC_TIMESTAMP() - INTERVAL 10 MINUTE AND
        `call_sign` = %s AND
        `latitude` = %s AND
        `longitude` = %s
    LIMIT 1
    """
    check_duplicate_params = (
        parsed.get('from'),
        parsed.get('latitude'),
        parsed.get('longitude')
    )

    check_duplicate_result = None
    crs = db.cursor()
    try:
        crs.execute(check_duplicate_query, check_duplicate_params)
        check_duplicate_result = crs.fetchone()
    except Exception as exception:
        logging.error("MySQL Error: " + str(exception))
    finally:
        crs.close()

    if check_duplicate_result is not None and check_duplicate_result[0] > 0:
        logging.warning(parsed.get('from') + " found duplicate, previous row will be removed")

        delete_query = """DELETE
        FROM
            `history`
        WHERE
            `date` > UTC_TIMESTAMP() - INTERVAL 10 MINUTE AND
            `call_sign` = %s AND
            `latitude` = %s AND
            `longitude` = %s
        LIMIT 1
        """
        delete_params = (
            parsed.get('from'),
            parsed.get('latitude'),
            parsed.get('longitude')
        )

        crs = db.cursor()
        try:
            crs.execute(delete_query, delete_params)
            db.commit()
        except Exception as exception:
            logging.error("MySQL Error: " + str(exception))
            db.rollback()
        finally:
            crs.close()

    insert_query = """INSERT INTO
        `history`
    SET
        `call_sign` = %s,
        `date` = UTC_TIMESTAMP(),
        `timestamp` = %s,
        `latitude` = %s,
        `longitude` = %s,
        `course` = %s,
        `speed` = %s,
        `altitude` = %s,
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
            `comment` = %s,
            `raw` = %s
    ;"""
    insert_params = (
        parsed.get('from'),
        parsed.get('timestamp'),
        parsed.get('latitude'),
        parsed.get('longitude'),
        parsed.get('course'),
        parsed.get('speed'),
        parsed.get('altitude'),
        parsed.get('comment'),
        parsed.get('raw'),

        parsed.get('from'),
        parsed.get('timestamp'),
        parsed.get('latitude'),
        parsed.get('longitude'),
        parsed.get('course'),
        parsed.get('speed'),
        parsed.get('altitude'),
        parsed.get('comment'),
        parsed.get('raw')
    )

    crs = db.cursor()
    try:
        crs.execute(insert_query, insert_params)
        db.commit()
    except Exception as exception:
        logging.error("MySQL Error: " + str(exception))
        db.rollback()
    finally:
        crs.close()

    logging.info(parsed.get('from') + " saved")


try:
    AIS = aprslib.IS(configuration['aprs']['callsign'], passwd="-1", host=configuration['aprs']['host'], port=configuration['aprs']['port'])
    AIS.set_filter(configuration['aprs']['filter'])
    AIS.connect()
    AIS.consumer(callback, raw=True)
except Exception as e:
    logging.error("MySQL Error: " + str(e))
except KeyboardInterrupt:
    logging.info("Program stopping")
finally:
    db.close()

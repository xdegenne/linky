#!/usr/bin/python3

# stdlib
import datetime, time, logging

# Self libraries
import linky


# ----------------------------- #
# Setup                         #
# ----------------------------- #

# log = logging.getLogger('linky')

# log.debug('Loading config...')
config = linky.load_config()
# log.debug(f'Config loaded! Values: {config}')
log = utils.init_log_system(config)
period = int(config.get('period', 60))

terminal = linky.setup_serial(config['device'])

# Trying to connect to db server and creating schema if not exists
#linky.test_db_connection(config['database']['server'], config['database']['user'], config['database']['password'], config['database']['name'])

client = InfluxDBClient(host=config['influx_db']['server'], port=config['influx_db']['port'])
client.switch_database(config['influx_db']['name'])

# ----------------------------- #
# Main loop                     #
# ----------------------------- #

if config.get('use_utc', False):
    current_loop_day = datetime.datetime.now(datetime.timezone.utc).day
    previous_loop_day = datetime.datetime.now(datetime.timezone.utc).day
else:
    current_loop_day = datetime.datetime.now().day
    previous_loop_day = datetime.datetime.now().day 

while True:
    log.debug("Cycle begins")
    data_BASE = None
    data_PAPP = None
    data_IINST = None

    if config.get('use_utc', False):
        current_loop_day = datetime.datetime.now(datetime.timezone.utc).day
    else:
        current_loop_day = datetime.datetime.now().day

    # Now beginning to read data from Linky
    log.debug("Opening terminal...")
    terminal.open()

    # reading continously output until we have data that interests us
    while True:
        line = terminal.readline().decode('ascii')
        log.debug(f"Current line: <{line}>")

        if line.startswith('BASE'):
            data_BASE = int(line.split(' ')[1])
            log.debug(f"Parsed BASE: {data_BASE}")
        if line.startswith('PAPP'):
            data_PAPP = int(line.split(' ')[1])
            log.debug(f"Parsed PAPP: {data_PAPP}")
        if line.startswith('IINST'):
            data_IINST = int(line.split(' ')[1])
            log.debug(f"Parsed IINST: {data_IINST}")

        log.debug(f"BASE={data_BASE}, PAPP={data_PAPP}. IINST={data_IINST} => {data_BASE and data_PAPP and data_IINST}")
        # We have BASE and PAPP, we can now close the connection
        if data_BASE and data_PAPP != None and data_IINST != None:
            log.debug(f"Output parsed: BASE={data_BASE}, PAPP={data_PAPP}. IINST={data_IINST}. Closing terminal.")
            terminal.close()
            break
    
    # Connecting to database
    # log.debug("Connecting to database")
    # db, cr = linky.open_db(config['database']['server'], config['database']['user'], config['database']['password'], config['database']['name'])

    # # first record of the day? generating dailies
    # if current_loop_day != previous_loop_day:
    #     log.debug(f"First record of the day! Inserting dailies record.")
    #     linky.insert_dailies(config, db, cr, data_BASE)

    # if config.get('use_utc', False):
    #     previous_loop_day = datetime.datetime.now(datetime.timezone.utc).day
    # else:
    #     previous_loop_day = datetime.datetime.now().day

    # # inserting values
    # log.debug("Inserting stream record")
    # linky.insert_stream(config, db, cr, data_BASE, data_PAPP, data_IINST)

    log.debug(f"Cycle ends, sleeping for {period} seconds")
    time.sleep(period)

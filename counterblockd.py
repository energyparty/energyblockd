#! /usr/bin/env python3

# Copyright (c) 2016-Present Energyparty Developers
# Distributed under the AGPL 3.0 with the OpenSSL exception, see the
# accompanying file LICENSE or <https://github.com/energyparty/energyblockd/>.

import gevent
from gevent import monkey
import grequests
if not monkey.is_module_patched("os"):
    monkey.patch_all()
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

import sys
import os
import argparse
import json
import logging
import datetime
import configparser
import time
import email.utils

import appdirs
import pymongo
import rollbar
import redis
import redis.connection
redis.connection.socket = gevent.socket #make redis play well with gevent

import pygeoip

from lib import (config, api, events, blockfeed, util)


if __name__ == '__main__':
    # Parse command-line arguments.
    parser = argparse.ArgumentParser(prog='energyblockd', description='Energywallet daemon. Works with energypartyd')
    parser.add_argument('-V', '--version', action='version', version="energyblockd v%s" % config.VERSION)
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', default=False, help='sets log level to DEBUG instead of WARNING')

    parser.add_argument('--reparse', action='store_true', default=False, help='force full re-initialization of the energyblockd database')
    parser.add_argument('--testnet', action='store_true', default=False, help='use EnergyCoin testnet addresses and block numbers')
    parser.add_argument('--data-dir', help='specify to explicitly override the directory in which to keep the config file and log file')
    parser.add_argument('--config-file', help='the location of the configuration file')
    parser.add_argument('--log-file', help='the location of the log file')
    parser.add_argument('--tx-log-file', help='the location of the transaction log file')
    parser.add_argument('--pid-file', help='the location of the pid file')

    #THINGS WE CONNECT TO
    parser.add_argument('--backend-connect', help='the hostname or IP of the backend energycoind JSON-RPC server')
    parser.add_argument('--backend-port', type=int, help='the backend JSON-RPC port to connect to')
    parser.add_argument('--backend-user', help='the username used to communicate with backend over JSON-RPC')
    parser.add_argument('--backend-password', help='the password used to communicate with backend over JSON-RPC')

    parser.add_argument('--counterpartyd-rpc-connect', help='the hostname of the energypartyd JSON-RPC server')
    parser.add_argument('--counterpartyd_rpc_port', type=int, help='the port used to communicate with energypartyd over JSON-RPC')
    parser.add_argument('--counterpartyd-rpc-user', help='the username used to communicate with energypartyd over JSON-RPC')
    parser.add_argument('--counterpartyd-rpc-password', help='the password used to communicate with energypartyd over JSON-RPC')

    parser.add_argument('--blockchain-service-name', help='the blockchain service name to connect to')
    parser.add_argument('--blockchain-service-connect', help='the blockchain service server URL base to connect to, if not default')

    parser.add_argument('--mongodb-connect', help='the hostname of the mongodb server to connect to')
    parser.add_argument('--mongodb-port', type=int, help='the port used to communicate with mongodb')
    parser.add_argument('--mongodb-database', help='the mongodb database to connect to')
    parser.add_argument('--mongodb-user', help='the optional username used to communicate with mongodb')
    parser.add_argument('--mongodb-password', help='the optional password used to communicate with mongodb')

    parser.add_argument('--redis-enable-apicache', action='store_true', default=False, help='set to true to enable caching of API requests')
    parser.add_argument('--redis-connect', help='the hostname of the redis server to use for caching (if enabled')
    parser.add_argument('--redis-port', type=int, help='the port used to connect to the redis server for caching (if enabled)')
    parser.add_argument('--redis-database', type=int, help='the redis database ID (int) used to connect to the redis server for caching (if enabled)')

    #THINGS WE HOST
    parser.add_argument('--rpc-host', help='the IP of the interface to bind to for providing JSON-RPC API access (0.0.0.0 for all interfaces)')
    parser.add_argument('--rpc-port', type=int, help='port on which to provide the energyblockd JSON-RPC API')
    parser.add_argument('--rpc-allow-cors', action='store_true', default=True, help='Allow ajax cross domain request')
    parser.add_argument('--socketio-host', help='the interface on which to host the energyblockd socket.io API')
    parser.add_argument('--socketio-port', type=int, help='port on which to provide the energyblockd socket.io API')

    parser.add_argument('--rollbar-token', help='the API token to use with rollbar (leave blank to disable rollbar integration)')
    parser.add_argument('--rollbar-env', help='the environment name for the rollbar integration (if enabled). Defaults to \'production\'')

    args = parser.parse_args()

    # Data directory
    if not args.data_dir:
        config.DATA_DIR = appdirs.user_data_dir(appauthor='Energyparty', appname='energyblockd', roaming=True)
    else:
        config.DATA_DIR = args.data_dir
    if not os.path.isdir(config.DATA_DIR): os.mkdir(config.DATA_DIR)

    #Read config file
    configfile = configparser.ConfigParser()
    if args.config_file:
        config_path = args.config_file
    else:
        config_path = os.path.join(config.DATA_DIR, 'energyblockd.conf')
    configfile.read(config_path)
    has_config = configfile.has_section('Default')

    # testnet
    if args.testnet:
        config.TESTNET = args.testnet
    elif has_config and configfile.has_option('Default', 'testnet'):
        config.TESTNET = configfile.getboolean('Default', 'testnet')
    else:
        config.TESTNET = False
        
    # reparse
    config.REPARSE_FORCED = args.reparse
        
    ##############
    # THINGS WE CONNECT TO

    if args.backend_connect:
        config.BACKEND_CONNECT = args.backend_connect
    elif has_config and configfile.has_option('Default', 'backend-connect') and configfile.get('Default', 'backend-connect'):
        config.BACKEND_CONNECT = configfile.get('Default', 'backend-connect')
    else:
        config.BACKEND_CONNECT = 'localhost'

    if args.backend_port:
        config.BACKEND_PORT = args.backend_port
    elif has_config and configfile.has_option('Default', 'backend-port') and configfile.get('Default', 'backend-port'):
        config.BACKEND_PORT = configfile.get('Default', 'backend-port')
    else:
        config.BACKEND_PORT = config.DEFAULT_BACKEND_PORT_TESTNET if config.TESTNET else config.DEFAULT_BACKEND_PORT

    try:
        config.BACKEND_PORT = int(config.BACKEND_PORT)
        assert int(config.BACKEND_PORT) > 1 and int(config.BACKEND_PORT) < 65535
    except:
        raise Exception("Please specific a valid port number backend-port configuration parameter")

    if args.backend_user:
        config.BACKEND_USER = args.backend_user
    elif has_config and configfile.has_option('Default', 'backend-user') and configfile.get('Default', 'backend-user'):
        config.BACKEND_USER = configfile.get('Default', 'backend-user')
    else:
        config.BACKEND_USER = 'rpc'

    if args.backend_password:
        config.BACKEND_PASSWORD = args.backend_password
    elif has_config and configfile.has_option('Default', 'backend-password') and configfile.get('Default', 'backend-password'):
        config.BACKEND_PASSWORD = configfile.get('Default', 'backend-password')
    else:
        config.BACKEND_PASSWORD = 'rpcpassword'

    config.BACKEND_AUTH = (config.BACKEND_USER, config.BACKEND_PASSWORD) if (config.BACKEND_USER and config.BACKEND_PASSWORD) else None
    config.BACKEND_URL_NOAUTH = 'http://' + config.BACKEND_CONNECT + ':' + str(config.BACKEND_PORT) + '/'

    if args.counterpartyd_rpc_connect:
        config.COUNTERPARTYD_RPC_CONNECT = args.counterpartyd_rpc_connect
    elif has_config and configfile.has_option('Default', 'energypartyd-rpc-connect') and configfile.get('Default', 'energypartyd-rpc-connect'):
        config.COUNTERPARTYD_RPC_CONNECT = configfile.get('Default', 'energypartyd-rpc-connect')
    else:
        config.COUNTERPARTYD_RPC_CONNECT = 'localhost'

    if args.counterpartyd_rpc_port:
        config.COUNTERPARTYD_RPC_PORT = args.counterpartyd_rpc_port
    elif has_config and configfile.has_option('Default', 'energypartyd-rpc-port') and configfile.get('Default', 'energypartyd-rpc-port'):
        config.COUNTERPARTYD_RPC_PORT = configfile.get('Default', 'energypartyd-rpc-port')
    else:
        if config.TESTNET:
            config.COUNTERPARTYD_RPC_PORT = 15556
        else:
            config.COUNTERPARTYD_RPC_PORT = 5556
    try:
        config.COUNTERPARTYD_RPC_PORT = int(config.COUNTERPARTYD_RPC_PORT)
        assert int(config.COUNTERPARTYD_RPC_PORT) > 1 and int(config.COUNTERPARTYD_RPC_PORT) < 65535
    except:
        raise Exception("Please specific a valid port number energypartyd-rpc-port configuration parameter")
            
    if args.counterpartyd_rpc_user:
        config.COUNTERPARTYD_RPC_USER = args.counterpartyd_rpc_user
    elif has_config and configfile.has_option('Default', 'energypartyd-rpc-user') and configfile.get('Default', 'energypartyd-rpc-user'):
        config.COUNTERPARTYD_RPC_USER = configfile.get('Default', 'energypartyd-rpc-user')
    else:
        config.COUNTERPARTYD_RPC_USER = 'rpcuser'

    if args.counterpartyd_rpc_password:
        config.COUNTERPARTYD_RPC_PASSWORD = args.counterpartyd_rpc_password
    elif has_config and configfile.has_option('Default', 'energypartyd-rpc-password') and configfile.get('Default', 'energypartyd-rpc-password'):
        config.COUNTERPARTYD_RPC_PASSWORD = configfile.get('Default', 'energypartyd-rpc-password')
    else:
        config.COUNTERPARTYD_RPC_PASSWORD = 'rpcpassword'

    config.COUNTERPARTYD_RPC = 'http://' + config.COUNTERPARTYD_RPC_CONNECT + ':' + str(config.COUNTERPARTYD_RPC_PORT) + '/api/'
    config.COUNTERPARTYD_AUTH = (config.COUNTERPARTYD_RPC_USER, config.COUNTERPARTYD_RPC_PASSWORD) if (config.COUNTERPARTYD_RPC_USER and config.COUNTERPARTYD_RPC_PASSWORD) else None

    if args.blockchain_service_name:
        config.BLOCKCHAIN_SERVICE_NAME = args.blockchain_service_name
    elif has_config and configfile.has_option('Default', 'blockchain-service-name') and configfile.get('Default', 'blockchain-service-name'):
        config.BLOCKCHAIN_SERVICE_NAME = configfile.get('Default', 'blockchain-service-name')
    else:
        config.BLOCKCHAIN_SERVICE_NAME = 'addrindex'

    if args.blockchain_service_connect:
        config.BLOCKCHAIN_SERVICE_CONNECT = args.blockchain_service_connect
    elif has_config and configfile.has_option('Default', 'blockchain-service-connect') and configfile.get('Default', 'blockchain-service-connect'):
        config.BLOCKCHAIN_SERVICE_CONNECT = configfile.get('Default', 'blockchain-service-connect')
    else:
        config.BLOCKCHAIN_SERVICE_CONNECT = None #use default specified by the library

    # mongodb host
    if args.mongodb_connect:
        config.MONGODB_CONNECT = args.mongodb_connect
    elif has_config and configfile.has_option('Default', 'mongodb-connect') and configfile.get('Default', 'mongodb-connect'):
        config.MONGODB_CONNECT = configfile.get('Default', 'mongodb-connect')
    else:
        config.MONGODB_CONNECT = 'localhost'

    # mongodb port
    if args.mongodb_port:
        config.MONGODB_PORT = args.mongodb_port
    elif has_config and configfile.has_option('Default', 'mongodb-port') and configfile.get('Default', 'mongodb-port'):
        config.MONGODB_PORT = configfile.get('Default', 'mongodb-port')
    else:
        config.MONGODB_PORT = 27017
    try:
        config.MONGODB_PORT = int(config.MONGODB_PORT)
        assert int(config.MONGODB_PORT) > 1 and int(config.MONGODB_PORT) < 65535
    except:
        raise Exception("Please specific a valid port number mongodb-port configuration parameter")
            
    # mongodb database
    if args.mongodb_database:
        config.MONGODB_DATABASE = args.mongodb_database
    elif has_config and configfile.has_option('Default', 'mongodb-database') and configfile.get('Default', 'mongodb-database'):
        config.MONGODB_DATABASE = configfile.get('Default', 'mongodb-database')
    else:
        if config.TESTNET:
            config.MONGODB_DATABASE = 'counterblockd_testnet'
        else:
            config.MONGODB_DATABASE = 'counterblockd'

    # mongodb user
    if args.mongodb_user:
        config.MONGODB_USER = args.mongodb_user
    elif has_config and configfile.has_option('Default', 'mongodb-user') and configfile.get('Default', 'mongodb-user'):
        config.MONGODB_USER = configfile.get('Default', 'mongodb-user')
    else:
        config.MONGODB_USER = None

    # mongodb password
    if args.mongodb_password:
        config.MONGODB_PASSWORD = args.mongodb_password
    elif has_config and configfile.has_option('Default', 'mongodb-password') and configfile.get('Default', 'mongodb-password'):
        config.MONGODB_PASSWORD = configfile.get('Default', 'mongodb-password')
    else:
        config.MONGODB_PASSWORD = None

    # redis-enable-apicache
    if args.redis_enable_apicache:
        config.REDIS_ENABLE_APICACHE = args.redis_enable_apicache
    elif has_config and configfile.has_option('Default', 'redis-enable-apicache') and configfile.get('Default', 'redis-enable-apicache'):
        config.REDIS_ENABLE_APICACHE = configfile.getboolean('Default', 'redis-enable-apicache')
    else:
        config.REDIS_ENABLE_APICACHE = False

    # redis connect
    if args.redis_connect:
        config.REDIS_CONNECT = args.redis_connect
    elif has_config and configfile.has_option('Default', 'redis-connect') and configfile.get('Default', 'redis-connect'):
        config.REDIS_CONNECT = configfile.get('Default', 'redis-connect')
    else:
        config.REDIS_CONNECT = '127.0.0.1'

    # redis port
    if args.redis_port:
        config.REDIS_PORT = args.redis_port
    elif has_config and configfile.has_option('Default', 'redis-port') and configfile.get('Default', 'redis-port'):
        config.REDIS_PORT = configfile.get('Default', 'redis-port')
    else:
        config.REDIS_PORT = 6379
    try:
        config.REDIS_PORT = int(config.REDIS_PORT)
        assert int(config.REDIS_PORT) > 1 and int(config.REDIS_PORT) < 65535
    except:
        raise Exception("Please specific a valid port number redis-port configuration parameter")

    # redis database
    if args.redis_database:
        config.REDIS_DATABASE = args.redis_database
    elif has_config and configfile.has_option('Default', 'redis-database') and configfile.get('Default', 'redis-database'):
        config.REDIS_DATABASE = configfile.get('Default', 'redis-database')
    else:
        if config.TESTNET:
            config.REDIS_DATABASE = 1
        else:
            config.REDIS_DATABASE = 0
    try:
        config.REDIS_DATABASE = int(config.REDIS_DATABASE)
        assert int(config.REDIS_DATABASE) >= 0 and int(config.REDIS_DATABASE) <= 16
    except:
        raise Exception("Please specific a valid redis-database configuration parameter (between 0 and 16 inclusive)")

    ##############
    # THINGS WE SERVE
    
    if args.rpc_host:
        config.RPC_HOST = args.rpc_host
    elif has_config and configfile.has_option('Default', 'rpc-host') and configfile.get('Default', 'rpc-host'):
        config.RPC_HOST = configfile.get('Default', 'rpc-host')
    else:
        config.RPC_HOST = 'localhost'

    if args.rpc_port:
        config.RPC_PORT = args.rpc_port
    elif has_config and configfile.has_option('Default', 'rpc-port') and configfile.get('Default', 'rpc-port'):
        config.RPC_PORT = configfile.get('Default', 'rpc-port')
    else:
        if config.TESTNET:
            config.RPC_PORT = 14200
        else:
            config.RPC_PORT = 4200
    try:
        config.RPC_PORT = int(config.RPC_PORT)
        assert int(config.RPC_PORT) > 1 and int(config.RPC_PORT) < 65535
    except:
        raise Exception("Please specific a valid port number rpc-port configuration parameter")

     # RPC CORS
    if args.rpc_allow_cors:
        config.RPC_ALLOW_CORS = args.rpc_allow_cors
    elif has_config and configfile.has_option('Default', 'rpc-allow-cors'):
        config.RPC_ALLOW_CORS = configfile.getboolean('Default', 'rpc-allow-cors')
    else:
        config.RPC_ALLOW_CORS = True

    ##############
    # OTHER SETTINGS

    #More testnet
    if config.TESTNET:
        config.BLOCK_FIRST = 260
    else:
        config.BLOCK_FIRST = 2441800

    # Log
    if args.log_file:
        config.LOG = args.log_file
    elif has_config and configfile.has_option('Default', 'log-file'):
        config.LOG = configfile.get('Default', 'log-file')
    else:
        config.LOG = os.path.join(config.DATA_DIR, 'energyblockd.log')
        
    if args.tx_log_file:
        config.TX_LOG = args.tx_log_file
    elif has_config and configfile.has_option('Default', 'tx-log-file'):
        config.TX_LOG = configfile.get('Default', 'tx-log-file')
    else:
        config.TX_LOG = os.path.join(config.DATA_DIR, 'energyblockd-tx.log')
    

    # PID
    if args.pid_file:
        config.PID = args.pid_file
    elif has_config and configfile.has_option('Default', 'pid-file'):
        config.PID = configfile.get('Default', 'pid-file')
    else:
        config.PID = os.path.join(config.DATA_DIR, 'energyblockd.pid')

     # ROLLBAR INTEGRATION
    if args.rollbar_token:
        config.ROLLBAR_TOKEN = args.rollbar_token
    elif has_config and configfile.has_option('Default', 'rollbar-token'):
        config.ROLLBAR_TOKEN = configfile.get('Default', 'rollbar-token')
    else:
        config.ROLLBAR_TOKEN = None #disable rollbar integration

    if args.rollbar_env:
        config.ROLLBAR_ENV = args.rollbar_env
    elif has_config and configfile.has_option('Default', 'rollbar-env'):
        config.ROLLBAR_ENV = configfile.get('Default', 'rollbar-env')
    else:
        config.ROLLBAR_ENV = 'counterblockd-production'

    # current dir
    config.COUNTERBLOCKD_DIR = os.path.dirname(os.path.realpath(__file__))

    # initialize json schema for json asset and feed validation
    config.ASSET_SCHEMA = json.load(open(os.path.join(config.COUNTERBLOCKD_DIR, 'schemas', 'asset.schema.json')))
    config.FEED_SCHEMA = json.load(open(os.path.join(config.COUNTERBLOCKD_DIR, 'schemas', 'feed.schema.json')))

    #Create/update pid file
    pid = str(os.getpid())
    pidf = open(config.PID, 'w')
    pidf.write(pid)
    pidf.close()    

    # Logging (to file and console).
    MAX_LOG_SIZE = 10 * 1024 * 1024
    MAX_LOG_COUNT = 5
    logger = logging.getLogger() #get root logger
    logger.setLevel(logging.DEBUG if args.verbose else logging.INFO)
    
    #Color logging on console for warnings and errors
    logging.addLevelName( logging.WARNING, "\033[1;31m%s\033[1;0m" % logging.getLevelName(logging.WARNING))
    logging.addLevelName( logging.ERROR, "\033[1;41m%s\033[1;0m" % logging.getLevelName(logging.ERROR))
    
    #Console logging
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG if args.verbose else logging.INFO)
    formatter = logging.Formatter('%(asctime)s :: %(levelname)s :: %(message)s')
    console.setFormatter(formatter)
    logger.addHandler(console)
    #File logging (rotated)
    if os.name == 'nt':
        fileh = util_windows.SanitizedRotatingFileHandler(config.LOG, maxBytes=MAX_LOG_SIZE, backupCount=MAX_LOG_COUNT)
    else:
        fileh = logging.handlers.RotatingFileHandler(config.LOG, maxBytes=MAX_LOG_SIZE, backupCount=MAX_LOG_COUNT)
    fileh.setLevel(logging.DEBUG if args.verbose else logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(message)s', '%Y-%m-%d-T%H:%M:%S%z')
    fileh.setFormatter(formatter)
    logger.addHandler(fileh)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.CRITICAL)
    #Transaction log
    tx_logger = logging.getLogger("transaction_log") #get transaction logger
    tx_logger.setLevel(logging.DEBUG if args.verbose else logging.INFO)
    if os.name == 'nt':
        tx_fileh = util_windows.SanitizedRotatingFileHandler(config.TX_LOG, maxBytes=MAX_LOG_SIZE, backupCount=MAX_LOG_COUNT)
    else:
        tx_fileh = logging.handlers.RotatingFileHandler(config.TX_LOG, maxBytes=MAX_LOG_SIZE, backupCount=MAX_LOG_COUNT)
    tx_fileh.setLevel(logging.DEBUG if args.verbose else logging.INFO)
    tx_formatter = logging.Formatter('%(asctime)s %(message)s', '%Y-%m-%d-T%H:%M:%S%z')
    tx_fileh.setFormatter(tx_formatter)
    tx_logger.addHandler(tx_fileh)
    tx_logger.propagate = False
    
    # GeoIP
    config.GEOIP = util.init_geoip()

    #Connect to mongodb
    mongo_client = pymongo.MongoClient(config.MONGODB_CONNECT, config.MONGODB_PORT)
    mongo_db = mongo_client[config.MONGODB_DATABASE] #will create if it doesn't exist
    if config.MONGODB_USER and config.MONGODB_PASSWORD:
        if not mongo_db.authenticate(config.MONGODB_USER, config.MONGODB_PASSWORD):
            raise Exception("Could not authenticate to mongodb with the supplied username and password.")
    config.mongo_db = mongo_db #should be able to access fine across greenlets, etc

    #insert mongo indexes if need-be (i.e. for newly created database)
    ##COLLECTIONS THAT ARE PURGED AS A RESULT OF A REPARSE
    #processed_blocks
    mongo_db.processed_blocks.ensure_index('block_index', unique=True)
    #tracked_assets
    mongo_db.tracked_assets.ensure_index('asset', unique=True)
    mongo_db.tracked_assets.ensure_index('_at_block') #for tracked asset pruning
    mongo_db.tracked_assets.ensure_index([
        ("owner", pymongo.ASCENDING),
        ("asset", pymongo.ASCENDING),
    ])
    #trades
    mongo_db.trades.ensure_index([
        ("base_asset", pymongo.ASCENDING),
        ("quote_asset", pymongo.ASCENDING),
        ("block_time", pymongo.DESCENDING)
    ])
    mongo_db.trades.ensure_index([ #events.py and elsewhere (for singlular block_index index access)
        ("block_index", pymongo.ASCENDING),
        ("base_asset", pymongo.ASCENDING),
        ("quote_asset", pymongo.ASCENDING)
    ])

    #balance_changes
    mongo_db.balance_changes.ensure_index('block_index')
    mongo_db.balance_changes.ensure_index([
        ("address", pymongo.ASCENDING),
        ("asset", pymongo.ASCENDING),
        ("block_time", pymongo.ASCENDING)
    ])
    #asset_market_info
    mongo_db.asset_market_info.ensure_index('asset', unique=True)
    #asset_marketcap_history
    mongo_db.asset_marketcap_history.ensure_index('block_index')
    mongo_db.asset_marketcap_history.ensure_index([ #events.py
        ("market_cap_as", pymongo.ASCENDING),
        ("asset", pymongo.ASCENDING),
        ("block_index", pymongo.DESCENDING)
    ])
    mongo_db.asset_marketcap_history.ensure_index([ #api.py
        ("market_cap_as", pymongo.ASCENDING),
        ("block_time", pymongo.DESCENDING)
    ])
    #asset_pair_market_info
    mongo_db.asset_pair_market_info.ensure_index([ #event.py, api.py
        ("base_asset", pymongo.ASCENDING),
        ("quote_asset", pymongo.ASCENDING)
    ], unique=True)
    mongo_db.asset_pair_market_info.ensure_index('last_updated')
    #asset_extended_info
    mongo_db.asset_extended_info.ensure_index('asset', unique=True)
    mongo_db.asset_extended_info.ensure_index('info_status')
    #btc_open_orders
    mongo_db.btc_open_orders.ensure_index('when_created')
    mongo_db.btc_open_orders.ensure_index('order_tx_hash', unique=True)
    #transaction_stats
    mongo_db.transaction_stats.ensure_index([ #blockfeed.py, api.py
        ("when", pymongo.ASCENDING),
        ("category", pymongo.DESCENDING)
    ])
    mongo_db.transaction_stats.ensure_index('block_index')
    #wallet_stats
    mongo_db.wallet_stats.ensure_index([
        ("when", pymongo.ASCENDING),
        ("network", pymongo.ASCENDING),
    ])
    
    ##COLLECTIONS THAT ARE *NOT* PURGED AS A RESULT OF A REPARSE
    #preferences
    mongo_db.preferences.ensure_index('wallet_id', unique=True)
    mongo_db.preferences.ensure_index('network')
    mongo_db.preferences.ensure_index('last_touched')
    #login_history
    mongo_db.login_history.ensure_index('wallet_id')
    mongo_db.login_history.ensure_index([
        ("when", pymongo.DESCENDING),
        ("network", pymongo.ASCENDING),
        ("action", pymongo.ASCENDING),
    ])

    last_wallet_message = mongo_db.wallet_messages.find_one(sort=[("_id", pymongo.DESCENDING)])
    if not last_wallet_message:
        mongo_db.wallet_messages.insert({
            '_id': 0,
            'when': time.mktime(time.gmtime()),
            'message': None,
        })
    config.cw_last_message_seq = last_wallet_message['_id'] if last_wallet_message else 0
    logging.debug("cw_last_message_seq: {}".format(config.cw_last_message_seq))
    #chat_handles
    mongo_db.chat_handles.ensure_index('wallet_id', unique=True)
    mongo_db.chat_handles.ensure_index('handle', unique=True)
    #chat_history
    mongo_db.chat_history.ensure_index('when')
    mongo_db.chat_history.ensure_index([
        ("handle", pymongo.ASCENDING),
        ("when", pymongo.DESCENDING),
    ])
    #feeds
    mongo_db.feeds.ensure_index('source')
    mongo_db.feeds.ensure_index('owner')
    mongo_db.feeds.ensure_index('category')
    mongo_db.feeds.ensure_index('info_url')

    #mempool
    mongo_db.mempool.ensure_index('tx_hash')

    #Connect to redis
    if config.REDIS_ENABLE_APICACHE:
        logging.info("Enabling redis read API caching... (%s:%s)" % (config.REDIS_CONNECT, config.REDIS_PORT))
        redis_client = redis.StrictRedis(host=config.REDIS_CONNECT, port=config.REDIS_PORT, db=config.REDIS_DATABASE)
    else:
        redis_client = None
    
    logging.info("Starting up energypartyd block feed poller...")
    gevent.spawn(blockfeed.process_cpd_blockfeed)

    #start up event timers that don't depend on the feed being fully caught up
    logging.debug("Starting event timer: expire_stale_prefs")
    gevent.spawn(events.expire_stale_prefs)
    logging.debug("Starting event timer: expire_stale_btc_open_order_records")
    gevent.spawn(events.expire_stale_btc_open_order_records)
    logging.debug("Starting event timer: generate_wallet_stats")
    gevent.spawn(events.generate_wallet_stats)

    logging.info("Starting up RPC API handler...")
    api.serve_api(mongo_db, redis_client)

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

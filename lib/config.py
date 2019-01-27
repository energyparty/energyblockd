# Copyright (c) 2016-Present Energyparty Developers
# Distributed under the AGPL 3.0 with the OpenSSL exception, see the
# accompanying file LICENSE or <https://github.com/energyparty/energyblockd/>.

# -*- coding: utf-8 -*-
VERSION = "1.4.0"

DB_VERSION = 23

CAUGHT_UP = False

UNIT = 1000000

SUBDIR_ASSET_IMAGES = "asset_img" #goes under the data dir and stores retrieved asset images
SUBDIR_FEED_IMAGES = "feed_img" #goes under the data dir and stores retrieved feed images

MARKET_PRICE_DERIVE_NUM_POINTS = 8 #number of last trades over which to derive the market price (via WVAP)

REGULAR_DUST_SIZE = 1000
MULTISIG_DUST_SIZE = 2000
ORDER_BTC_DUST_LIMIT_CUTOFF = MULTISIG_DUST_SIZE

mongo_db = None #will be set on server init

BTC = 'ENRG'
XCP = 'XEP'

MAX_REORG_NUM_BLOCKS = 15
MAX_FORCED_REORG_NUM_BLOCKS = 25

ARMORY_UTXSVR_PORT_MAINNET = 6590
ARMORY_UTXSVR_PORT_TESTNET = 6591
FUZZY_MAX_WALLET_MESSAGES_STORED = 1000

DEFAULT_BACKEND_PORT_TESTNET = 42705
DEFAULT_BACKEND_PORT = 22705

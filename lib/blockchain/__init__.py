# Copyright (c) 2016-Present Energyparty Developers
# Distributed under the AGPL 3.0 with the OpenSSL exception, see the
# accompanying file LICENSE or <https://github.com/energyparty/energyblockd/>.

'''
Proxy API to make queries to popular blockchains explorer
'''
import sys

from lib import config
from lib.blockchain import addrindex

# http://test.insight.is/api/sync
def check():
    return sys.modules['lib.blockchain.{}'.format(config.BLOCKCHAIN_SERVICE_NAME)].check()

# http://test.insight.is/api/status?q=getInfo
def getinfo():
    return sys.modules['lib.blockchain.{}'.format(config.BLOCKCHAIN_SERVICE_NAME)].getinfo()

def listunspent(address):
    return sys.modules['lib.blockchain.{}'.format(config.BLOCKCHAIN_SERVICE_NAME)].listunspent(address)

def getaddressinfo(address):
    return sys.modules['lib.blockchain.{}'.format(config.BLOCKCHAIN_SERVICE_NAME)].getaddressinfo(address)

def gettransaction(tx_hash):
    return sys.modules['lib.blockchain.{}'.format(config.BLOCKCHAIN_SERVICE_NAME)].gettransaction(tx_hash)

def get_pubkey_for_address(address):
    """attempts to get the public key from an address. the address must have at least made one transaction"""
    return sys.modules['lib.blockchain.{}'.format(config.BLOCKCHAIN_SERVICE_NAME)].get_pubkey_for_address(address)

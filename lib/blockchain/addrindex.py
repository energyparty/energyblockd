# Copyright (c) 2016-Present Energyparty Developers
# Distributed under the AGPL 3.0 with the OpenSSL exception, see the
# accompanying file LICENSE or <https://github.com/energyparty/energyblockd/>.

import time
import json

from lib import config, util, util_bitcoin

def bitcoind_rpc(command, params):
    return util.call_jsonrpc_api(command, 
                            params = params,
                            endpoint = config.BACKEND_URL_NOAUTH, 
                            auth = config.BACKEND_AUTH, 
                            abort_on_error = True)['result']

def getinfo():
    return {'info': bitcoind_rpc('getinfo', None)}

def searchrawtransactions(address):
    return bitcoind_rpc('searchrawtransactions', [address, 1, 0, 9999999])

def gettransaction(tx_hash):
    return bitcoind_rpc('gettransaction', [tx_hash])

def get_unspent_txouts(source):
    return util.call_jsonrpc_api("get_unspent_txouts", {'address': source}, abort_on_error=True)['result']

def listunspent(address):
    outputs = get_unspent_txouts(address)
    utxo = []
    for txo in outputs:
        newtxo = {
            'address': address,
            'txid': txo['txid'],
            'vout': txo['vout'],
            'ts': 0,
            'scriptPubKey': txo['scriptPubKey'],
            'amount': str(txo['amount']),
            'confirmations': txo['confirmations'],
            'confirmationsFromCache': False
        }
        utxo.append(newtxo)
    return utxo

def getaddressinfo(address):
    confirmed_unspent = get_unspent_txouts(address)
    balance = sum(out['amount'] for out in confirmed_unspent)
    balance = round(balance,6)
    transactions = []
    results = {
        'addrStr': address,
        'balance': balance,
        'balanceSat': str(balance * config.UNIT),
        'transactions': transactions
    }
    try:
      raw_transactions = reversed(searchrawtransactions(address))
    except Exception:
      return results

    for tx in raw_transactions:
        if 'confirmations' in tx and tx['confirmations'] > 0:
            transactions.append(tx['txid'])

    return results

def get_pubkey_from_transactions(address, raw_transactions):
    for tx in raw_transactions:
        for vin in tx['vin']:
            scriptsig = vin['scriptSig']
            asm = scriptsig['asm'].split(' ')
            pubkey_hex = asm[1]
            try:
                if util_bitcoin.pubkey_to_address(pubkey_hex) == address:
                    return pubkey_hex
            except:
                pass
    return None

def get_pubkey_for_address(address):
    addresses = [address]  
    pubkeys = []

    for address in addresses:
        raw_transactions = searchrawtransactions(address)
        pubkey = get_pubkey_from_transactions(address, raw_transactions)
        if pubkey: pubkeys.append(pubkey)
    return pubkeys

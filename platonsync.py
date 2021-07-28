# Indexer for Ethereum to get transaction list by ETH address
# https://github.com/Adamant-im/ETH-transactions-storage
# By Artem Brunov, Aleksei Lebedev. (c) ADAMANT TECH LABS
# v. 1.1

from web3 import HTTPProvider, Web3, exceptions
from eth import PlatON
from hexbytes import HexBytes
import psycopg2
import time
import sys
import logging

import configparser

cp=configparser.ConfigParser()
cp.read("config.ini")
node_address = cp.get('base','node_address')
log_file_path = cp.get('base','log_file_path')
start_block_number = cp.get('base','start_block_number')

db_host = cp.get('db','host')
db_user = cp.get('db','user')
db_password = cp.get('db','password')

# Get postgre database name
if len(sys.argv) < 2:
    print('Add postgre database name as an argument')
    exit()

dbname = sys.argv[1]

# Connect to geth node
#web3 = Web3(Web3.IPCProvider("/home/geth/.ethereum/geth.ipc"))

# Or connect to parity node
#web3 = Web3(Web3.IPCProvider("/data/platon/data/jsonrpc.ipc"))
web3 = Web3(HTTPProvider(node_address))
platon = PlatON(web3)
# web3 = Web3(Web3.providers.HttpProvider("http://172.16.214.21:8547"))

# Start logger
logger = logging.getLogger("PlatONIndexerLog")
logger.setLevel(logging.INFO)
lfh = logging.FileHandler(log_file_path)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
lfh.setFormatter(formatter)
logger.addHandler(lfh)

try:
    conn = psycopg2.connect(dbname=dbname, host=db_host,  user=db_user, password=db_password)
    conn.autocommit = True
    logger.info("Connected to the database")
except Exception as e:
    print ('str(Exception):\t'+str(Exception))
    print ('str(e):\t\t'+str(e))
    print ('repr(e):\t'+ repr(e))
    print ('e.message:\t'+ e.message)
    print ('traceback.print_exc():'+ traceback.print_exc())
    print ('traceback.format_exc():\n%s' +traceback.format_exc())
    logger.error("Unable to connect to database")

# Delete last block as it may be not imparted in full
cur = conn.cursor()
cur.execute('DELETE FROM public.platontxs WHERE block = (SELECT Max(block) from public.platontxs)')
cur.close()
conn.close()

# Adds all transactions from Ethereum block
def insertion(blockid, tr):
    time = platon.getBlock(hex(blockid))['timestamp']
    for x in range(0, tr):
        trans = platon.getTransactionByBlock(hex(blockid), hex(x))
        txhash = trans['hash']
        value = trans['value']
        inputinfo = trans['input']
        # Check if transaction is a contract transfer
        if (value == 0 and not inputinfo.startswith('0xa9059cbb')):
            continue
        fr = trans['from']
        to = trans['to']
        gasprice = trans['gasPrice']
        gas = platon.getTransactionReceipt(trans['hash'])['gasUsed']
        contract_to = ''
        contract_value = ''
        # Check if transaction is a contract transfer
        if inputinfo.startswith('0xa9059cbb'):
            contract_to = inputinfo[10:-64]
            contract_value = inputinfo[74:]
        #print('time ' + str(int(time,0)))
        #print('fr '+fr)
        #print('to '+to)
        #print('value '+value + ' ' + str(int(value,0)))
        #print('gas '+gas + ' ' + str(int(gas,0)))
        #print('gasprice '+gasprice + ' ' + str(int(gasprice,0)))
        #print('blockid '+str(blockid))
        #print('txhash '+txhash)
        #print('contract_to '+contract_to)
        #print('contract_value '+contract_value)
        if fr is not None:
            fr=fr.lower()
        if to is not None:
            to = to.lower()
        if txhash is not None:
            txhash = txhash.lower()
        if contract_to is not None:
            contract_to=contract_to.lower()
        cur.execute(
            'INSERT INTO public.platontxs(time, txfrom, txto, value, gas, gasprice, block, txhash, contract_to, contract_value) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
            (int(time,0)/ 1000, fr, to, int(value,0), int(gas,0), int(gasprice,0), blockid, txhash, contract_to, contract_value))

# Fetch all of new (not in index) Ethereum blocks and add transactions to index
while True:
    try:
        conn = psycopg2.connect(dbname=dbname, host=db_host,  user=db_user, password=db_password)
        conn.autocommit = True
    except:
        logger.error("Unable to connect to database")

    cur = conn.cursor()

    cur.execute('SELECT Max(block) from public.platontxs')
    maxblockindb = cur.fetchone()[0]
    # On first start, we index transactions from a block number you indicate. 46146 is a sample.
    if maxblockindb is None:
        maxblockindb = int(start_block_number)
    if maxblockindb < int(start_block_number):
        maxblockindb = int(start_block_number)

    endblock = int(platon.blockNumber, 0)
    print(endblock)

    logger.info('Current best block in index: ' + str(maxblockindb) + '; in Ethereum chain: ' + str(endblock))

    for block in range(maxblockindb + 1, endblock):
        #print(block)
        transactions = platon.getBlockTransactionCount(hex(block))
        #print(int(transactions,0))
        if int(transactions, 0) > 0:
            insertion(block, int(transactions, 0))
            print('insertion block ' + str(block))
        else:
            logger.info('Block ' + str(block) + ' does not contain transactions')
            print('Block ' + str(block) + ' does not contain transactions')
    cur.close()
    conn.close()
    time.sleep(20)

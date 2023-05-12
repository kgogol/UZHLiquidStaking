import os
from web3 import Web3
import pandas as pd
from hexbytes import HexBytes
from typing import List, Tuple, Dict, Any
from eth_utils import decode_hex

w3 = Web3(Web3.HTTPProvider("http://127.0.0.1:8547"))#

#1 get log
def get_logs(topics:list,address,fromBlock:int,toBlock:int):
    logs=w3.eth.getLogs({
                                "topics":topics,
                                "address": w3.toChecksumAddress(address),
                                "fromBlock": fromBlock,
                                "toBlock": toBlock,
                            } )
    return logs# attributedict

#2. parse data field in log

def parse_data(event_name,data: str):
    
    data_bytes = HexBytes(data)
    if event_name=='Mint':
        addr=Web3.toChecksumAddress('0x'+data[0:32].hex()[-40:])
        num1 = int(data[32:64].hex(), 16)
        num2 = int(data[64:96].hex(), 16)
        num3 = int(data[96:].hex(), 16)
        data_dict = {
            'sender': addr,
            'amount': num1,
            'amount0': num2,
            'amount1': num3
        }
    elif event_name=='Swap':
        amount0=int(data[0:32].hex(), 16)
        if amount0 & (1 << (len(data[0:32].hex()) * 4 - 1)): 
            amount0 = amount0 - (1 << (len(data[0:32].hex()) * 4))
        amount1 = int(data[32:64].hex(), 16)
        if amount1 & (1 << (len(data[32:64].hex()) * 4 - 1)):
            amount1 = amount1 - (1 << (len(data[32:64].hex()) * 4))
        sqrtPriceX96 = int(data[64:96].hex(), 16)
        liquidity = int(data[96:128].hex(), 16)
        tick=int(data[128:].hex(), 16)
        if tick & (1 << (len(data[128:].hex()) * 4 - 1)):
            tick = tick - (1 << (len(data[128:].hex()) * 4))
        data_dict = {
            'amount0': amount0,
            'amount1': amount1,
            'sqrtPriceX96': sqrtPriceX96,
            'liquidity': liquidity,
            'tick': tick,
        }
    
    elif event_name=='Burn':
        amount=int(data[0:32].hex(), 16)
        if amount & (1 << (len(data[0:32].hex()) * 4 - 1)): 
            amount= amount - (1 << (len(data[0:32].hex()) * 4))
        amount0 = int(data[32:64].hex(), 16)
        if amount0 & (1 << (len(data[32:64].hex()) * 4 - 1)):
            amount0 = amount0 - (1 << (len(data[32:64].hex()) * 4))
        amount1 = int(data[64:96].hex(), 16)
        if amount1 & (1 << (len(data[64:96].hex()) * 4 - 1)):
            amount1= amount1 - (1 << (len(data[64:96].hex()) * 4))
        data_dict = {
            'amount': amount,
            'amount0': amount0,
            'amount1': amount1,
        }
    return data_dict

#3. combine all the fields in log together
def log_to_dataframe(event_name,logs: List[Dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for log in logs:
        data = decode_hex(log['data'])
        data = parse_data(event_name,data)
        row = {
            'Address': log['address'],
            'BlockNumber': log['blockNumber'],
            'TransactionHash': log['transactionHash'].hex(),
            'LogIndex': log['logIndex'],
            'Removed': log['removed'],
            'Event':event_name,
            **data
        }
        rows.append(row)
    return pd.DataFrame(rows)
# logs=get_logs(['0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde'],w3.toChecksumAddress('0xD340B57AAcDD10F96FC1CF10e15921936F41E29c'),12376093,17034804)
# res=log_to_dataframe('Mint',logs)
# res.to_csv('/home/user/yan/ETH/python/liquidity_tokens_uniswapv3/test.csv')

if __name__ == '__main__':
    pairs={'0xa3f558aebaecaf0e11ca4b2199cc5ed341edfd74':{'LDO_WETH':12512163}, #no api on etherscan
       '0xbea615376d1184f3670a341b70f6f45d9d0fbaad':{'cbETH_WETH':15403832},
       '0xe42318ea3b998e8355a3da364eb9d48ec725eb45':{'WETH_RPL':13598687},
       '0x15d12305d0f57c99d947e66d4164094ffccf78fc':{'SWISE_WETH':15172021},
       '0x63818bbdd21e69be108a23ac1e84cbf66399bd7d':{'stETH_WETH':14937573},
       '0x1241f4a348162d99379a23e73926cf0bfcbf131e':{'ANKR_WETH':12556816},
       '0x794f685b0eab894aedad5c3d9846739d3f4a11e7':{'SD_WETH':16318683},
       '0xd340b57aacdd10f96fc1cf10e15921936f41e29c':{'wstETH_WETH':12376093},
       '0xa4e0faa58465a2d369aa21b3e42d43374c6f9613':{'rETH_WETH':14155364},
       '0x840deeef2f115cf50da625f7368c24af6fe74410':{'cbETH_WETH':15404282},
       '0x8a15b2dc9c4f295dcebb0e7887dd25980088fdcb':{'fraxETH_ETH':16642896},
       '0x7379e81228514a1d2a6cf7559203998e20598346':{'ETH_sETH2':12673737}}

    topics={
            "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67":"Swap"}
    block_interval=1000
    for address,name_created_block in pairs.items():
        print(address)
        pair_name=list(name_created_block.keys())[0]
        for topic,event in topics.items():
            creation_block=list(name_created_block.values())[0]
            if 17034804-creation_block<block_interval:
                end_block=17034804
            else:
                end_block=creation_block+block_interval
            while True: 
                print(creation_block,end_block)
                logs=get_logs([topic],w3.toChecksumAddress(address),creation_block,end_block)
                res=log_to_dataframe(event,logs)
                res.to_csv(f'/local/scratch/nft_data_TY/Uniswap_v3_{pair_name}_{event}_data.csv',mode='a')
                if end_block>=17034804:
                    break
                if end_block+block_interval>17034804:
                    creation_block=end_block
                    end_block=17034804
                elif end_block+block_interval<=17034804:
                    creation_block=end_block
                    end_block=end_block+block_interval
                

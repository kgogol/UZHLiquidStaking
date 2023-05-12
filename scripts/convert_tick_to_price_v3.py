import pandas as pd



pairs={#'0xa3f558aebaecaf0e11ca4b2199cc5ed341edfd74':{'LDO/WETH':12512163}, #no api on etherscan
       #'0xbea615376d1184f3670a341b70f6f45d9d0fbaad':{'cbETH/WETH':15403832},
       '0xe42318ea3b998e8355a3da364eb9d48ec725eb45':{'WETH/RPL':13598687}, #no api on etherscan
       #'0x8a15b2dc9c4f295dcebb0e7887dd25980088fdcb':{'frxETH/WETH':16642896}, 
       '0x15d12305d0f57c99d947e66d4164094ffccf78fc':{'SWISE/WETH':15172021},
       '0x63818bbdd21e69be108a23ac1e84cbf66399bd7d':{'stETH/WETH':14937573},#no api on etherscan
       '0x1241f4a348162d99379a23e73926cf0bfcbf131e':{'ANKR/WETH':12556816}, #sushiswap
       '0x794f685b0eab894aedad5c3d9846739d3f4a11e7':{'SD/WETH':16318683},
       '0xd340b57aacdd10f96fc1cf10e15921936f41e29c':{'wstETH/WETH':12376093},#no api on etherscan
       '0xa4e0faa58465a2d369aa21b3e42d43374c6f9613':{'rETH/WETH':14155364},#no api on etherscan
       '0x840deeef2f115cf50da625f7368c24af6fe74410':{'cbETH/WETH':15404282},
       '0x8a15b2dc9c4f295dcebb0e7887dd25980088fdcb':{'fraxETH/ETH':12378320},
       '0x7379e81228514a1d2a6cf7559203998e20598346':{'ETH/sETH2':12673737},#no api on etherscan
}

df=pd.read_csv('/local/scratch/exported/Ethereum_token_txs_data/nft_data_TY/Uniswap_v3_stETH_WETH_Swap_data.csv')

from web3 import Web3, HTTPProvider
import pandas as pd
from eth_defi.uniswap_v3.pool import fetch_pool_details

web3 = Web3(HTTPProvider('http://127.0.0.1:8547'))
def convert_price(row,token0):
        # USDC/WETH pool has reverse token order, so let's flip it WETH/USDC
        tick = row["tick"]
        if token0=='WETH' or token0=="ETH":
            reverse_token_order=True
        else:
            reverse_token_order=False
        return pool_details.convert_price_to_human(tick, reverse_token_order=reverse_token_order)


    
pool_address = Web3.to_checksum_address('0x63818bbdd21e69be108a23ac1e84cbf66399bd7d')
pool_details = fetch_pool_details(web3, pool_address)
df["price"] = df.apply(convert_price,token0=pool_details.token0, axis=1) 


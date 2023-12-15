
import pandas as pd
from urllib.request import urlopen
import requests

# function to use requests.post to make an API call to the subgraph url
def run_query(query):

    # endpoint where you are making the request
    request = requests.post('https://api.thegraph.com/subgraphs/name/lidofinance/lido'
                            '',
                            json={'query': query})
    if request.status_code == 200:
        return request.json()
    else:
        raise Exception('Query failed. return code is {}.      {}'.format(request.status_code, query))

## revise the timestamp of {createdAtBlockNumber_lt:10288355}, increase this timestamp
query_init = '''
{
  totalRewards(first: 1000, orderBy: block, orderDirection: desc,where: {block_gt:17300000}) #16070181
  {
    totalRewardsWithFees
    totalPooledEtherBefore
    totalPooledEtherAfter
    totalSharesBefore
    totalSharesAfter
    block
    blockTime
    transactionIndex
    totalRewards
  }
}
''' 


query_iter = '''
{
 totalRewards(first: 1000, orderBy: block, orderDirection: desc, where: {block_lt:initial}) 
 {
    totalRewardsWithFees
    totalPooledEtherBefore
    totalPooledEtherAfter
    totalSharesBefore
    totalSharesAfter
    block
    blockTime
    transactionIndex
    totalRewards
  }
}
''' 

pair_frame = [] 

query = query_init
result = run_query(query)
for totalReward in result['data']['totalRewards']:
    pair_frame.append(totalReward)

last_block = result['data']['totalRewards'][-1]['block']
query_iter = query_iter.replace('initial',last_block)
query = query_iter

try:
    while(1):
        result = run_query(query_iter)
        for totalReward in result['data']['totalRewards']:
            pair_frame.append(totalReward)
        query_iter = query_iter.replace(last_block,result['data']['totalRewards'][-1]['block'])
        if( int(result['data']['totalRewards'][-1]['block'])<10073216):#11473216
          break
        last_block = result['data']['totalRewards'][-1]['block']
        print(last_block)
        

except Exception as e:
    try:
      print(result['errors'])
    except:
      print(e)
    df = pd.json_normalize(pair_frame)
    df.to_csv('/home/user/yan/ETH/python_web3/lsd_staking/stETH_reward_data.csv',mode='a',index=False)




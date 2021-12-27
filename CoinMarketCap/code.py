import requests
from requests import Session
import tokenkey
from prettyprinter import pprint as pp
import pandas as pd

# Test Basic Request
url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/map'
headers = {
  'Accepts': 'application/json',
  'X-CMC_PRO_API_KEY': tokenkey.API_TOKEN,
}
# r = requests.get(url, headers=headers)
# pp(r.json()['data'][0:5])

# Create object so we can easily access the API
class CMC:
    def __init__(self, token):
        self.apiurl = 'https://pro-api.coinmarketcap.com/'
        self.headers = {'Accepts': 'application/json', 'X-CMC_PRO_API_KEY': token}
        self.session = Session()
        self.session.headers.update(self.headers) # To make session always update the headers and set to default
    
    def getAllCoins(self, limit=5000):
        url = self.apiurl + 'v1/cryptocurrency/map'
        parameters = {'limit':limit}
        r = self.session.get(url, params=parameters)
        data = r.json()['data']
        return data
    
    def getLastestPrice(self, limit=100):
        url = self.apiurl + 'v1/cryptocurrency/listings/latest'
        parameters = {'limit':limit}
        r = self.session.get(url, params=parameters)
        data = r.json()['data']
        return data
    
    def getSpecific(self, symbol='BTC'):
        url = self.apiurl + 'v1/cryptocurrency/quotes/latest'
        parameters = {'symbol':symbol}
        r = self.session.get(url, params=parameters)
        data = r.json()['data']
        return data
    
    def savetodf(self, data, record_prefix=True):
        return pd.json_normalize(data, record_prefix=record_prefix)
        
        
    
    

allcoin = CMC(tokenkey.API_TOKEN)
test = allcoin.getAllCoins(10)
df = allcoin.savetodf(test)
print(df)
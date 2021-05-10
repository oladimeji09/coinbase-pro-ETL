import hmac, hashlib, time, requests, base64, pandas as pd, sys, env
from requests.auth import AuthBase

items = []
baseURL = 'https://api.pro.coinbase.com/'
creds=(('api_key','secret_key', 'passphrase'),
('api_key','secret_key','passphrase'))

# Create custom authentication for Exchange
class CoinbaseExchangeAuth(AuthBase):
    def __init__(self, api_key, secret_key, passphrase):
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase

    def __call__(self, request):
        timestamp = str(time.time())
        message = timestamp + request.method + request.path_url + (request.body or b'').decode()
        hmac_key = base64.b64decode(self.secret_key)
        signature = hmac.new(hmac_key, message.encode(), hashlib.sha256)
        signature_b64 = base64.b64encode(signature.digest()).decode()

        request.headers.update({
            'CB-ACCESS-SIGN': signature_b64,
            'CB-ACCESS-TIMESTAMP': timestamp,
            'CB-ACCESS-KEY': self.api_key,
            'CB-ACCESS-PASSPHRASE': self.passphrase,
            'Content-Type': 'application/json'
        })
        return request

def get_profiles():
    """Return active profiles"""
    profiles =[]
    for item in request('profiles'):
        if item['active'] == True:
             profiles.append([item['id'],item['name']])
    return profiles

def request(end_point, a=creds[0][0],b=creds[0][1],c=creds[0][2]):
    resp=requests.get(baseURL+end_point, auth=CoinbaseExchangeAuth(a,b,c))
    return resp.json()

def get_ids():
    """Get a list of product ids"""
    ids = []
    for i in list(request('accounts')):
        ids.append(i['currency']+'-EUR')
        ids.append(i['currency']+'-GBP')
    return ids

def get_fills():
    """Get a list of transactions"""
    for i in creds: #loop through all profiles
        for e in get_ids():   #loop through all product_ids
            print("Geting data for {} using {} account.".format(e,i[2]))
            time.sleep(1)
            for item in request('fills?product_id={}'.format(e),i[0],i[1],i[2]): #loop through all product_ids
                if 'message' not in item:
                    if item:
                        items.append(item)
    return items

def transform():
    """Transform data and export to google sheets"""
    df= pd.DataFrame()
    df = df.append(pd.io.json.json_normalize(get_fills()))
    df[['ticker','currency']] = df.product_id.str.split('-',expand=True)
    for i in  get_profiles():
        df.loc[df.profile_id == i[0], ['profile_id']] = i[1]
    df['load_date']= pd.Timestamp.now().strftime('%Y-%m-%d  %H:%M:%S')
    df.created_at=pd.to_datetime(df.created_at).dt.strftime('%Y-%m-%d  %H:%M:%S')
    df = df.sort_values(['created_at'], ascending=True )
    env.rep_data_sh(df,'google sheet id','Crypto: Data',df.shape[1])
    return "Coinbase job completed successfully"

results = transform()

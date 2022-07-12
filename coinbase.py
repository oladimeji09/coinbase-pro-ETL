#@auto-fold regex /./
import hmac, hashlib, time, requests, base64, pandas as pd, sys,json
from python_helpers import python_helper as ph
from python_helpers import google_helper as gh
from requests.auth import AuthBase

baseURL = 'https://api.pro.coinbase.com/'
creds = json.load(open(ph.root_fp+'/creds/creds.json')).get('coinbase')


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

def request(end_point:str, cred:json):
    resp=requests.get(baseURL+end_point, auth=CoinbaseExchangeAuth(cred.get('api_key'),cred.get('secret_key'),cred.get('passphrase')))
    return resp.json()

def get_profiles(cred):
    """Return active profiles"""
    cred = creds.get('portfolio_1')
    profiles =[]
    for item in request('profiles', cred):
        if item['active'] == True:
             profiles.append([item['id'],item['name']])
    return profiles

def get_ids(cred):
    """Get a list of product ids"""
    ids = []
    for i in list(request('accounts', cred)):
        ids.append(i['currency']+'-EUR')
        ids.append(i['currency']+'-GBP')
    return ids

def get_fills(cred):
    """Get a list of transactions"""
    # TODO: find a smart way to get fills, that doesn't require querying every product combo
    transactions = []
    products = get_ids(cred)
    for product in products:   #loop through all product_ids
        print("Geting transactions for {}".format(product))
        time.sleep(1)
        for line in request('fills?product_id={}'.format(product),cred): #
            if 'message' not in line:
                if line:
                    transactions.append(line)
    return transactions

def transform(cred):
    """Transform data and export to google sheets"""
    df = pd.DataFrame()
    profiles = get_profiles(creds)
    for key,value in creds.items():
        print('executing transform for'+ key)
        df = df.append(pd.json_normalize(get_fills(value)))
        df[['ticker','currency']] = df.product_id.str.split('-',expand=True)
        for i in  profiles:
            df.loc[df.profile_id == i[0], ['profile_id']] = i[1]
        df['load_date']= pd.Timestamp.now().strftime('%Y-%m-%d  %H:%M:%S')
        df.created_at = pd.to_datetime(df.created_at)
        # df.created_at = pd.to_datetime(df.created_at).dts.strftime('%Y-%m-%d  %H:%M:%S')
        df = df.sort_values(['created_at'], ascending=True )
    gh.rep_data_sh(df,'18HjRhb8maIt0ypGxSAmjz592_1oQZqEN0f915RlGsjw','Crypto: Data')
    return [df,"Coinbase job completed successfully"]

results = transform(creds)

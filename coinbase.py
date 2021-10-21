#@auto-fold regex /./
import hmac, hashlib, time, requests, base64, pandas as pd, sys
from requests.auth import AuthBase
sys.path.insert(0,'C:/Finance/projects')
import env,json

items = []
baseURL = 'https://api.pro.coinbase.com/'
creds = json.load(open(r'C:\Finance\projects\personal\creds.json')).get('coinbase')
base_creds = creds.get('portfolio_1')

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
    for item in request('profiles',base_creds.get('api_key'),
                    base_creds.get('secret_key'), base_creds.get('passphrase')):
        if item['active'] == True:
             profiles.append([item['id'],item['name']])
    return profiles

def request(end_point, api_key,secret_key,passphrase):
    resp=requests.get(baseURL+end_point, auth=CoinbaseExchangeAuth(api_key,secret_key,passphrase))
    return resp.json()

def get_ids():
    """Get a list of product ids"""
    ids = []
    for i in list(request('accounts',base_creds.get('api_key'),
                    base_creds.get('secret_key'),base_creds.get('passphrase') )):
        ids.append(i['currency']+'-EUR')
        ids.append(i['currency']+'-GBP')
    return ids

def get_fills():
    """Get a list of transactions"""
    for folio in creds: #loop through all portfolio
        for product in get_ids():   #loop through all product_ids
            print("Geting data for {} using {}.".format(product,folio))
            time.sleep(1)
            for item in request('fills?product_id={}'.format(product),creds.get(folio).get('api_key')
                                ,creds.get(folio).get('secret_key'),creds.get(folio).get('passphrase')): #
                if 'message' not in item:
                    if item:
                        items.append(item)
    return items

def transform():
    """Transform data and export to google sheets"""
    df = pd.json_normalize(get_fills())
    df[['ticker','currency']] = df.product_id.str.split('-',expand=True)
    for i in  get_profiles():
        df.loc[df.profile_id == i[0], ['profile_id']] = i[1]
    df['load_date']= pd.Timestamp.now().strftime('%Y-%m-%d  %H:%M:%S')
    df.created_at=pd.to_datetime(df.created_at).dt.strftime('%Y-%m-%d  %H:%M:%S')
    df = df.sort_values(['created_at'], ascending=True )
    env.rep_data_sh(df,'18HjRhb8maIt0ypGxSAmjz592_1oQZqEN0f915RlGsjw','Crypto: Data')
    return "Coinbase job completed successfully"

results = transform()

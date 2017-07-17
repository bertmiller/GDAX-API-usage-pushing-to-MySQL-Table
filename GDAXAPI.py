import gdax
import dateutil.parser as parser
import datetime
import _mysql

db = _mysql.connect("hostname","username","password","tablename")

instrument = 'BTC-USD'

start_text = '01/01/2016 00:00:00' #Start date
start_date = parser.parse(start_text)

hardend_text = '01/01/2016 10:00:00' # "hard" end date, or when you want your data to stop
hardend_date = parser.parse(hardend_text)

print('Attempting to get ' + instrument + ' data and push to SQL from ' + start_text + ' to ' + hardend_text)
while (start_date < hardend_date):
    if (start_date + datetime.timedelta(minutes = 50) > hardend_date):
        end_date = hardend_date
    else:
        end_date = start_date + datetime.timedelta(minutes = 50)

    public_client = gdax.PublicClient()
    ETHUSDdata = public_client.get_product_historic_rates(instrument, start = start_date, end = end_date, granularity = 15)

    for x in ETHUSDdata:
        time = x[0]
        low = "%.9f" % x[1]
        high = "%.9f" % x[2]
        open_str = "%.9f" % x[3]
        close = "%.9f" % x[4]
        volume = "%.9f" % x[5]
        query = "INSERT INTO BTCUSD." + instrument.replace("-","") + " (date, low, high, open, close, volume) VALUES (\'" + datetime.datetime.fromtimestamp(int(x[0])).strftime('%Y-%m-%d %H:%M:%S') + "\', \'" + low + "\' , \'" + high + "\' , \'" + open_str + "\' , \'" + close + "\' , \'" + volume + "\')";
        print(query)
        db.query(query)

    start_date = end_date + datetime.timedelta(minutes = 1)


print("All done!")

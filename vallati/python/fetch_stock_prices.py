#!/usr/bin/env python

##
# Retrieves last minute prices for the stock tickers defined in tikers[]. The
# retrieved prices are pushed to the corresponding stock metric in Gnocchi DB.
##
import pyEX
import json
import urllib.request
from datetime import datetime

__author__ = "Leonardo Turchetti, Lorenzo Tonelli, Ludovica Cocchella and Rambod Rahmani"
__copyright__ = "Copyright (C) 2021 Leonardo Turchetti, Lorenzo Tunelli, Ludovica Cocchella and Rambod Rahmani"
__license__ = "GPLv3"

# pyEX engine
c = pyEX.Client("pk_e58014e8a6bd415d8af6e459f2353eb5")

# keystone token
keystone_token = "gAAAAABgqo4EBJerAKtAUGHMUyE93A7Vnoy3XUXZ7lLOGLXki_qXc0rCX6JqFXwFbbv_VrsMud-VEjyGu5KLQDp5NEZpHIExnVhdx6BIVkRAPGdIqKyRV5KJ4ycOLiuwjVkZGurhmvHPCfL1WO6veSc0Gk3D0AsuYPX-dAwGEQbPCN23o5dC9nk"

# stock tickers to be retrieved
tickers = ["AMZN", "GOOG", "BKNG", "CHTR", "NVDA", "TSLA", "NFLX", "ADBE", \
        "FB", "MSFT", "PYPL", "ADSK", "AMD", "EBAY", "FOX"];
tickers_metrics = ["a34a5ffb-a616-4583-99df-73885cbac719", \
                "06a298cc-aa96-47b0-ac3a-4242536dda0e", \
                "64499075-fcc6-4a12-bf8f-8fc6e7b5d20d", \
                "606f0e27-7a9c-446b-8d8e-32283eeb3524", \
                "955bf952-0f22-4a33-828b-e3046bcceb51", \
                "ed8da2ec-fc58-48af-92f8-b746cef30a2d", \
                "8f7cd857-b88a-4eb8-9ad7-ad361dea71ce", \
                "e2bf842c-f010-44a5-8b91-510c524f8eee", \
                "b634fe45-2fec-47b5-8816-c0aa672e90af", \
                "e7a19eaa-1590-4d1d-aa98-589a3a5862f8", \
                "d4007b77-197a-4b17-b2f2-54ee97331ae0", \
                "1b089d84-b79b-4309-868e-0d22019e7d63", \
                "266d9a2f-4749-4a6b-9a7c-57315c1d331d", \
                "233cce21-8ad9-4b57-a02d-1d6a581a3fbf", \
                "1f1f7244-bdda-456a-ad6f-7464c2c8645c"]

while True:
    for i in range(len(tickers)):
        try:
            # retrieve current UTC timestamp
            timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

            # retrieve last second stock price
            price = c.price(tickers[i])
            print(timestamp + " | " + tickers[i] + " - " + str(price))

            # check if the retrieved price is valid
            if (price > 0.0):
                # push data to gnocchi
                conditionsSetURL = "http://252.3.238.176:8041/v1/metric/" + tickers_metrics[i] + "/measures"
                newConditions = [{"timestamp": timestamp, "value": str(price)}]
                params = json.dumps(newConditions).encode('utf8')
                req = urllib.request.Request(conditionsSetURL, data=params, headers={'content-type': 'application/json', 'X-AUTH-TOKEN': keystone_token})
                urllib.request.urlopen(req)
        except Exception:
            print("Exception occurred while fetching stock market data. Ignoring.")
            pass

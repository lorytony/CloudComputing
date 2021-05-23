#!/usr/bin/env python

##
# Retrieves last minute prices for the stock tickers defined in tikers[]. The
# retrieved prices are pushed to the corresponding stock metric in Gnocchi DB.
##
import json
import datetime
import yfinance as yf
import urllib.request
import dateutil.parser

__author__ = "Leonardo Turchetti, Lorenzo Tonelli, Ludovica Cocchella and Rambod Rahmani"
__copyright__ = "Copyright (C) 2021 Leonardo Turchetti, Lorenzo Tunelli, Ludovica Cocchella and Rambod Rahmani"
__license__ = "GPLv3"

# keystone token
keystone_token = "gAAAAABgo3ljkfH41aeXCgwj1Ois7rLwtji13hI3MbAXpJ1sU0NJsYyVAntZWsTaMngwbVOCp4b5Q3YJJQucSUhks5aAaBHOvGVuSvEI1HB85609mynhGeEiOgIwHd7PrMQfj6v8sH5Ej7RxIx4tpgcIhNzD_yN1XlKoplbbKo5Dae4M2TmydEM"

# stock tickers to be retrieved
tickers = ["ENEL.MI", "ISP.MI", "STLA", "ENI.MI", "RACE.MI", "G.MI", "STM.MI", \
        "UCG.MI", "CNHI.MI", "EXO.MI", "SRG.MI", "PST.MI", "MONC.MI", "ATL.MI",\
        "TRN.MI"];
tickers_metrics = ["faafee03-ec51-4929-b530-0452eef75464", \
        "4042d720-7226-466c-b4aa-21e0a8ed2b96", \
        "88cf571d-8573-4da7-8dc5-71ae369d0c0b", \
        "bac224a0-4bb9-46ec-877b-9a0c1e2cf395", \
        "47d0b85c-d4d3-45c1-a58f-93fd8b6adfb6", \
        "40475954-1aa1-4c7c-889d-57cd7461ca50", \
        "c4aa045a-c02b-4dca-9969-0c95757a46ca", \
        "1af4592a-fc9c-4e2d-9789-d3e0029d0564", \
        "88f6ac70-e899-409c-9f0d-f0bbcbac9b29", \
        "3aad068e-aad2-4e51-8d63-35cb3499678c", \
        "9eb816af-144c-4a1b-a5d6-ee22092fee8a", \
        "86b51e5f-dca8-46ce-bff8-ccd7e74f7f8e", \
        "473e2213-234c-4865-b71b-7bd68ed7bbf8", \
        "014e1074-8277-4da7-acd2-57cfabc2277e", \
        "55a269c2-3119-4a2c-b57f-9cbbc00fddd2"]

while True:
    for i in range(len(tickers)):
        # retrieve last minute price
        msft = yf.Ticker(tickers[i])
        print("Processing: " + msft.info['shortName'])
        pricesDF = msft.history(period="1d", interval="1m")

        # yfinance api timeouts might result in empty dataframes
        if not pricesDF.empty:
            # extract new highest price and timestamp
            priceDateString = str(pricesDF.index[-1])
            priceValue = pricesDF['High'][-1]
            priceDate = dateutil.parser.isoparse(priceDateString)
            print(priceDate.strftime("%Y-%m-%dT%H:%M:%S") + " - " + str(priceValue))

            # push data to gnocchi
            conditionsSetURL = "http://252.3.238.176:8041/v1/metric/" + tickers_metrics[i] + "/measures"
            newConditions = [{"timestamp": priceDate.strftime("%Y-%m-%dT%H:%M:%S"), "value": priceValue}]
            params = json.dumps(newConditions).encode('utf8')
            req = urllib.request.Request(conditionsSetURL, data=params, headers={'content-type': 'application/json', 'X-AUTH-TOKEN': keystone_token})
            response = urllib.request.urlopen(req)
            print(response.read().decode('utf8'))

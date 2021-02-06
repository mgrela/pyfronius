#!/usr/bin/env python3

import asyncio
import aiohttp
import sys
import argparse
import time
from structlog import get_logger
import random
from types import SimpleNamespace
from urllib.parse import urlparse, urljoin
import json
from asyncio_mqtt.client import Client

from pyfronius.datamanager import DatamanagerClient

config = SimpleNamespace(
    # Datalogger settings
    datamanager_url = "http://datamanager",
    reconnect_delay = 30,
    fuzz_reconnect_delay = True,

    # MQTT settings
    broker_url = 'mqtt://localhost',
    topic_base = 'fronius'
)

async def main(config):
    log = get_logger()

    log.debug('starting', config=config)

    if config.topic_base[:-1] != '/':
        config.topic_base += '/'
        
    log = log.bind(datamanager_url=config.datamanager_url)

    try:    
        # Parse the broker URL to be an object
        parsed_broker_url = urlparse(config.broker_url)
        log = log.bind(broker_url=config.broker_url)
    except Exception as e:
        log.fatal('cannot parse broker url', broker_url=config.broker_url, exc=str(e))
        sys.exit(1)

    log.info('starting main loop')
    while True:
        try:

            log.info('connecting to MQTT broker', broker_url=config.broker_url, topic_base=config.topic_base)

            # TODO: How to support TLS in this library?
            async with Client(parsed_broker_url.netloc) as mqtt_client:

                log = log.bind(datamanager_url=config.datamanager_url)

                async with aiohttp.ClientSession() as session:

                    log.info("connecting to datamanager")        
                    datamanager_client = DatamanagerClient(aio_session=session, url=config.datamanager_url)
                    solarapi = await datamanager_client.solar_api

                    logger_info = await solarapi.logger_info()

                    log.debug('logger info', logger_info=logger_info.body)
                    
                    # Always add timestamp to retained messages
                    logger_info.body['LoggerInfo']['$ts'] = time.time()
                    await mqtt_client.publish(urljoin(config.topic_base, 'logger/info'), payload=json.dumps(logger_info.body['LoggerInfo']), qos=1, retain=True)

                    devices = await solarapi.active_devices()
                    log.info('active devices', devices=devices)

                    inv_info = await solarapi.inverter_info()
                    log.info('inverter info', inverters=inv_info)
                    for (id, inv) in inv_info.items():
                        inv['$ts'] = time.time()
                        await mqtt_client.publish(urljoin(config.topic_base, '{}/info'.format(id)), payload=json.dumps(inv), qos=1, retain=True)

                    while True:

                        powerflow = await solarapi.powerflow_realtime()
                        log.debug('realtime power flow', header=json.dumps(powerflow.head), powerflow=json.dumps(powerflow.body))

                        print(json.dumps(datamanager_client.data._data, indent=4))

                        # Energy counters
                        e_day = powerflow.body['Data']['Site']['E_Day']
                        e_total = powerflow.body['Data']['Site']['E_Total']
                        e_year = powerflow.body['Data']['Site']['E_Year']

                        await mqtt_client.publish(urljoin(config.topic_base, 'site/e_day'), payload=json.dumps(dict(v=e_day,u='Wh')), qos=1, retain=True)
                        await mqtt_client.publish(urljoin(config.topic_base, 'site/e_total'), payload=json.dumps(dict(v=e_total,u='Wh')), qos=1, retain=True)
                        await mqtt_client.publish(urljoin(config.topic_base, 'site/e_year'), payload=json.dumps(dict(v=e_year,u='Wh')), qos=1, retain=True)

                        pv_power = powerflow.body['Data']['Site']['P_PV']
                        if pv_power:
                            await mqtt_client.publish(urljoin(config.topic_base, 'site/pv_power'), payload=json.dumps(dict(v=pv_power,u='W')))
                    
                        await asyncio.sleep(30)
        except Exception as e:
            log.error('main loop exception', exc_info=e)

        delay = config.reconnect_delay
        if config.fuzz_reconnect_delay:
            delay = delay + (delay * random.uniform(-1,1) / 5)

        log.debug('waiting for reconnect', delay=delay)
        await asyncio.sleep(delay)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Scrape statistics from Fronius Datamanager and send them to an MQTT broker")
    parser.add_argument("--broker-url", metavar="URL", required=True, help="Send data to specified MQTT broker URL")
    parser.add_argument("--datamanager-url", metavar="URL", required=True, help="Scrape data from the specified Fronius Datamanager URL")

    args = parser.parse_args()
    config.__dict__.update(vars(args))

    asyncio.run(main(config))

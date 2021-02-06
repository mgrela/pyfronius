#!/usr/bin/env python3

import aiohttp
import json
from structlog import get_logger
from .solarapi import ApiResponse, Device, SolarApiV0, SolarApiV1
from urllib.parse import urljoin
from .utils import VFS

class DatamanagerClient(object):
    '''
    Handle the AIO client session to the datalogger.
    
    Wrap and load appropriate protocol plugins based on the protocol requested:
    - Solar API versions V0 or V1
    - Modbus RTU and Modbus TCP (not implememented yet)

    TODO: Should be an async context manager.
    '''
    def __init__(self, **kwargs):
        self.datalogger_url = kwargs['url']
        self.aio_session = kwargs.get('aio_session', aiohttp.ClientSession())
        self.vfs = VFS()

        # The initial solar api version is unknown
        self._solar_api_plugin = None
        self.api_base = self.datalogger_url

        self.log = get_logger().bind(api_base=self.api_base)
    
    async def raw_request(self, endpoint, **kwargs):
        '''
        Query a named CGI script and parse the resulting JSON
        '''
        url = urljoin(self.api_base, endpoint)
        self.log.debug('fetching', url=url, **kwargs)

        ## 42,0410,2012,EN 2.3 Requests 
        ## "Use HTTP-GET requests to query data from Solar API"
        resp = await self.aio_session.get(url, **kwargs)

        if resp.status != 200:
            self.log.error("error response", url=resp.url, status=resp.status, reason=resp.reason)
            # Don't return just yet, try to parse the response content just in case

        try:
            ## 42,0410,2012,EN 2.4 Responses
            ## "The response will always be a valid JSON string ready to be evaluated by standard libraries.
            ## If the response is delivered through HTTP, theContent-TypeHeader shall be either text/javascript or application/json."
            ## 
            ## aiohttp doesn't allow two content-type values to be specified as holding JSON data therefore we disable the check.
            j = await resp.json(content_type=None)
            if not j:
                self.log.error("cannot parse json", url=resp.url, content=resp.text())
                return None

            return j
        except Exception as e:
            self.log.error("exception while parsing json", exc_info=e, url=resp.url, content=await resp.text())
            return None
    
    async def api_request(self, endpoint, **kwargs):
        '''
        Send a request to and endpoint in the Solar API and build a response object.

        Return None if the request has resulted in an error.
        '''
        resp = ApiResponse(await self.raw_request(endpoint, **kwargs))
        if not resp.ok:
            self.log.error("error response", status_text=resp.status['Status_Text'], reason=resp.status['Reason'], msg=resp.status['UserMessage'], body=json.dumps(resp.body))
            return None

        return resp

    async def link_solar_api_plugin(self):
        '''
        Detect the solar api version supported by the datalogger and link the appropriate plugin.
        '''

        ## TODO: Load these in some more robust way
        SOLAR_API_PLUGINS = { '0': SolarApiV0, '1': SolarApiV1 }

        ## 42,0410,2012,EN 
        ## "The highest supported version on the device can be queried using the URL /solar_api/GetAPIVersion.cgi."
        api_info = await self.raw_request("/solar_api/GetAPIVersion.cgi")
        self.log.debug("api information", api_info=api_info)

        try:          
            if api_info is None:
                # Use API V0 when version cannot be fetched
                solarapi_plugin = SOLAR_API_PLUGINS.get('0')
            else:
                solarapi_plugin = SOLAR_API_PLUGINS.get(str(api_info['APIVersion']))
                
            if not solarapi_plugin:
                self.log.error('no plugin for Solar API version', api_info=api_info)
                return False
            
            # Pass session to the plugin to allow it to call fetch on our behalf
            self._solar_api_plugin = solarapi_plugin(client=self, vfs=self.vfs)

            self.api_base = urljoin(self.api_base, api_info['BaseURL'])
            self.log = self.log.bind(api_base=self.api_base, plugin=str(solarapi_plugin))
            self.log.info("attaching plugin")
            return True
        except Exception as e:
           self.log.error('cannot determine API version', api_info=api_info, exc_info=e)
           return False
        
    @property
    async def solar_api(self):
        '''
        Get an adapter object to use the Solar API JSON protocol.

        Reference: 42,0410,2011     002-06082013 (Fronius Solar API V0)
        Reference: 42,0410,2012,EN  013-02062020 (Fronius Solar API V1)
        '''
        if not self._solar_api_plugin:
            status = await self.link_solar_api_plugin()
            if not status:
                self.log.error('cannot load API plugin')
                raise NotImplementedError('cannot load API plugin')
        return self._solar_api_plugin

    @property
    async def modbus_api(self):
        '''
        Get an adapter object to directly use the Modbus protocol.

        Reference: 42,0410,2049     021-23092020    (Fronius DatamanagerModbus TCP & RTU)
        Reference: 42,0410,2108     007-200120      (Fronius Datamanager 2.0Modbus RTU Quickstart Guide)
        '''
        raise NotImplementedError("Modbus RTU and TCP support not yet implemented")

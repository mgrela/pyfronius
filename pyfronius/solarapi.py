#!/usr/bin/env python3

import json
from structlog import get_logger
import html
import collections
from urllib.parse import urljoin
from .mappings import RequestStatusCodes, InverterStatusCodes, DeviceClasses, DeviceTypes, Scopes
import time
import os

class ApiResponse(object):
    def __init__(self, data):
        self.log = get_logger()
        self._data = data
        self._unpack(data)

    def _unpack(self, data):
        try:
            self.head = data['Head']
            self.status = self.head['Status']
            self.body = data['Body']

            # Populate extended status information
            self.status['Status_Text'] = RequestStatusCodes.status_text(self.status['Code'])
            self.status['Status_Descr'] = RequestStatusCodes.status_description(self.status['Code'])

        except Exception as e:
            self.log.error("response form not recognized", exc_info=e, json=data)
            raise e

    @property
    def ok(self):
        return RequestStatusCodes.status_ok(self.status['Code'])


class Device(object):
    '''
    A simple object wrapping the device identification informration returned by the GetActiveDevices api request.
    '''
    def __init__(self, **kwargs):
        self.device_class = kwargs['device_class']
        self.device_id = kwargs['device_id']

        self.device_type = kwargs['device_type']
        self.model_name = DeviceTypes.model_name(self.device_type)

        # These properties are optional
        self.serial = kwargs.get('serial')
        self.channel_names = kwargs.get('channels')
        
    def __repr__(self):
        d = dict(device_class=str(self.device_class), id=self.device_id, device_type=self.device_type, model_name=self.model_name)
        if self.serial:
            d['serial'] = self.serial
        if self.channel_names:
            d['channel_names'] = self.channel_names
        return json.dumps(d)


class SolarApiCommon(object):
    '''
    Implements operations common between the V0 and V1 versions of the Fronius Solar API.

    Reference: 42,0410,2011     002-06082013 (Fronius Solar API V0)
    Reference: 42,0410,2012,EN  013-02062020 (Fronius Solar API V1)
    '''
    def __init__(self, **kwargs):
        self.client = kwargs['client']
        self.vfs = kwargs['vfs']
        self.log = get_logger()

    async def logger_info(self):      
        '''
        Reference: 42,0410,2011     002-06082013    3.4 GetLoggerInfo request
        Reference: 42,0410,2012,EN  013-02062020    3.4 GetLoggerInfo request
        '''  
        return await self.session.api_request('GetLoggerInfo.cgi')

    async def inverter_info(self):
        '''
        Reference: 42,0410,2011     002-06082013    3.5 GetInverterInfo request
        Reference: 42,0410,2012,EN  013-02062020    3.6 GetInverterInfo request
        '''
        inv = {}

        resp = await self.session.api_request('GetInverterInfo.cgi')
        
        for (id, info) in resp.body['Data'].items():
            # Populate model names and inverter status text
            info['Model_Name'] = DeviceTypes.model_name(info['DT'])
            info['Status_Text'] = InverterStatusCodes.status_text(info['StatusCode'])

            # The CustomName field is sometimes HTML encoded (likely a counter-XSS measure)
            info['CustomName'] = html.unescape(info['CustomName'])

            inv[ os.path.join(DeviceClasses.Inverter, id) ] = info

        return inv

class SolarApiV0(SolarApiCommon):
    '''
    Version-specific code for the V0 version of the Fronius Solar API.
    TODO: This was not tested with a real V0 API endpoint

    Reference: 42,0410,2011     002-06082013 (Fronius Solar API V0)
    '''
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


    async def inverter_realtime(self, **kwargs):
        '''
        Reference: 42,0410,2011     002-06082013    3.1 GetInverterRealtimeData request
        '''
        params = dict(Scope=str(Scopes.System), DataCollection=kwargs.get('collection', 'CommonInverterData'))
        if 'device_id' in kwargs:
            params.update(dict(Scope=str(Scopes.Device), DeviceIndex=kwargs['device_id']))
        return await self.session.api_request('GetInverterRealtimeData.cgi', params=params)


    async def sensor_realtime(self, **kwargs):
        '''
        Reference: 42,0410,2011     002-06082013    3.2 GetSensorRealtimeData request
        '''
        params = dict(DeviceIndex=kwargs['device_id'], DataCollection=kwargs.get('collection', 'NowSensorData'))
        return await self.session.api_request('GetSensorRealtimeData.cgi', params=params)


    async def string_realtime(self, **kwargs):
        '''
        Reference: 42,0410,2011     002-06082013    3.3 GetStringRealtimeData request
        '''
        params = dict(Scope=str(Scopes.Device), DeviceIndex=kwargs['device_id'], DataCollection=kwargs.get('collection', 'NowStringControlData'))
        if 'period' in kwargs:
            params.update(dict(TimePeriod=kwargs['period']))

        return await self.session.api_request('GetStringRealtimeData.cgi', params=params)

    # logger_info is implememted in the common class
    # Reference: 42,0410,2011     002-06082013    3.4 GetLoggerInfo request

    # inverter_info is implememted in the common class
    # Reference: 42,0410,2011     002-06082013    3.5 GetInverterInfo request

    async def active_devices(self, **kwargs):
        '''
        TODO: This was not tested with a real V0 API endpoint

        Reference: 42,0410,2011     002-06082013   3.6 GetActiveDeviceInfo Request
        '''
        devs = {}

        device_classes = kwargs.get('device_classes', set([DeviceClasses.System]))

        # List all types of active devices when DeviceClasses.System is requested. the V0 API doesn't support this DeviceClass
        if device_classes == set([DeviceClasses.System]):
            # Enumerate all device classes available for V0 api
            device_classes = set([ DeviceClasses.Inverter, DeviceClasses.SensorCard, DeviceClasses.StringControl ])

        for device_class in device_classes:
            resp = await self.session.api_request('GetActiveDeviceInfo.cgi', params=dict(DeviceClass=str(device_class)))
            for (device_id, d) in resp.body['Data'].items():
                devs[ os.path.join(device_class, device_id) ]= Device(device_class=DeviceClasses[device_class], device_id=device_id, device_type=d['DT'])

        return devs

class SolarApiV1(SolarApiCommon):
    '''
    Version-specific code for the V1 version of the Fronius Solar API.

    Reference: 42,0410,2012,EN  013-02062020 (Fronius Solar API V1)
    '''
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    async def inverter_realtime(self, **kwargs):
        '''
        Reference: 42,0410,2012,EN  013-02062020    3.1 GetInverterRealtimeData request
        '''
        params = dict(Scope=str(Scopes.System), DataCollection=kwargs.get('collection', 'CommonInverterData'))
        if 'device_id' in kwargs:
            params.update(dict(Scope=str(Scopes.Device), DeviceId=kwargs['device_id']))
        return await self.session.api_request('GetInverterRealtimeData.cgi', params=params)

    async def sensor_realtime(self, **kwargs):
        '''
        Reference: 42,0410,2012,EN  013-02062020    3.2 GetSensorRealtimeData request
        '''
        params = dict(Scope=str(Scopes.System), DataCollection=kwargs.get('collection', 'NowSensorData'))
        if 'device_id' in kwargs:
            params.update(dict(Scope=str(Scopes.Device), DeviceId=kwargs['device_id']))
        return await self.session.api_request('GetSensorRealtimeData.cgi', params=params)

    async def string_realtime(self, **kwargs):
        '''
        Reference: 42,0410,2012,EN  013-02062020    3.3 GetStringRealtimeData request
        '''
        params = dict(Scope=str(Scopes.System), DataCollection=kwargs.get('collection', 'NowStringControlData'))
        if 'device_id' in kwargs:
            params.update(dict(Scope=str(Scopes.Device), DeviceId=kwargs['device_id']))
        return await self.session.api_request('GetStringRealtimeData.cgi', params=params)

    # logger_info is implememted in the common class
    # Reference: 42,0410,2012,EN  013-02062020    3.4 GetLoggerInfo request

    async def logger_led_info(self):
        '''
        Reference: 42,0410,2012,EN  013-02062020    3.5 GetLoggerLEDInfo request
        '''
        return await self.session.api_request('GetLoggerLEDInfo.cgi')

    # inverter_info is implememted in the common class
    # Reference: 42,0410,2012,EN  013-02062020    3.6 GetInverterInfo request

    async def active_devices(self, **kwargs):
        '''
        Reference: 42,0410,2012,EN  013-02062020    3.7 GetActiveDeviceInfo request
        '''
        devs = {}
        device_classes = kwargs.get('device_classes', set([DeviceClasses.System]))

        for device_class in device_classes:
            resp = await self.session.api_request('GetActiveDeviceInfo.cgi', params=dict(DeviceClass=str(device_class)))

            if device_class == DeviceClasses.System:
                # System device class has a different response format:
                # Reference: 42,0410,2012,EN  013-02062020   3.7 GetActiveDeviceInforequest -> 3.7.5 DeviceClass is System
                for (device_class, device_list) in resp.body['Data'].items():
                    for (device_id, info) in device_list.items():
                        self.log.debug('discovered device', device_class=device_class, device_id=device_id, info=info)
                        devs[ os.path.join(device_class, device_id) ] = Device(
                                device_class=DeviceClasses[device_class],
                                device_id=device_id,
                                device_type=info['DT'],

                                # These are optional
                                # Reference: 42,0410,2012,EN  013-02062020  3.7 GetActiveDeviceInforequest -> Listing 37: Object structure of request body for GetActiveDeviceInfo request
                                serial=info.get('Serial'),
                                channels=info.get('ChannelNames')
                                )
            else:
                for (device_id, info) in resp.body['Data'].items():
                    devs[ os.path.join(device_class, device_id) ] = Device(
                            device_class=DeviceClasses[device_class],
                            device_id=device_id,
                            device_type=info['DT'],

                            # These are optional
                            # Reference: 42,0410,2012,EN  013-02062020  3.7 GetActiveDeviceInforequest -> Listing 37: Object structure of request body for GetActiveDeviceInfo request
                            serial=info.get('Serial'),
                            channels=info.get('ChannelNames')
                            )

        return devs

    async def meter_realtime(self, **kwargs):
        '''
        Reference: 42,0410,2012,EN  013-02062020    3.8 GetMeterRealtimeData request
        '''
        params = dict(Scope=Scopes.System)
        if 'device_id' in kwargs:
            params.update(dict(Scope=Scopes.Device, DeviceId=kwargs['device_id']))
        return await self.session.api_request('GetMeterRealtimeData.cgi', params=params)

    async def storage_realtime(self, **kwargs):
        '''
        Reference: 42,0410,2012,EN  013-02062020    3.9 GetStorageRealtimeData request
        '''
        params = dict(Scope=Scopes.System)
        if 'device_id' in kwargs:
            params.update(dict(Scope=Scopes.Device, DeviceId=kwargs['device_id']))
        return await self.session.api_request('GetStorageRealtimeData.cgi', params=params)

    async def ohmpilot_realtime(self, **kwargs):
        '''
        Reference: 42,0410,2012,EN  013-02062020    3.10 GetOhmPilotRealtimeData request
        '''
        params = dict(Scope=Scopes.System)
        if 'device_id' in kwargs:
            params.update(dict(Scope=Scopes.Device, DeviceId=kwargs['device_id']))
        return await self.session.api_request('GetOhmPilotRealtimeData.cgi', params=params)

    async def powerflow_realtime(self):
        '''
        Reference: 42,0410,2012,EN  013-02062020    3.11 GetPowerFlowRealtimeData request
        '''
        v = await self.session.api_request('GetPowerFlowRealtimeData.fcgi')
        self._unpack_powerflow_realtime(v.body['Data'])
        return v


    #
    # Undocumented functions
    #
    # WARNING: These functions are undocumented in the V1 API reference. They were discovered 
    # by observing the network traffic generated by the browser UI of the Datamanager.

    async def logger_connectioninfo(self):
        '''
        Get information about the status of a logger network conectivity:
        - WLAN connection state
        - Solar.NET connection state (this seems to be the Modbus interface)
        - Solar.WEB conection state (the Fronius Cloud)

        Status code '2' appears to mean that the service is connected.
        '''
        return self.session.api_request('GetLoggerConnectionInfo.cgi')
    
    #
    # JSON data unpacking to different sensors and updating the VFS.
    # Calling these functions is not mandatory to get the data you want but using them
    # populates the VFS making it much easier to navigate the data generated by the datalogger
    # The datapoints schema is based on SenML (https://tools.ietf.org/html/rfc8428).
    #
    def _unpack_item(data, key, **kwargs):
        '''
        Unpack a single item from a dict and create a data point from it.
        Can apply arbitrary properties to the data points as well as convert the value if needed.
        '''
        path = kwargs['path']
        del kwargs['path']

        if not data.get(key): # An item doesn't exist or is null
            return

        p = dict(v=data[key])
        if 'vconv' in kwargs:
            p['v'] = kwargs['vconv'](p['v']) # Allow a user to apply a lambda to convert a value
            del kwargs['vconv']
        
        if 'unit' in kwargs:
            p.update(dict(u=kwargs['u']))
            del kwargs['u']

        p.update(kwargs) # Apply the rest of kwargs as data point properties
        self.session.data.put(os.path.join(path, key), p)

    def _unpack_powerflow_realtime(self, data):
        '''
        Reference: 42,0410,2012,EN  013-02062020    3.11 GetPowerFlowRealtimeData request
        '''
        self.log.debug('unpacking realtime powerflow', data=data)
        
        # All datapoints should have the same timestamp as they are collected as part of a single request
        ts = time.time()

        site = data['Site']
        
        common = dict(path='site', t=ts)
        self._unpack_item(site, 'Mode', **common)
        self._unpack_item(site, 'BatteryStandby', **common)
        self._unpack_item(site, 'BackupMode', **common)
        self._unpack_item(site, 'P_Grid', u='W', **common)
        self._unpack_item(site, 'P_Load', u='W', **common)
        self._unpack_item(site, 'P_Akku', u='W', **common)
        self._unpack_item(site, 'P_PV', u='W', **common)
        # Convert to ratio to be compliant with RFC8428
        self._unpack_item(site, 'rel_SelfConsumption', u='/', **common, vconv=lambda v: float(v)/100) 
        self._unpack_item(site, 'rel_Autonomy', u='/', **common, vconv=lambda v: float(v)/100)
        self._unpack_item(site, 'Meter_Location', **common)
        self._unpack_item(site, 'E_Day', u='Wh', **common)
        self._unpack_item(site, 'E_Year', u='Wh', **common)
        self._unpack_item(site, 'E_Total', u='Wh', **common)

        for (id, data) in data.get('Inverters', {}).items():
            common = dict(path=os.path.join('Inverters', id), ts=ts)

            self._unpack_item(data, 'DT', **common)
            self._unpack_item(data, 'P', u='W', **common)
            self._unpack_item(data, 'SOC', u='/', **common, vconv=lambda v: float(v)/100)
            self._unpack_item(data, 'CID', **common)
            self._unpack_item(data, 'Battery_Mode', **common)
            self._unpack_item(data, 'E_Day', u='Wh', **common)
            self._unpack_item(data, 'E_Year', u='Wh', **common)
            self._unpack_item(data, 'E_Total', u='Wh', **common)
        
        # implemented since PowerFlowVersion 10
        # TODO: Not tested as I do not have an Ohmpilot

        for (id, data) in data.get('Smartloads', {}).get('Ohmpilots', {}).items():
            common = dict(path='Smartloads/Ohmpilots', t=ts)

            self._unpack_item(data, 'P_AC_Total', u='W', **common)
            self._unpack_item(data, 'State', **common)
            self._unpack_item(data, 'Temperature', u='Cel', **common)

        # implemented since PowerFlowVersion 11
        # TODO: Not tested as I do not have a SecondaryMeter
        for (id, data) in data.get('SecondaryMeters', {}).items():

            common = dict(path='SecondaryMeters', t=ts)

            self._unpack_item(data, 'P', u='W', **common)
            self._unpack_item(data, 'MLoc', **common)
            self._unpack_item(data, 'Label', **common)
            self._unpack_item(data, 'Category', **common)

        
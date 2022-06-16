#!/usr/bin/python
#
# install the "pysnmp4" module to be able to run this script
#
# http://pysnmp.sourceforge.net/

import collections
#import imp
#import optparse
#import os
import periodic_timer
#import re
import signal
#import sys
#import threading
import time

from pysnmp.entity import engine, config
from pysnmp.carrier.asynsock.dgram import udp
from pysnmp.entity.rfc3413 import cmdgen

def dotted_oid_to_tuple(oid):
    '''
    converts an OID string with dots as separators to a tuple
    '''
    return tuple(map(int, oid.split('.')))

def callback_outlet_count(sendRequestHandle, errorIndication, errorStatus, errorIndex,
          varBinds, cbCtx):
    '''
    callback to determine the number of outlets for a PDU
    '''
    cbCtx['errorIndication'] = errorIndication
    cbCtx['errorStatus'] = errorStatus
    cbCtx['errorIndex'] = errorIndex
    cbCtx['varBinds'] = varBinds

def callback_label(sendRequestHandle, errorIndication, errorStatus, errorIndex, varBindTable, cbCtx):
    '''
    callback to determine the label of an outlet
    '''
    if errorIndication:
        print errorIndication
        return
    if errorStatus:
        print errorStatus.prettyPrint()
        return

    for varBindRow in varBindTable:
        for oid, val in varBindRow:
            if val is not None:
                cbCtx[oid] = val

def callback_active_power(sendRequestHandle, errorIndication, errorStatus, errorIndex, varBinds, outletCtx):
    '''
    callback to determine the active power for an outlet
    '''
    if errorIndication:
        print errorIndication
        return
    if errorStatus:
        print errorStatus.prettyPrint()
        return

    oid, val = varBinds[0]
    outletCtx.set_active_power(int(val))

def callback_watt_hours(sendRequestHandle, errorIndication, errorStatus, errorIndex, varBinds, outletCtx):
    '''
    callback to determine the cumulative active energy for an outlet
    '''
    if errorIndication:
        print errorIndication
        return
    if errorStatus:
        print errorStatus.prettyPrint()
        return

    oid, val = varBinds[0]
    outletCtx.set_watt_hours(int(val))

oidmap = {
    'outletCount': '1.3.6.1.4.1.13742.4.1.2.1', # scalar
    'outletLabel': '1.3.6.1.4.1.13742.4.1.2.2.1.2', # table
    'outletActivePower': '1.3.6.1.4.1.13742.4.1.2.2.1.7', # table
    'outletVoltage': '1.3.6.1.4.1.13742.4.1.2.2.1.6', # table
    'outletWattHours': '1.3.6.1.4.1.13742.4.1.2.2.1.31' # table
}

class Outlet:
    '''
    an object of this class represents a single outlet in a single PDU
    '''

    def __init__(self, pdu_conf_name, pdu_ip, table_index, outlet_label):
        self.pdu_conf_name = pdu_conf_name
        self.pdu_ip = pdu_ip
        self.table_index = table_index
        self.active_power = None
        self.watt_hours = None
        self.outlet_label = outlet_label

    def get_pdu_conf_name(self):
        return self.pdu_conf_name

    def get_pdu_ip(self):
        return self.pdu_ip

    def get_table_index(self):
        return self.table_index

    def get_outlet_label_oid(self):
        return oidmap['outletLabel'] + '.' + str(self.table_index)

    def get_active_power_oid(self):
        return oidmap['outletActivePower'] + '.' + str(self.table_index)

    def get_watt_hours_oid(self):
        return oidmap['outletWattHours'] + '.' + str(self.table_index)

    def get_active_power(self):
        return self.active_power

    def set_active_power(self, active_power):
        self.active_power = active_power

    def get_watt_hours(self):
        return self.watt_hours

    def set_watt_hours(self, watt_hours):
        self.watt_hours = watt_hours

    def get_outlet_label(self):
        return self.outlet_label

class EnergyConsumptionTracker:
    '''
    tracks the energy consumption of a group of outlets in a group of PDUs
    '''

    def __init__(self, pdu_ips, username, password):
        self.snmpEngine = engine.SnmpEngine()

        # configures the SNMPv3 user who has access to the PDUs
        config.addV3User(
            self.snmpEngine,
            username,
            config.usmHMACMD5AuthProtocol, password,
            config.usmDESPrivProtocol, password
            )

        config.addTargetParams(self.snmpEngine, 'pdu-params', username, 'authPriv')

        # configurations for the different PDUs
        pduCount = 0
        self.configs = []
        self.config_to_ip_mapping = {}
        for pdu_ip in pdu_ips:
            conf_name = 'pdu' + str(pduCount)
            self.configs.append(conf_name)
            self.config_to_ip_mapping[conf_name] = pdu_ip
            config.addTargetAddr(
                self.snmpEngine, self.configs[-1], config.snmpUDPDomain,
                (pdu_ip, 161), 'pdu-params')
            pduCount += 1

        config.addSocketTransport(
            self.snmpEngine,
            udp.domainName,
            udp.UdpSocketTransport().openClientMode())

        # Used to pass data from callback function
        cbCtx = {}

        # determine number of outlets in each PDU
        for c in self.configs:
            t = (dotted_oid_to_tuple(oidmap['outletCount'] + '.0'), None)
            cbCtx[c] = {}

            cmdgen.GetCommandGenerator().sendReq(
                self.snmpEngine, c, (t, ), callback_outlet_count, cbCtx[c])

        self.snmpEngine.transportDispatcher.runDispatcher()

        self.outletCount = collections.defaultdict(int)

        for c in self.configs:
            if cbCtx[c]['errorIndication']:
                print cbCtx['errorIndication']
            elif cbCtx[c]['errorStatus']:
                print cbCtx[c]['errorStatus'].prettyPrint()
            else:
                self.outletCount[c] = int(cbCtx[c]['varBinds'][0][1])

        assert sum(self.outletCount.values()) > 0

        self.outletLabelOidTable = collections.defaultdict(list)
        self.outletActivePowerOidTable = collections.defaultdict(list)
        for c in self.configs:
            for outletNum in range(self.outletCount[c]):
                self.outletLabelOidTable[c].append(
                    dotted_oid_to_tuple(oidmap['outletLabel'] + '.' + str(outletNum + 1)))
                self.outletActivePowerOidTable[c].append(
                    dotted_oid_to_tuple(oidmap['outletActivePower'] + '.' + str(outletNum + 1)))

        self.__get_outlet_labels()

    def __get_outlet_labels(self):
        cbCtx = {}
        for c in self.configs:
            cbCtx[c] = {}
            cmdgen.NextCommandGenerator().sendReq(
                self.snmpEngine, c,
                tuple(map(lambda x: (x, None), self.outletLabelOidTable[c])),
                callback_label, cbCtx[c])
        self.snmpEngine.transportDispatcher.runDispatcher()

        self.outlet_label_to_outlet = {}
        for c in self.configs:
            labels = cbCtx[c]
            for val in labels:
                outlet_label = str(labels[val])
                self.outlet_label_to_outlet[outlet_label] = \
                    Outlet(c, self.config_to_ip_mapping[c], tuple(val)[-1], \
                               outlet_label)

    def get_outlets(self):
        return self.outlet_label_to_outlet.values()

    def get_outlet_active_power(self, outlet_labels):
        '''
        returns a dictionary of outlets to active power in watts (W)
        '''

        assert type(outlet_labels) == list
        for outlet_label in outlet_labels:
            outlet = self.outlet_label_to_outlet[outlet_label]
            cmdgen.GetCommandGenerator().sendReq(
                self.snmpEngine, outlet.get_pdu_conf_name(),
                ((outlet.get_active_power_oid(), None),),
                callback_active_power, outlet)
        self.snmpEngine.transportDispatcher.runDispatcher()

        results = {}
        for outlet_label in outlet_labels:
            outlet = self.outlet_label_to_outlet[outlet_label]
            active_power = outlet.get_active_power()
            results[outlet_label] = active_power

        return results

    def get_outlet_watt_hours(self, outlet_labels):
        '''
        returns a dictionary of outlets to watt hours (Wh)
        '''
        assert type(outlet_labels) == list
        for outlet_label in outlet_labels:
            outlet = self.outlet_label_to_outlet[outlet_label]
            cmdgen.GetCommandGenerator().sendReq(
                self.snmpEngine, outlet.get_pdu_conf_name(),
                ((outlet.get_watt_hours_oid(), None),),
                callback_watt_hours, outlet)
        self.snmpEngine.transportDispatcher.runDispatcher()

        results = {}
        for outlet_label in outlet_labels:
            outlet = self.outlet_label_to_outlet[outlet_label]
            watt_hours = outlet.get_watt_hours()
            results[outlet_label] = watt_hours

        return results

def timer_callback(e, f, outlet_labels):
    watts = e.get_outlet_active_power(outlet_labels)
    t = time.time()

    for w in watts:
        print >>f, t, w, watts[w]

def signal_handler(signum, frame, timer):
    timer.stop()

t = None
fp = None

def prepare_exp(outlet_labels) :
    pdu_snmp_user="admin"
    pdu_snmp_password="bargel21"

    pdus = ['141.76.50.20', '141.76.50.21', '141.76.50.22', '141.76.50.23', '141.76.50.24']

    e = EnergyConsumptionTracker(pdus, pdu_snmp_user, pdu_snmp_password)
    print e.get_outlet_active_power(outlet_labels)
    global f
    f = open('power.csv', 'w')
    global t
    measurement_interval_msec = 500
    t = periodic_timer.PeriodicTimer(measurement_interval_msec/1000.0,
                                     lambda : timer_callback(e, f, outlet_labels))
    t.start()

def finish_exp() :
    t.stop()
    t.join()
    f.close()
    f.close()

if __name__ == '__main__':
    prepare_exp(['stream26', 'stream27', 'stream28'])
    time.sleep(10)
    finish_exp()

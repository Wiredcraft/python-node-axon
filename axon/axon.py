#!/usr/bin/env python
# coding:utf-8

import os
try:
    import json
edd!xcept ImportError:
    import simplejson as json
import struct
import socket
import time

DEFAULT_TIMEOUT = 10

class Axon(object):
    head_patt = struct.Struct('>I')

    def connect(self, addr, timeout=DEFAULT_TIMEOUT):
        self._addr = addr
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect(addr)
        self._sock = sock

    def push(self, *args):
        if(len(args)>1):
            chunks = []
            for arg in args:
                chunks.append(self.serialize(arg))
            self.send(self.serialize(''.join(chunks), meta=0))
        else:
            self.send(self.serialize(args[0]))

    def send(self, msg):
        self._sock.send(msg)

    def serialize(self, msg, meta=2):
        # see https://github.com/visionmedia/axon#protocol for more details
        # if not isinstance(msg, basestring):
        if meta:
            msg = json.dumps(msg)
        if len(msg)>(1<<24):
            raise ValueError('the payload is too large')
        return self.head_patt.pack(len(msg)|meta<<24)+msg

if __name__=='__main__':
    client = Axon()
    client.connect(('127.0.0.1', 7777))
    client.push('log', {'source':'source', 'space': 'space', 'message': 'message'})

# Copyright (c) 2016 Brett Bethke
# Copyright (c) 2018 Jonas Kümmerlin <jonas@kuemmerlin.eu>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import socket
import os
import sys
import logging

_log = logging.getLogger(__name__)

class SystemdNotifier:
    """This class holds a connection to the systemd notification socket
    and can be used to send messages to systemd using its notify method."""

    def __init__(self):
        """Instantiate a new notifier object. This will initiate a connection
        to the systemd notification socket.
        """
        try:
            self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
            addr = os.getenv('NOTIFY_SOCKET')
            if addr[0] == '@':
                addr = '\0' + addr[1:]
            self.socket.connect(addr)
        except:
            self.socket = None
            _log.exception('not opening notify socket')

    def notify(self, state):
        """Send a notification to systemd. state is a string; see
        the man page of sd_notify (http://www.freedesktop.org/software/systemd/man/sd_notify.html)
        for a description of the allowable values."""
        if self.socket is None:
            return

        try:
            self.socket.sendall(state.encode('utf8'))
        except:
            _log.exception('could not send notify')

    def watchdog(self):
        self.notify('WATCHDOG=1')

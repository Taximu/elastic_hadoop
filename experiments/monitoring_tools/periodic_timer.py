import threading
import time

class PeriodicTimer(threading.Thread):
    ''' class to periodically call a certain function '''

    def __init__(self, interval, f):
        threading.Thread.__init__(self)

        self.__function = f
        self.__interval = interval
        self.__stop = False

    def run(self):
        while not self.__stop:
            self.__function()
            time.sleep(self.__interval)

    def stop(self):
        self.__stop = True

    # overwrite join, the reason is here http://bugs.python.org/issue1167930
    def join(self):
        while self.isAlive():
            threading.Thread.join(self, 1.0)

from datetime import datetime, timedelta
import time


class WaiterException(Exception):
    pass


class Waiter(object):
    def __init__(self, logger=None, max_duration=120, retries=5):
        self.logger = logger
        self.max_duration = max_duration
        self.start = datetime.now()
        self.until = self.start + timedelta(seconds=self.max_duration)
        self.interval = max_duration / retries

    def wait(self):
        if self._check():
            self._log('Success')
            return True
        while datetime.now() < self.until:
            if self._check():
                self._log('Success')
                return True
            time.sleep(self.interval)
        raise WaiterException('Max duration reached with condition not meet')

    def _log(self, message):
        if self.logger:
            self.logger.info(message)
        else:
            print(message)

    def _check(self):
        raise NotImplemented()

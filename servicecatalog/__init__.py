import logging
import random
from collections import defaultdict, namedtuple
from threading import Lock, Thread
from time import sleep

from consul import Consul

instance = namedtuple('serviceinstance', ['address', 'port'])
service = namedtuple('service', ['ts', 'instances'])


class ServiceInstance(instance):
    def as_uri(self, path=""):
        return "http://{0}:{1}/{2}".format(self.address, self.port, path)


class ServiceCatalog:
    def __init__(self, host='localhost', port=8500, interval=30):
        self.client = Consul(host=host, port=port, consistency='stale')
        self.interval = interval
        self.cache = defaultdict(list)
        self._lock = Lock()
        self.updater = Thread(name="Consul-update", target=self._update)
        self.updater.daemon = True
        self.updater.start()

    def fetch(self, name, index=None):
        try:
            idx, result = self.client.catalog.service(name, index=index)

            return service(index, [
                ServiceInstance(x['ServiceAddress'] or x["Address"],
                                x["ServicePort"]) for x in result
            ])
        except Exception as e:
            logging.error(
                "Failed while fetching data for %s", name, exc_info=True)

    def _update(self):
        self._isrunning = True

        while self._isrunning:
            for k, v in self.cache.items():
                service = self.fetch(k)

                if service:
                    self._lock.acquire()
                    self.cache[k] = service
                    self._lock.release()

            sleep(self.interval)

    def stop(self):
        self._isrunning = False

    def __getitem__(self, name):
        self._lock.acquire()

        if not self.cache[name]:
            logging.info(
                "Adding new service `%s` to the service catalog" % name)
            self.cache[name] = self.fetch(name)
        result = random.choice(self.cache[name].instances)
        self._lock.release()

        if not result:
            raise KeyError("Can't find service with name %s" % name)

        return result

    def all(self, name):
        self._lock.acquire()

        if not self.cache[name]:
            logging.info(
                "Adding new service `%s` to the service catalog" % name)
            self.cache[name] = self.fetch(name)
        self._lock.release()

        return self.cache[name].instances

import logging
import os
import random
from collections import defaultdict, namedtuple
from threading import Lock, Thread
from time import sleep

from consul import Consul

instance = namedtuple('serviceinstance', ['address', 'port'])
service = namedtuple('service', ['ts', 'instances'])


class ServiceInstance(instance):
    def as_uri(self, scheme='http', path=""):
        return "{0}://{1}:{2}/{3}".format(scheme, self.address, self.port, path)


class ServiceCatalog:
    def __init__(self, host='localhost', port=8500, interval=30, env=os.environ):
        self.online_mode = self._get_online_mode(env)
        self.service_overrides = self._get_service_overrides(env)
        self._lock = Lock()
        self.cache = defaultdict(list)

        if self.online_mode:
            self.client = Consul(host=host, port=port, consistency='stale')
            self.interval = interval

            self.updater = Thread(name="Consul-update", target=self._update)
            self.updater.daemon = True
            self.updater.start()

    def _get_online_mode(self, env):
        """
        Method returns flag whether this library should run in online mode (thus talking to consul)
        or offline mode - thus only use environment variables to serve the
        :return:
        """
        offline_mode = env.get('SERVICECATALOG_OFFLINE_MODE', '0')

        # online mode is by default, so it's only disabled
        # when offline mode env. var is set to 1
        return not offline_mode == '1'

    def _get_service_overrides(self, env):
        """
        Method returns a map of service_name=ServiceInstance(host, port) which is read from environment variables.

        Eg. by setting these env. variables:
        SERVICECATALOG_SERVICE_HOST_AVAILABILITY_VARNISH=http://varnish
        SERVICECATALOG_SERVICE_PORT_AVAILABILITY_VARNISH=80

        the service instance that will be returned for availability-varnish is ServiceInstance("http://varnish", 80).
        The port 80 is default and will be returned if it's not specified in env. vars.

        :param env:
        :return:
        """
        service_host_prefix = "SERVICECATALOG_SERVICE_HOST_"
        service_port_prefix = "SERVICECATALOG_SERVICE_PORT_"
        result = {}
        hosts = {}
        ports = {}

        for key, value in env.items():
            if key.startswith(service_host_prefix):
                # this should turn "SERVICECATALOG_SERVICE_HOST_AVAILABILITY_VARNISH" into "availability-varnish"
                service_name = key.replace(service_host_prefix, '').replace('_', '-').lower()
                hosts[service_name] = value
            elif key.startswith(service_port_prefix):
                # this should turn "SERVICECATALOG_SERVICE_PORT_AVAILABILITY_VARNISH" into "availability-varnish"
                service_name = key.replace(service_port_prefix, '').replace('_', '-').lower()
                try:
                    ports[service_name] = int(value)
                except Exception:
                    logging.error(f"Unsupported value {value} for {key} - should be number.")
                    raise

        for service_name, host in hosts.items():
            port = ports.get(service_name, 80)
            result[service_name] = service(None, [ServiceInstance(host, port)])
        return result

    def fetch(self, name, index=None):
        overriden_value = self.service_overrides.get(name)

        if overriden_value:
            return overriden_value

        if not self.online_mode:
            return service(index, [])

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

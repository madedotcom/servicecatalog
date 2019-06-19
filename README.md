# ServiceCatalog

Servicecatalog is a simple library that provides a dict-based interface to Consul's service discovery.

## Usage

```python
import servicecatalog

catalog = servicecatalog.ServiceCatalog(
    host='consul.service.consul',
    port=8500, 
    interval=60 # servicecatalog will poll Consul for changes periodically
)

db = catalog['my-database'] # returns the namedtuple (address, port).
web = catalog['my-web-service']

uri = web.as_uri('/foo?bar=123') # returns http://{address}:{port}/foo?bar=123
```

## FAQ

### But ... why?

Consulate and python-consul are great but, as a client, all I want is to get a registered service in a low-ceremony, easy-to-mock fashion.

### Is data kept up-to-date when services are registered or deregistered?

Yes. Servicecatalog runs a background thread that keeps data fresh. So long as you fetch your services from the catalog whenever you need them, you should be good to go.

### What if I have multiple instances of the same service registered?

Servicecatalog will return a random member of the set each time you ask for a service.

### Running in offline mode eg. on continuous integration

If you wanna use this library in offline mode - without actually talking to consul - eg. when running your tests locally / on CI,
you can use offline mode and service values overrides.
 
See tests/test_offline_mode.py as an example how to set environment variables for such use case.

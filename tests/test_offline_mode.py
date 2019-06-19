import servicecatalog

fake_env = {
    'SERVICECATALOG_OFFLINE_MODE': '1',
    'SERVICECATALOG_SERVICE_HOST_AVAILABILITY_VARNISH': 'varnish',
    'SERVICECATALOG_SERVICE_PORT_AVAILABILITY_VARNISH': 80,
    'SERVICECATALOG_SERVICE_HOST_AVAILABILITY_API': 'api',
}
catalog = servicecatalog.ServiceCatalog(env=fake_env)

varnish = catalog['availability-varnish']

assert varnish.as_uri() == "http://varnish:80/"

[instance] = catalog.all('availability-varnish')

assert instance.as_uri() == "http://varnish:80/"

[instance] = catalog.all('availability-api')

assert instance.as_uri() == "http://api:80/"

instances = catalog.all('availability-db')

assert len(instances) == 0

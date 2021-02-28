# [WIP] locust-timestream-listener (This still doesn't work!)

Package that uses locust 'event' hooks to push locust related events to an timestream database.

## Installation

Install using your favorite package installer:

```bash
pip install locust-timestream-listener
# or
easy_install locust-timestream-listener
```


### Usage

Import the library and use the `event.init` hook to register the listener.

```python
...
from locust_timestream_listener import TimestreamListener, TimestreamSettings

@events.init.add_listener
def on_locust_init(environment, **_kwargs):
    """
    Hook event that enables starting an timestream connection
    """
    # this settings matches the given docker-compose file
    timestreamSettings = TimestreamSettings(
        database = 'test-project'
    )
    # start listerner with the given configuration
    TimestreamListener(env=environment, timestreamSettings=timestreamSettings)
...
```

### Example

You can find a working example under the [examples folder](https://github.com/hoodoo-digital/locust-timestream-listener/blob/main/example)

*Timestream with Grafana*

We have included a working example `docker-compose.yml` file that can be used to spin a reporting setup in case you don't have one.

(Make sure you have `docker` and `docker-compose` installed and just run:

```bash
docker-compose up
```

*Configuration*

Once grafana is running (by default on port: 3000) `https://localhost:3000` , you need to:

* Connect to timestream as the datasource:
  * Host: https://timestream:8086
  * User: admin
  * Password: pass

* Import a new dashboard. We have provided a custom dashboard for you `locust-grafana-dashboard.json` that just works out of the box with the locust-events that the listener will emmit.

![Grafa Example](https://i.ibb.co/p2kbzZk/grafana.png)

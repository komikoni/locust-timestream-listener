from locust import between, events, tag, task, HttpUser

from locust_timestream_listener import TimestreamListener, TimestreamSettings


@events.init.add_listener
def on_locust_init(environment, **_kwargs):
    """
    Hook event that enables starting an timestream connection
    """
    # this settings matches the given docker-compose file
    timestreamSettings = TimestreamSettings(
        influx_host = 'localhost',
        influx_port = '8086',
        user = 'admin',
        pwd = 'pass',
        database = 'test-project'
    )
    # start listerner with the given configuration
    TimestreamListener(env=environment, timestreamSettings=timestreamSettings)

class TestWebUser(HttpUser):

    wait_time = between(1,5)
         
    @tag('home_page')
    @task(1)
    def home_page(self):
        with self.client.get("/", catch_response=True) as response:
            if response.status_code != 200:
                response.failure("Got wrong response")
    
    @tag('connectors')
    @task(1)
    def workfront_connector(self):
        with self.client.get("/connectors/workfront", catch_response=True) as response:
            if 'Do More Work, Faster' not in response.text:
                response.failure("Expected test was not displayed")
            
    
    def on_start(self):
        print('New user was spawned')
       

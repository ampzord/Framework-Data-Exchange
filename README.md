# Framework-Data-Exchange-CPS
Repository for Master's Dissertation @ FEUP.

This Project was developed to complement the Dissertation: __Framework for Intelligent Data Exchange in Cyber Physical Systems__

## Additional Software used:
- [MQTT](https://mqtt.org/)
- [InfluxDB](https://www.influxdata.com/)
- [Eclipse Mosquitto Broker](https://mosquitto.org/)
- [Grafana](https://grafana.com/)

## Requirements

```sh
pip install -r requirements.txt
```

## Documentation

https://framework-for-intelligent-data-exchange.readthedocs.io/en/latest/

## Usage

On Windows run ```influxd.exe``` in influxdb-1.8.10-1 folder to start influxDB.

Install Mosquitto Broker and make sure it is running using:

```sh
netstat -an
```

![Mosquitto Broker](https://i.imgur.com/l54VwwB.png)


Run ```simulation.py``` with intended arguments of __NUMBER_CLIENTS__, __NUMBER_ITERATIONS_TILL_WRITE__, __NUMBER_GENERATED_POINTS_PER_CYCLE__, __TIME_TILL_REQUEST__ and __MAX_ITERATIONS_SIMULATION__.

__NUMBER_CLIENTS__ - Number of clients will dictate how many clients are created for the simulation.

__NUMBER_ITERATIONS_TILL_WRITE__ - Number of iterations till write relates to the number of iterations executed before saving the generated data to client local database.

__NUMBER_GENERATED_POINTS_PER_CYCLE__ - Number of generated points per cycle is the number of how many data points are gonna be generated in the welding workflow cycle.

__TIME_TILL_REQUEST__ - Time passed until Master is executed and the request of information is done.

__MAX_ITERATIONS_SIMULATION__ - Number of iterations ran with the same arguments, this argument is used to establish an average and standard deviation ground.

To view execution through debug mode select: __"DEBUG_MODE"__ instead of __"INFO_MODE"__

## Branches

__Master__ branch contains Concurrent Solution Approach.

__SelfOrganizingSolution__ branch contains Self-Organizing Solution Approach.

__LinearSolution__ branch contains Linear Solution Approach.

__No-Request__ branch contains the execution of the program without any request of Master (This is the benchmark used to compare metrics).

## License

This project is licensed under the terms of the **MIT** [license](https://github.com/ampzord/Framework-Data-Exchange/blob/master/LICENSE).

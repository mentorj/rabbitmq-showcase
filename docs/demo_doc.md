# Tests suite documentation

## Goals
This project aims to demonstrate :
- how to properly manage connections & channels
- how to use the prefetch settings

while giving some assets to quantify how such or such parameter affects the
application whole performance.

## Lessons learned
Reports generated show :
- using docker containers is a bad idea, metrics obtained with or without containers
are not comparable
- as stated in RabbitMQ docs connections are intended to be long living objects and
intensive open/close of such objects should be avoided
- as stated  in RabbitMQ docs channels are intended to be long living objects and
intensive open/close of such objects should be avoided (impact reduced compared to connections)
- explicit acknowledgment (client) has a cost , so if not needed do not use it
- *fetchsize* parameter may be useful if all consumers from a queue use it or if the consumer is the
only one
- mixing clients using and not using fetch parameter has a big downside : faster consumers
are downgraded to the throughput from the slowest ones because of *load balancing*

## Using the demos
Running the 2 tests suites requires:
+ maven
+ jdk 8 at least
+ network connection
+ 2 machines (at least 2 Vms) but not 2 containers living on the same box

## pending jobs
This project still requires hard work:
+ adding more tests
+ huge code cleaning & refactoring

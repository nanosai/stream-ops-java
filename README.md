# Stream Ops for Java
Stream Ops for Java is a fully embeddable data streaming engine and stream processing API for Java.
The data streaming engine and the stream processing API can be used together, or separately.

Stream Ops addresses the same use cases as Kafka, but with some significant design differences which gives you
a higher degree of architectural freedom. You can embed Stream Ops in a mobile app, desktop app, microservice,
web app backend, and possibly also inside serverless functions like AWS Lambda etc. (we are still looking into
how this can be done) and Google App Engine apps.

Stream Ops design differences also supports a wider set of use cases than Kafka does out-of-the-box. We will
describe these differences as the code that provides them is cleaned up and released.

Stream Ops is developed by Nanosai: [http://nanosai.com](https://nanosai.com)


# Stream Ops Tutorial
We have a Stream Ops tutorial in progress here:

[http://tutorials.jenkov.com/stream-ops-java/index.html](http://tutorials.jenkov.com/stream-ops-java/index.html)


# The 1 Billion Records Per Second Challenge
We (Nanosai) will try to get Stream Ops to be able to process 1 billion records per second. You can read more about
that challenge at [The 1BRS Challenge](https://nanosai.com/1brs-challenge).

# License
The intention is to release Stream Ops under the Apache License 2.0 . We need to discuss this a bit further internally
in Nanosai, but in any case, it will be something close to that.


# Dependencies
Stream Ops depends on these two other Nanosai projects:

[Mem Ops for Java](https://github.com/nanosai/mem-ops-java)
[ION Ops for Java](https://github.com/nanosai/rion-ops-java)

# Getting Started

We are working on getting all our projects into the Maven central repository. Until that succeeds, the way to
use Stream Ops is:


Clone / fork Stream Ops and both of the dependencies (RION Ops + Mem Ops) and build all three locally
to get Stream Ops working.

In a near future we will create a Nanosai aggregate repository which will contain all of our distributed systems,
distributed data and data science related projects in one repository. Then you can fork just one project to
play with everything! That aggregate repository will get updated with new versions of the individual projects
as they are released, and we will make sure that all versions play well together in each release of the aggregate
repository.


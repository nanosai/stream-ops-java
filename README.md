# Stream Ops for Java

 - [Introduction](#introduction)
 - [Stream Ops Tutorial](#stream-ops-tutorial)
 - [The 1 Billion Records Per Second Challenge](#1brs)
 - [License](#license)
 - [Getting Started](#getting-started)
 - [Maven Dependency](#maven-dependency)
 - [Dependencies](#dependencies)


<a name="introduction"></a>

# Introduction
Stream Ops for Java is a fully embeddable data streaming engine and stream processing API for Java.
The data streaming engine and the stream processing API can be used together, or separately.

Stream Ops addresses the same use cases as Kafka, but with some significant design differences which gives you
a higher degree of architectural freedom. You can embed Stream Ops in a mobile app, desktop app, microservice,
web app backend, and possibly also inside serverless functions like AWS Lambda etc. (we are still looking into
how this can be done) and Google App Engine apps.

Stream Ops design differences also supports a wider set of use cases than Kafka does out-of-the-box. We will
describe these differences as the code that provides them is cleaned up and released.

Stream Ops is developed by Nanosai: [http://nanosai.com](https://nanosai.com)


<a name="stream-ops-tutorial"></a>

# Stream Ops Tutorial
We have a Stream Ops tutorial in progress here:

[http://tutorials.jenkov.com/stream-ops-java/index.html](http://tutorials.jenkov.com/stream-ops-java/index.html)




<a name="1brs"></a>

# The 1 Billion Records Per Second Challenge
We (Nanosai) will try to get Stream Ops to be able to process 1 billion records per second. You can read more about
that challenge at [The 1BRS Challenge](https://nanosai.com/1brs-challenge).


<a name="license"></a>

# License
The intention is to release Stream Ops under the Apache License 2.0 . We need to discuss this a bit further internally
in Nanosai, but in any case, it will be something close to that.


<a name="getting-started">

# Getting Started

The easiest way to use Stream Ops is via Maven. See the Maven dependency for Stream Ops in the next section.

Alternatively you can clone Stream Ops and build it yourself. Stream Ops have 2 small dependencies which you may
(or may not) have to clone too. That depends on what you are trying to do. The dependencies are also available
from the central Maven repository though, so it is not necessary to clone and build them. Not unless you want
to compile them all using a different Java version than the Java version they are currently built with.


<a name="maven-dependency"></a>
# Maven Dependency

If you want to use Stream Ops with Maven, the Maven dependency for Stream Ops looks like this:

    <dependency>
        <groupId>com.nanosai</groupId>
        <artifactId>stream-ops</artifactId>
        <version>0.7.0</version>
    </dependency>

Remember to substitute the version number with the version of Stream Ops you want to use. You can see the
version history of Stream Ops further down this page.


<a name="dependencies"></a>

# Dependencies
Stream Ops depends on these two other Nanosai projects:

 - [Mem Ops for Java](https://github.com/nanosai/mem-ops-java)
 - [RION Ops for Java](https://github.com/nanosai/rion-ops-java)



<a name="version-history"></a>

# Version History

| Version | Java Version | Change |
|---------|--------------|--------|
| 0.7.0   | Java 8       | First release |



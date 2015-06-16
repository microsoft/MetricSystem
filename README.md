# MetricSystem
Suite of libraries and applications for collecting high volume performance counter data

## Documentation

A full set of documentation is (or will be!) available in the doc/ directory of the code.

## Structure

This code is broken into a set of libraries (some with interdependencies) and associated unit test code. The following libraries are provided:
* Client: Client for reading from and writing to MetricSystem servers.
* Configuration: Code for providing JSON-based configuration of counters and the (unreleased) service.
* Data: The core data libraries for reading/writing/querying raw MetricSystem data.
* Schemas: Bond schemas used when interacting with MetricSystem server APIs.
* Server: An HTTP server which provides RESTful API access to underlying MetricSystem data (on this and other servers).
* Utilities: Stuff that didn't fit elsewhere and was commonly used across multiple projects. Every sufficiently large project has one of these. There's probably a So-and-so-from-usenet's law for this.


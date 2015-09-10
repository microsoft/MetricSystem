# General notes

All REST APIs can currently be supplied with data either in Bond compact binary format or JSON (note the JSON is internally deserialized to Bond, so Bond
JSON peculiarities around certain elements must be honored). Additionally, either Bond CB or JSON can be retrieved.

The MIME type used to indicate Bond is `bond/compact-binary`, for JSON it is the standard `application/json`.

When describing individual commands this document will use JSON-format values. For referencing the Bond types you can use the Schemas library.

## Pattern matching

Some commands the support the use of simplified 'glob' style patterns for filtering. Currently only the * (zero or more characters) and ? (one character)
wildcards are supported. E.g. `/Foo*` will match `/Foo`, `/Foo/Bar`, etc.

## Timestamps

Timestamps provided in commands or returned in responses all use a common format. They are represented as
64 bit integers measuring the milliseconds elapsed since the Unix epoch (1970/01/01 00:00:00 UTC). All
timestamps provided are UTC.

### Timestamps in query parameters

Except these. They can contain any rational format that .NET's [DateTime.Parse](https://msdn.microsoft.com/en-us/library/system.datetime.parse.aspx) method.
The strongly recommended format is ISO 8601 with included timezone information (`o` when using ToString with
a format parameter in .NET). If timezone information is not provided the default behavior is to use the
local time of the service. It is strongly recommended to provide timezone data.

## Tiered / aggregate requests

Queries can be issued with a POST body indicating the desired source(s) to be queried. In this case the server internally will issue queries using
the tiered query client library (see Client project). Note that this is not required for querying a single source.

### ServerInfo objects
Each source is represented as a ServerInfo object which has two elements:
* Hostname (required): the hostname (or IP) of the source to query.
* Port (optional): the port to issue queries to the host for. If omitted the default `4200` is used.

Sample:
    {"Hostname":"some.host.name", "Port":4200}


### TieredRequest objects

The TieredRequest object declares a set of sources to query, along with parameters to control how querying is performed. The object has the following elements:
* Sources (required): a list of ServerInfo objects to query against (required).
* FanoutTimeoutInMilliseconds (optional): an integer indicating how long to wait for down-level sources to respond. If omitted the default `300` is used.
* MaxFanout (optional): an integer indicating the maximum number of sources to directly query. If more sources are provided the queries will be tiered and downstream sources will be asked to request from multiple sources. If omitted the default `50` is used.
* IncludeRequestDiagnostics (optional): a boolean indicating whether extended diagnostic information for each source should be returned with the response. If omitted the default `true` is used.

Sample tiered request:

    {
     "Sources":[
        {"Hostname":"Endpoint1.Hostname"},
        {"Hostname":"Endpoint2.Hostname","Port":5100}],
     "FanoutTimeoutInMilliseconds":10000,
     "MaxFanout":20
    }
     

### TieredResponse and RequestDetails objects

All responses to endpoints capable of tiering derive from the TieredResponse object. This object defines a single member, `RequestDetails`, which is a list of RequestDetails objects.

RequestDetails objects have the following members:
* Server: a ServerInfo object representing the associated query destination for the details.
* Status: an enumeration value indicating the status of the request, possible values are:
  * 0: Success
  * 1: TimedOut
  * 2: RequestException (indicating a local exception when attempting the query)
  * 3: ServerFailureResponse (the server returned a non-200 status code)
  * 4: FederationError (indicating the server query details are unknown because it was queried by a downstream endpoint and that query failed)
* HttpResponseCode: an integer containing the response code of the queried endpoint (e.g. 200, 404, etc).
* StatusDescription: a string containing the associated response value of the endpoint.
* IsAggregator: a boolean indicating whether this endpoint was asked to query multiple downstream endpoints.

Sample:

    {
     "RequestDetails":[
        {"Server":{"Hostname":"some.hostname","Port":4200},
         "Status":0,
         "HttpResponseCode":200,
         "StatusDescription":"OK",
         "IsAggregator":false},
        {"Server":{"Hostname":"some.other.hostname","Port":5309},
         "Status":1,
         "HttpResponseCode":0,
         "StatusDescription":"",
         "IsAggregator":false}
    }

## /counters: Query information about one or more counters

This endpoint allows the user to query data relating to counters. It has two subcommands, one for querying
about counter metadata (what dimensions and values for dimensions exist, etc) and another for querying
about actual counter data, with allowances for filtering by one or more dimensions.

For all calls to the /counters endpoint the format is `/counters[/counter name or pattern][/command][?query parameters]`.
The two supported commands are `info` and `query`.

### Query parameters

Both the info and query commands can take zero or more query parameters in order to scope their operation.
Common parameters include:
* start: specifies a start time for the query.
* end: specifies an end time for the query.
* environment: specifies that the query should be restricted to services with a matching environment value.
* machineFunction: specifies that the query should be restricted to services with a matching 'machine function' value.
* datacenter: specifies that the query should be restricted to services with a matching datacenter value.

Additionally the following parameters are reserved names (and discussed below:
* machine: To be deprecated
* percentile: controls queries against histogram counters
* aggregate: controls the behavior of time series aggregation when querying counters
* dimension: allows operations against a specific dimension (or pattern of dimensions)

Beyond this any query parameter is treated as a reference to the dimension of the counter in question.
For example, when using the parameter 'taco=spicy' you are instructing the query to match data for the
'taco' dimension only when it contains the value 'spicy'. Further applications of this are discussed
below.

### Info sub-command

The info command can be used to retrieve a variety of information about counters. Each info query has a
`CounterInfoResponse` object containing zero or more `CounterInfo` objects. These objects have the
following members:
* Name: a string containing the name of the counter for which information was retrieved.
* Type: an integer enumeration containing the type of counter. Valid values are:
  * 0: HitCount
  * 1: Histogram
  * 2: Unknown (this should never appear but is part of the enumeration)
* StartTime: a 64 bit integer timestamp marking the beginning of data queried.
* EndTime: a 64 bit integer timestamp marking the end of data queried.
* Dimensions: a list of strings with the names of known dimensions for the counter.
* DimensionValues: a list of string followed by a child list of strings where each pair represents the name of a dimension and its associated values.

When querying information about multiple counters 'glob' patterns can be used. For example, to get basic
information on all counters beginning with `/Foo` you can issue a query to `/counters/Foo*/info`.

A variety of query parameters can be used to scope operations when using the info command. The common
parameters above work as described, and in addition the special 'dimension' parameter can be used to
request dimension values for one or more dimensions. When supplying the 'dimension' parameter the query
performs glob pattern matching on the value supplied. For example to get all values for all dimensions
for counter `/Foo` you would use the query `/counters/Foo/info?dimension=*`. In addition it is possible
to get only dimension values matching a specific pattern. For example, to get all dimension values
for the 'taco' dimension which include the text 'spicy' you would use the query '/counters/Foo/info?dimension=taco&taco=*spicy*`.`

Sample (`/counters/Demo/*/info?dimension=*`):

    {"Counters":[
        {"Name":"/Demo/Histogram",
         "Type":1,
         "StartTime":1434681624000,
         "EndTime":1434681631000,
         "Dimensions":["a","b","c"],
         "DimensionValues":[
            ["a",["1","ok"],
             "b",["2"],
             "c",["","banana"]
            ]
         ]},
        {"Name":"/Demo/HitCount",
         "Type":0,
         "StartTime":1434681640000,
         "EndTime":1434681643000,
         "Dimensions":["a","b","c"],
         "DimensionValues":[
            ["a",[""],
             "b",[""],
             "c",[""]
            ]
         ]
        }
    ]}

### Query sub-command

The query command is used to retrieve matching data for a single counter. For queries against multiple counters
please see the the `/batch` command documentation. Each query command returns a `CounterQueryResponse` object
with the following structure:
* UserContext: a string providing an optional piece of user context. Used in batch queries.
* HttpResponseCode: an integer representing the HTTP response code of the underlying query. This code may
  have the following values:
  * 200: The query was at least partially successful (partial in the case of tiered queries).
  * 400: The query was malformed.
  * 404: No matching data was found.
  * 409: There were multiple distinct errors when performing a tiered query. If diagnostics is used the
         individual server errors will be provided where possible.
* ErrorMessage: a descriptive error message in failure conditions.
* Samples: a list of `DataSample` objects containing the response data.

DataSample objects contain the following members:
* Name: a string containing the name of the queried counter.
* Dimensions: a list of string pairs containing the relevant dimensions for the sample. This will include
              any explicitly queried dimensions and, in the case of "split" requests also the current value
              for the split dimension.
* StartTime: a 64 bit integer timestamp indicating the start time for this sample.
* EndTime: a 64 bit integer timestamp indicating the end time for this sample.
* SampleType: an integer enumeration indicating the type of sample being returned. Possible values are:
  0. None (should not be returned)
  1. HitCount (a direct count of the total hits represented by the query)
  2. Histogram (a set of key/value pairs representing the histogram for this query)
  3. Percentile (the value at a specified percentile for a histogram query)
  4. Average (the average value for a histogram query)
  5. Maximum (the maximum value for a histogram query)
  6. Minimum (the minimum value for a histogram query)
* HitCount: a 64 bit integer containing the total hit count for the query. Only valid for 'HitCount'
            sample type.
* Histogram: a list of 64 bit integer and 32 bit unsigned integer pairs containing each element in the
             histogram. The first item in the pair is the value, and the second is the number of times the
             value occurs in the histogram. Values are not sorted. Only valid for 'Histogram' sample type.
* SampleCount: a 64 bit unsigned integer containing the total number of relevant samples for a histogram
               counter. Valid for Histogram, Percentile, Average, Minimum, and Maximum sample types.
* Percentile: a double precision floating point value indicating the percentile queried against (from
              0 to 100 inclusive). Only valid for 'Percentile' sample type.
* PercentileValue: a 64 bit integer containing the value at the requested percentile. Only valid for
                   'Percentile' sample type.
* Average: a 64 bit integer containing the average of all values in a histogram. Only valid for 'Average'
           sample type.
* MinValue: a 64 bit integer containing the minimum value in a histogram. Only valid for 'Minimum' sample type.
* MaxValue: a 64 bit integer containing the maximum value in a histogram. Only valid for 'Maximum' sample type.
* MachineCount: a 32 bit unsigned integer containing the total number of machines which provided data for
                this sample. Only set for aggregated queries. This value may be different than the total
                number of responding machines if some machines did not contain data for the specific query
                parameters.

When performing queries it is possible to filter and aggregate the data in specific ways. Common queries
will tend to provide a start and end time, and potentially specific dimension values to filter data to.
For example to retrieve data from the `/Foo` counter for dimensions `bar` and `baz` where `bar` values
match the pattern 'fancy' and the 'baz' dimension has the exact value 'lad' you would use the query
`/counters/Foo/query?start=[start time]&end=[end time]&bar=*fancy*&baz=lad`.

## /write: Write data to one or more counters

This endpoint allows writing values into a server's data store. The full path of the request is used as the counter name. E.g. `http://localhost:4200/write/Foo`
will write to the `/Foo` counter. The data to write should be supplied in a POST body to the endpoint. GET queries are not supported.

Writes are guaranteed to be atomic; either all succeed or no data is written.

The write endpoint does not support tiering or pattern matching of counter names.

Each write request can send one or more CounterWriteOperation objects. Each CounterWriteOperation object
contains the following members:
* Value (required): a 64 bit integer indicating the value to be written to the counter.
* Count (optional): a 64 bit integer indicating the number of times to write the value (useful for histograms).
* DimensionValues (optional): A list of paired strings of associated dimensions and their values to assign to the write.
  Unknown dimensions will be ignored, and dimensions configured for the counter but not supplied by the write
  will have a wildcard value written to them.
* Timestamp (optional): a 64 bit integer expressing the time the write should be recorded for.

Sample:

    {"Writes":[
        {"Value": 1,
         "DimensionValues": ["Dimension1","Value1", "Dimension2","Value2"]
        },
        {"Value": 867,
         "Count": 5309,
         "Timestamp": 1434674651714
        }
    }


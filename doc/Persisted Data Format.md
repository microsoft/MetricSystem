# Serialization Format

MetricSystem data is serialized using a custom untagged protocol. This protocol evolved over time from work done to
enhance performance of reading/writing large (many hundred MB) datasets.

## Variable Length Encoding

Many integer values in the serialized data use the standard VLE approach. This encodes integers as one or more bytes,
depending on their size. From least to most significant bits (right to left) in a value 7 bits are removed and written
as a byte value with the final bit (0x80) set if there are additional bits afterwards. For 2 byte integers this requires
a maximum of 3 bytes to encode, for 4 byte integers this requires a maximum of 5 bytes, and for 8 byte integers a maximum
of 10 bytes. Since many values within those ranges tend to be much smaller the overall result is a large savings in
storage space.

Values which use VLE are called out as such in the format description. All values not marked as using VLE use fixed-length 
encoded little-endian values.

## String Encoding

All strings are written as follows:

* string length (4 byte VLE integer)
* characters (each character is a 2 byte VLE integer, consistent with .NET's "char" type)

Strings are not terminated with `\0`.

## Header

Each table of data is prefixed with a header containing metadata for the table. This header can be scanned without
fully loading the data in cases where only the header values are useful. The entire header is compressed using the Deflate
compression algorithm.

The header is written as follows:

 * name (string)
 * start time (VLE 8 byte integer representing milliseconds since Unix epoch time 1970-01-01 00:00:00 UTC)
 * end time (VLE 8 byte integer representing milliseconds since Unix epoch time)
 * type of the data (VLE 4 byte integer, currently only hitcounts and histograms are supported)
 * count of data sources (VLE 4 byte integer)
 * 0 or more data sources
 * DimensionSet specification
 * Count of elements in the subsequent data block (VLE 4 byte integer)

### Data Sources

Each data source is encoded with the name of the source and the status of that source for the associated dataset. Sources
may be:

* Unknown (the source was known but data was not retrieved)
* Available (the source provided data for the entire range of time for the dataset)
* Unavailable (the source provided no data for the entire range of time for the dataset)
* Partial (the source provided data, but not for the entire range of time for the dataset)

Sources are written as:

* Name (string, representing the FQDN of the data source)
* Status (VLE 4 byte integer)

### Dimension Sets

Dimension sets represent the set of known dimensions for the associated dataset. A dataset may have zero or more dimensions.
Within a dataset dimensions are considered ordered (see the definition of keys).

Dimension sets are written as:

* Count of dimensions (VLE 4 byte integer)
* Zero or more Dimensions

#### Dimensions

Dimensions represent a single dimension slice for the dataset. Each dimension has a name and zero or more associated values.
Dimension names must be unique within a single dataset. Values are considered ordered (see the definition of keys)

Dimensions are written as:
* Name (string)
* Count of values (VLE 4 byte integer)
* Zero or more values (strings)

## Data table

Following the header is the data table. Depending on the type of data the format will differ. Data tables consist of zero
or more pairings of keys with values. The nature of the value is different depending on the table, and the table may have
supplemental data at the end (histograms do, hit counts do not). Data tables are not compressed.

### Key

Each key is a collection of zero or more 4 byte integers which reflect back to their paired dimensions (i.e. the first integer
is a reference to the first dimension, and so on). The integer values specify which specific value in the dimension is
associated with the key. For example a basic key tuple like (867, 5309) says the key refers to the combination of the 867th
string in the first dimension, and the 5,309th in the second. For arguments sake let's call those ("Tommy" and "Tutone").

There is a special value representing a "null" or "unset" dimension value within a key which is an integer with all bits set
(`0xffffffff`).

All key values use fixed-length encoding.

The data table has the following format:

* Key1, Value1
* Key2, Value2
* ...
* KeyN, ValueN
* With possible supplemental data at the end of the buffer.

### Hit count tables

Tables storing a counts for a set of keys (hit count) have no supplemental data. The value associated with each key is an 8
byte fixed-length integer representing the total for that particular key.

### Histogram tables

Tables storing histogram data for a set of keys have additional data at the end of the table. The value associated with each
key is a 4 byte fixed-length integer pointing to the beginning of the data for the given key. For example a key with a value
of 42 has data beginning at byte 42 in the supplemental buffer.

Histograms are encoded as follows:

* Byte length of serialized histogram (4 byte fixed-length integer)
* Values

Histograms may either be written as a direct sequence of values, or compressed into values and their associated counts within
the histogram. When histograms are compressed the most significant bit (`0x8000000`) of the length is set.

For uncompressed histograms the data is written as a sequence of VLE 8 byte integers.

For compressed histograms the data is written as a sequence of tuples of (VLE 8 byte integer value, VLE 4 byte integer count).

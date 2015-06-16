namespace MetricSystem
{
    public static class RestCommands
    {
        /// <summary>
        /// POST command. Request body is BatchQueryRequest. Response is BatchQueryResponse
        /// </summary>
        public const string BatchQueryCommand = "/batch";

        /// <summary>
        /// POST with a CounterWriteRequest body to write one or more values into a single machine. Fanout is not
        /// supported. The counter name should be specified after write (e.g. /write/Some/Counter)
        /// </summary>
        public const string CounterWriteCommand = "/write";

        /// <summary>
        /// Root Prefix of all counter requests (list all counters, list dimensions, query, etc). 
        /// Subcommands can be GET or POST. No subcommand requires a POST with a CounterQueryRequest body
        /// </summary>
        public const string CounterRequestCommand = "/counters";

        /// <summary>
        /// GET will query information for counters on this machine as a CounterInfoResponse.
        /// POST will fan out to other machines. POST body is a TieredRequest, response is AggregatedListDataResponse.
        /// </summary>
        public const string CounterInfoCommand = "info";

        /// <summary>
        /// GET for basic query to a single machine with dimension filtering /counters/&lt;pattern&gt;/query?dim1=12;dim2=abc
        /// POST can fan out to other machines. POST body is CounterQueryRequest body.
        /// </summary>
        public const string CounterQueryCommand = "query";
    }
}

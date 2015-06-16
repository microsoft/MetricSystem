// ---------------------------------------------------------------------
// <copyright file="Counter.cs" company="Microsoft">
//       Copyright 2014 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Data
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.IO;

    /// <summary>
    /// Handles data sealing events from individual counters.
    /// </summary>
    /// <param name="source">Source counter which was sealed.</param>
    /// <param name="startTime">Start time of the sealed segment.</param>
    /// <param name="endTime">End time of the sealed segment.</param>
    public delegate void CounterDataSealedHandler(Counter source, DateTimeOffset startTime, DateTimeOffset endTime);

    [Flags]
    public enum CounterFlags
    {
        AggregationEnabled = 0x1,
    }

    public abstract class Counter
    {
        internal readonly IDataSet DataSet;

        internal Counter(IDataSet dataSet)
        {
            if (dataSet == null)
            {
                throw new ArgumentNullException("dataSet");
            }

            this.DataSet = dataSet;
            this.DataSet.OnDataSealed +=
                (start, end) =>
                {
                    if (this.OnCounterDataSealed != null)
                    {
                        this.OnCounterDataSealed(this, new DateTimeOffset(start), new DateTimeOffset(end));
                    }
                };
        }

        /// <summary>
        /// Name of the counter.
        /// </summary>
        public string Name
        {
            get { return this.DataSet.Name; }
        }

        /// <summary>
        /// Type of the counter.
        /// </summary>
        public abstract CounterType Type { get; }

        /// <summary>
        /// All dimensions for the counter (may be empty).
        /// </summary>
        public IEnumerable<string> Dimensions
        {
            get { return this.DataSet.GetDimensions(); }
        }

        public DateTimeOffset StartTime
        {
            get { return this.DataSet.StartTime; }
        }

        public DateTimeOffset EndTime
        {
            get { return this.DataSet.EndTime; }
        }

        /// <summary>
        /// Flags applied to this counter.
        /// </summary>
        public CounterFlags Flags { get; internal set; }

        /// <summary>
        /// Event fired when data is sealed.
        /// </summary>
        [SuppressMessage("Microsoft.Design", "CA1009:DeclareEventHandlersCorrectly")]
        public event CounterDataSealedHandler OnCounterDataSealed;

        /// <summary>
        /// Get all values for a particular dimension.
        /// </summary>
        /// <param name="dimensionName">Name of the dimension.</param>
        /// <param name="filterDims">Filter dimensions</param>
        /// <returns>An enumerator of values for the dimension (may be empty).</returns>
        public IEnumerable<string> GetDimensionValues(string dimensionName, DimensionSpecification filterDims)
        {
            if (filterDims == null)
            {
                throw new ArgumentNullException("filterDims");
            }
            return this.DataSet.GetDimensionValues(dimensionName, filterDims);
        }

        /// <summary>
        /// Serialize into the persisted data protocol the counter data between the provided times.
        /// </summary>
        /// <param name="startTime">Start of the time range.</param>
        /// <param name="endTime">End of the time range.</param>
        /// <param name="destination">Destination stream for the serialized data.</param>
        /// <returns></returns>
        public bool SerializeData(DateTime startTime, DateTime endTime, Stream destination)
        {
            return this.DataSet.Serialize(startTime, endTime, destination);
        }

        /// <summary>
        /// Execute a query against the counter data.
        /// </summary>
        /// <param name="queryParameters">Query parameters.</param>
        /// <returns>An enumeration of <see cref="DataSample"/>s matching the query parameters.</returns>
        public IEnumerable<DataSample> Query(DimensionSpecification queryParameters)
        {
            QuerySpecification querySpec;
            if (queryParameters == null)
            {
                throw new ArgumentNullException("queryParameters");
            }

            var filterDims = this.ProcessQueryParameters(queryParameters, out querySpec);
            if (filterDims == null)
            {
                return null;
            }

            return this.DataSet.QueryData(filterDims, querySpec);
        }

        private DimensionSpecification ProcessQueryParameters(DimensionSpecification queryParameters,
                                                              out QuerySpecification querySpec)
        {
            var filterDimensions = ExtractQuerySpec(queryParameters, out querySpec);
            if (querySpec.QueryType == QueryType.Average && !this.DataSet.SupportsAverageQuery)
            {
                querySpec.QueryType = QueryType.Normal;
            }
            else if (querySpec.QueryType == QueryType.Percentile && !this.DataSet.SupportsPercentileQuery)
            {
                querySpec.QueryType = QueryType.Normal;
            }
            return filterDimensions;
        }

        internal static DimensionSpecification ExtractQuerySpec(DimensionSpecification queryParameters,
                                                                out QuerySpecification querySpec)
        {
            var filterDimensions = new DimensionSpecification();
            querySpec = new QuerySpecification();

            foreach (var param in queryParameters)
            {
                if (string.Equals(param.Key, ReservedDimensions.DimensionDimension, StringComparison.OrdinalIgnoreCase))
                {
                    querySpec.CrossQueryDimension = param.Value;

                    // Ensure that split dimension is not also sent a filter dimension
                    if (queryParameters.ContainsKey(param.Value))
                    {
                        return null;
                    }
                }
                else if (string.Equals(param.Key, ReservedDimensions.AggregateSamplesDimension, StringComparison.OrdinalIgnoreCase))
                {
                    querySpec.Combine = Convert.ToBoolean(param.Value);
                }
                else if (string.Equals(param.Key, ReservedDimensions.PercentileDimension, StringComparison.OrdinalIgnoreCase))
                {
                    if (string.Equals(param.Value, ReservedDimensions.PercentileDimensionValueForAverage,
                                      StringComparison.OrdinalIgnoreCase))
                    {
                        querySpec.QueryType = QueryType.Average;
                    }
                    else if (string.Equals(param.Value, ReservedDimensions.PercentileDimensionValueForMaximum,
                                           StringComparison.OrdinalIgnoreCase))
                    {
                        querySpec.QueryType = QueryType.Maximum;
                    }
                    else if (string.Equals(param.Value, ReservedDimensions.PercentileDimensionValueForMinimum,
                                           StringComparison.OrdinalIgnoreCase))
                    {
                        querySpec.QueryType = QueryType.Minimum;
                    }
                    else
                    {
                        querySpec.QueryType = QueryType.Percentile;
                        querySpec.Percentile = double.Parse(param.Value);
                    }
                }
                else
                {
                    filterDimensions.Add(param.Key, param.Value);
                }
            }

            return filterDimensions;
        }

        /// <summary>
        /// Determine if this counter should be aggregated.
        /// </summary>
        /// <returns>True if aggregation should be performed.</returns>
        internal bool ShouldAggregate()
        {
            return (int)(this.Flags & CounterFlags.AggregationEnabled) != 0;
        }
    }
}

namespace MetricSystem
{
    public static class ReservedDimensions
    {
        /// <summary>
        /// For any dimension value which you would like to set as 'empty' or 'other' use this constant.
        /// </summary>
        public const string EmptyDimensionValue = null;

        /// <summary>
        /// Reserved dimension used to select a machine.
        /// </summary>
        /// TODO: consider removing, querying services provide this mechanism in a more robust fashion.
        public const string MachineDimension = "machine";

        /// <summary>
        /// Reserved dimension used to select a machine.
        /// </summary>
        public const string MachineFunctionDimension = "machinefunction";

        /// <summary>
        /// Reserved dimension used to select an environment.
        /// </summary>
        public const string EnvironmentDimension = "environment";

        /// <summary>
        /// Reserved dimension used to select a datacenter.
        /// </summary>
        public const string DatacenterDimension = "datacenter";

        /// <summary>
        /// Reserved dimension used to indicate a start time.
        /// </summary>
        public const string StartTimeDimension = "start";

        /// <summary>
        /// Reserved dimension used to indicate an end time.
        /// </summary>
        public const string EndTimeDimension = "end";

        /// <summary>
        /// Reserved dimension used to indicate a percentile of data.
        /// </summary>
        public const string PercentileDimension = "percentile";

        /// <summary>
        /// Special Dimension Value for Percentile Dimension used to hack in
        /// an average operation.
        /// </summary>
        public const string PercentileDimensionValueForAverage = "average";

        /// <summary>
        /// Special Dimension Value for Percentile Dimension used to hack in
        /// an maximum operation.
        /// </summary>
        public const string PercentileDimensionValueForMaximum = "maximum";

        /// <summary>
        /// Special Dimension Value for Percentile Dimension used to hack in
        /// an minimum operation.
        /// </summary>
        public const string PercentileDimensionValueForMinimum = "minimum";

        /// <summary>
        /// Reserved dimension used to specify if the samples matching the query
        /// need to aggregated entirely at the machine level.
        /// </summary>
        public const string AggregateSamplesDimension = "aggregate";

        /// <summary>
        /// Reserved dimension used to provide dimension names in the query system.
        /// Additionally reserved because having a dimension called 'dimension' would be confusing and overly meta.
        /// </summary>
        public const string DimensionDimension = "dimension";
    }
}

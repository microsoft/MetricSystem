// ---------------------------------------------------------------------
// <copyright file="QuerySpecification.cs" company="Microsoft">
//       Copyright 2013 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Data
{
    internal class QuerySpecification
    {
        public bool Combine;
        public QueryType QueryType = QueryType.Normal;
        public string CrossQueryDimension;
        public double Percentile = double.NaN;

        public bool IsCrossQuery
        {
            get { return !string.IsNullOrEmpty(this.CrossQueryDimension); }
        }

        public override string ToString()
        {
            var result = this.Combine ? "Combined" : "Bucketed";
            result += ", " + this.QueryType;
            if (this.IsCrossQuery)
            {
                result += ", Split: " + this.CrossQueryDimension;
            }
            if (this.QueryType == QueryType.Percentile)
            {
                result += ", Percentile: " + this.Percentile;
            }

            return result;
        }
    }

    internal enum QueryType
    {
        Normal,
        Percentile,
        Average,
        Maximum,
        Minimum,
    }
}

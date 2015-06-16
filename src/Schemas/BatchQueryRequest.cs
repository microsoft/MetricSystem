// ---------------------------------------------------------------------
// <copyright file="BatchQueryRequest.cs" company="Microsoft">
//       Copyright 2015 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem
{
    using System.Collections.Generic;

    using Bond;

    /// <summary>
    /// List of Counter Queries to be processed in a batch
    /// </summary>
    [Schema]
    public class BatchQueryRequest : TieredRequest
    {
        public BatchQueryRequest()
        {
            this.Queries = new List<BatchCounterQuery>();
        }

        /// <summary>
        /// Individual queries.
        /// </summary>
        [Id(1), Required, Type(typeof(List<BatchCounterQuery>))]
        public IList<BatchCounterQuery> Queries { get; set; }

        public override object Clone()
        {
            return Clone(this);
        }

        protected override void CloneProperties(TieredRequest request)
        {
            base.CloneProperties(request);

            var obj = (BatchQueryRequest)request;
            obj.Queries = new List<BatchCounterQuery>(this.Queries);
        }
    }
}

// ---------------------------------------------------------------------
// <copyright file="CounterWriteRequest.cs" company="Microsoft">
//       Copyright 2015 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem
{
    using System.Collections.Generic;

    using Bond;

    [Schema]
    public sealed class CounterWriteRequest
    {
        public CounterWriteRequest()
        {
            this.Writes = new List<CounterWriteOperation>();
        }

        [Id(1), Required, Type(typeof(List<CounterWriteOperation>))]
        public IList<CounterWriteOperation> Writes { get; set; }
    }
}

// ---------------------------------------------------------------------
// <copyright file="ServerRegistration.cs" company="Microsoft">
//       Copyright 2015 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem
{
    using System.Collections.Generic;

    using Bond;
    using Bond.Tag;

    /// <summary>
    /// Details of MetricSystem server registered with aggregation server
    /// </summary>
    [Schema]
    public class ServerRegistration
    {
        public ServerRegistration()
        {
            this.Hostname = string.Empty;
            this.MachineFunction = string.Empty;
            this.Datacenter = string.Empty;
            this.Counters = new List<CounterInfo>();
        }

        /// <summary>
        /// Server hostname.
        /// </summary>
        [Id(1), Required]
        public string Hostname { get; set; }

        /// <summary>
        /// Server port number.
        /// </summary>
        [Id(2), Required]
        public ushort Port { get; set; }

        /// <summary>
        /// Machine function.
        /// </summary>
        [Id(3), Required]
        public string MachineFunction { get; set; }

        /// <summary>
        /// Datacenter the server is registering from.
        /// </summary>
        [Id(4), Required]
        public string Datacenter { get; set; }

        /// <summary>
        /// Counters hosted by the registering server.
        /// </summary>
        [Id(5), Type(typeof(List<CounterInfo>))]
        public IList<CounterInfo> Counters;
    }
}

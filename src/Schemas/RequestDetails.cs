// ---------------------------------------------------------------------
// <copyright file="RequestDetails.cs" company="Microsoft">
//       Copyright 2015 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem
{
    using Bond;

    /// <summary>
    /// Diagnostic data about an individual response sent to the remote MS server
    /// </summary>
    [Schema]
    public class RequestDetails
    {
        public RequestDetails()
        {
            this.Server = new ServerInfo();
            this.Status = RequestStatus.Success;
            this.StatusDescription = string.Empty;
        }

        /// <summary>
        /// Server name/address
        /// </summary>
        [Id(1)]
        public ServerInfo Server { get; set; }

        /// <summary>
        /// Did the client request succeed or fail?
        /// </summary>
        [Id(2)]
        public RequestStatus Status { get; set; }

        /// <summary>
        /// Optional. description relevant to the status
        /// </summary>
        [Id(3)]
        public string StatusDescription { get; set; }

        /// <summary>
        /// .NET HttpResponseCode from the server
        /// </summary>
        [Id(4)]
        public short HttpResponseCode { get; set; }

        /// <summary>
        /// Was this host an aggregator node? (Did it fan out to any other machines?)
        /// </summary>
        [Id(5)]
        public bool IsAggregator { get; set; }
    }
}

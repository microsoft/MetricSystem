// ---------------------------------------------------------------------
// <copyright file="RequestStatus.cs" company="Microsoft">
//       Copyright 2015 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem
{
    /// <summary>
    /// Status of an individual client request to a server
    /// </summary>
    public enum RequestStatus
    {
        /// <summary>
        /// Received a response from the server. The server may return its own success or failure code.
        /// </summary>
        Success = 0,

        /// <summary>
        /// Client request timed out.
        /// </summary>
        TimedOut,

        /// <summary>
        /// Client encountered an exception issuing the request.
        /// </summary>
        RequestException,

        /// <summary>
        /// Server sent back a non-200 response
        /// </summary>
        ServerFailureResponse,

        /// <summary>
        /// No direct information from this server as it was part of a failed block request
        /// </summary>
        FederationError,
    }
}

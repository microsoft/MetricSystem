// ---------------------------------------------------------------------
// <copyright file="RequestHandler.cs" company="Microsoft">
//       Copyright 2014 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Server
{
    using System.Threading.Tasks;

    using MetricSystem.Utilities;

    public abstract class RequestHandler
    {
        public enum TaskPoolDesired
        {
            None,
            Default,
            Individual
        }

        /// <summary>
        /// Task runner to use for this handler (set by the Server code).
        /// </summary>
        internal SemaphoreTaskRunner taskRunner;

        /// <summary>
        /// Which type of task pool to use when executing the handler.
        /// </summary>
        public virtual TaskPoolDesired TaskPool { get { return TaskPoolDesired.Default; } }

        /// <summary>
        /// The path prefix ('e.g. /counters') for this handler.
        /// </summary>
        public abstract string Prefix { get; }

        public abstract Task<Response> ProcessRequest(Request request);
    }
}

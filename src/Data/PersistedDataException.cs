// ---------------------------------------------------------------------
// <copyright file="PersistedDataException.cs" company="Microsoft">
//       Copyright 2013 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Data
{
    using System;
    using System.Runtime.Serialization;

    [Serializable]
    public sealed class PersistedDataException : Exception
    {
        public PersistedDataException() { }
        public PersistedDataException(string message) : base(message) { }
        public PersistedDataException(string message, Exception inner) : base(message, inner) { }
        private PersistedDataException(SerializationInfo info, StreamingContext context) : base(info, context) { }
    }
}

// ---------------------------------------------------------------------
// <copyright file="ArraySegmentStream.cs" company="Microsoft">
//       Copyright 2015 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Utilities
{
    using System;
    using System.IO;

    /// <summary>
    /// Helper class used in safely cloning memory streams.
    /// </summary>
    public class ArraySegmentStream : MemoryStream
    {
        public ArraySegmentStream(ArraySegment<byte> segment)
            : base(segment.Array, segment.Offset, segment.Count, false, true)
        {
            this.OriginalSegment = segment;
        }

        public ArraySegment<byte> OriginalSegment { get; private set; }
    }
}

// ---------------------------------------------------------------------
// <copyright file="MergeDataSources.cs" company="Microsoft">
//       Copyright 2014 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Data
{
    /// <summary>
    /// Indicates an object is a source for data merges.
    /// </summary>
    internal interface IMergeSource
    {
        bool MultiValue { get; }
    }

    internal struct SingleValueMergeSource : IMergeSource
    {
        public bool MultiValue { get { return false; } }

        public long Value;

        public SingleValueMergeSource(long value)
        {
            this.Value = value;
        }
    }

    internal struct MultiValueMergeSource : IMergeSource
    {
        public bool MultiValue { get { return true; } }

        public IBufferedValueArray BufferedData;
        public long StartOffset;

        public MultiValueMergeSource(IBufferedValueArray bufferedData, long startOffset)
        {
            this.BufferedData = bufferedData;
            this.StartOffset = startOffset;
        }
    }
}

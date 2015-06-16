// ---------------------------------------------------------------------
// <copyright file="SampleCombiner.cs" company="Microsoft">
//       Copyright 2015 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Client
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.IO;

    using MetricSystem.Utilities;

    /// <summary>
    /// Abstracts the details of combining data samples for aggregation. Does not combine 'envelope' fields such 
    /// as dimensions, name, date/time, etc.
    /// </summary>
    public class SampleCombiner
    {
        /// <summary>
        /// Have we updated the timerange? If so, we need to update the sample range before returning to the caller
        /// </summary>
        private bool hasUpdatedTimeRange;

        [SuppressMessage("Microsoft.Design", "CA1051:DoNotDeclareVisibleInstanceFields")
        ]
        protected DataSample internalSample;

        /// <summary>
        /// TimeRange these samples cover
        /// </summary>
        private TimeRange timeRange;

        public SampleCombiner(DataSample sample)
        {
            if (sample == null)
            {
                throw new ArgumentNullException("sample");
            }

            // validate that this sampletype is acceptable
            switch (sample.SampleType)
            {
            case DataSampleType.Average:
            case DataSampleType.Histogram:
            case DataSampleType.HitCount:
            case DataSampleType.Maximum:
            case DataSampleType.Minimum:
                break;

            default:
                throw new ArgumentException("Cannot combine samples for percentiles or other types");
            }

            this.internalSample = new DataSample
                                  {
                                      SampleType = sample.SampleType,
                                      Dimensions = sample.Dimensions,
                                      Name = sample.Name
                                  };

            this.AddData(sample);
            this.timeRange = new TimeRange(sample.StartTime, sample.EndTime);
            this.hasUpdatedTimeRange = true;
        }

        /// <summary>
        /// Get the final DataSample representing this combined data
        /// </summary>
        public DataSample Data
        {
            get
            {
                if (this.hasUpdatedTimeRange)
                {
                    this.internalSample.StartTime = this.timeRange.Start.ToMillisecondTimestamp();
                    this.internalSample.EndTime = this.timeRange.End.ToMillisecondTimestamp();
                    this.hasUpdatedTimeRange = false;
                }

                return this.internalSample;
            }
        }

        public uint MachineCount
        {
            get { return this.internalSample.MachineCount; }
            set { this.internalSample.MachineCount = value; }
        }

        public TimeSpan ElapsedTime
        {
            get { return this.timeRange.Elapsed; }
        }

        [SuppressMessage("Microsoft.Design",
            "CA1006:DoNotNestGenericTypesInMemberSignatures")]
        public static SampleCombiner Create(IEnumerable<DataSample> samples)
        {
            if (samples == null)
            {
                throw new ArgumentNullException("samples");
            }

            SampleCombiner combiner = null;
            foreach (var sample in samples)
            {
                if (combiner == null)
                {
                    combiner = new SampleCombiner(sample);
                }
                else
                {
                    combiner.AddSample(sample);
                }
            }

            return combiner;
        }

        public void AddSample(DataSample sample)
        {
            if (sample == null)
            {
                throw new ArgumentNullException("sample");
            }

            if (sample.SampleType != this.internalSample.SampleType)
            {
                throw new InvalidDataException(string.Format(
                                                             "Sample Type: {0} does not match combined sample type: {1}",
                                                             sample.SampleType, this.internalSample.SampleType));
            }

            if (sample.StartTime != this.internalSample.StartTime ||
                sample.EndTime != this.internalSample.EndTime)
            {
                this.timeRange = TimeRange.Merge(this.timeRange,
                                                 new TimeRange(sample.StartTime, sample.EndTime));
                this.hasUpdatedTimeRange = true;
            }

            this.AddData(sample);
        }

        public void Merge(SampleCombiner combiner)
        {
            if (combiner == null)
            {
                throw new ArgumentNullException("combiner");
            }

            if (combiner.internalSample.SampleType != this.internalSample.SampleType)
            {
                throw new InvalidDataException(string.Format(
                                                             "Sample Type: {0} does not match combined sample type: {1}",
                                                             combiner.internalSample.SampleType,
                                                             this.internalSample.SampleType));
            }

            this.AddSample(combiner.internalSample);
            this.internalSample.MachineCount = Math.Max(this.internalSample.MachineCount,
                                                        combiner.internalSample.MachineCount);

            //merge the time ranges as well
            this.hasUpdatedTimeRange = true;
            this.timeRange = TimeRange.Merge(this.timeRange, combiner.timeRange);
        }

        /// <summary>
        /// Determine if there is a machine count as part of this sample (e.g. if the sample is an aggregated data sample).
        /// Otherwise return 1
        /// </summary>
        /// <param name="sample"></param>
        /// <returns></returns>
        internal static uint ExtractMachineCount(DataSample sample)
        {
            return sample.MachineCount > 0 ? sample.MachineCount : 1;
        }

        /// <summary>
        /// Add data to the combined sample. Ignore envelope data
        /// </summary>
        /// <param name="sample"></param>
        internal void AddData(DataSample sample)
        {
            switch (this.internalSample.SampleType)
            {
            case DataSampleType.Average:
                this.MergeAverages(sample);
                break;

            case DataSampleType.Histogram:
                this.MergeHistograms(sample);
                break;

            case DataSampleType.Maximum:
                this.MergeMax(sample);
                break;

            case DataSampleType.Minimum:
                this.MergeMin(sample);
                break;
            case DataSampleType.HitCount:
                this.MergeHitCounts(sample);
                break;

            default:
                throw new InvalidOperationException("Impossible to add this data");
            }
        }

        private void MergeAverages(DataSample sample)
        {
            ulong newSampleCount;
            checked
            {
                newSampleCount = sample.SampleCount + this.internalSample.SampleCount;
            }

            // avoid overflow by combining two samples in pieces as opposed to expanding them and re-dividing. 
            // There is a new denominator common to both samples. Scale both samples appropriately
            double oldMulitiplier = (this.internalSample.SampleCount / (double)newSampleCount);
            double newMultiplier = (sample.SampleCount / (double)newSampleCount);

            this.internalSample.Average = (oldMulitiplier * this.internalSample.Average) +
                                          (newMultiplier * sample.Average);
            this.internalSample.SampleCount = newSampleCount;
        }

        private void MergeHitCounts(DataSample sample)
        {
            this.internalSample.HitCount += sample.HitCount;
        }

        private void MergeHistograms(DataSample sample)
        {
            foreach (var pair in sample.Histogram)
            {
                uint existingValue;
                if (!this.internalSample.Histogram.TryGetValue(pair.Key, out existingValue))
                {
                    existingValue = 0;
                }

                this.internalSample.Histogram[pair.Key] = existingValue + pair.Value;
            }

            this.internalSample.SampleCount += sample.SampleCount;
        }

        private void MergeMin(DataSample sample)
        {
            if (sample.MinValue < this.internalSample.MinValue)
            {
                this.internalSample.MinValue = sample.MinValue;
            }

            this.internalSample.SampleCount += sample.SampleCount;
        }

        private void MergeMax(DataSample sample)
        {
            if (sample.MaxValue > this.internalSample.MaxValue)
            {
                this.internalSample.MaxValue = sample.MaxValue;
            }

            this.internalSample.SampleCount += sample.SampleCount;
        }
    }
}

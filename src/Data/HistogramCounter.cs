// The MIT License (MIT)
//
// Copyright (c) 2015 Microsoft
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

namespace MetricSystem.Data
{
    using System;

    using Newtonsoft.Json;

    /// <summary>
    /// Determines the type of value rounding performed for histogram counters.
    /// </summary>
    [JsonConverter(typeof(CounterConfiguration.CounterRoundingConverter))]
    public enum CounterRounding
    {
        /// <summary>
        /// No rounding is performed.
        /// </summary>
        None,

        /// <summary>
        /// Values are rounded to a certain number of significant digits.
        /// </summary>
        SignificantDigits,

        /// <summary>
        /// Values are rounded based on byte boundaries beyond a given threshold.
        /// </summary>
        ByteCount,

        Unknown
    }

    public sealed class HistogramCounter : Counter
    {
        private readonly DataSet<InternalHistogram> histogram;
        private long minimumRoundingValue;
        private CounterRounding rounding = CounterRounding.None;
        private long roundingFactor;
        private WriteValue writeValue;

        internal HistogramCounter(DataSet<InternalHistogram> dataSet)
            : base(dataSet)
        {
            this.histogram = dataSet; // this avoids repeated casting.
            this.writeValue = this.WriteUnroundedValue;
        }

        public override CounterType Type
        {
            get { return CounterType.Histogram; }
        }

        /// <summary>
        /// Specifies the type of rounding to perform on written values.
        /// Changes to this value are <b>UNSAFE</b> if writes are occurring concurrently.
        /// </summary>
        public CounterRounding Rounding
        {
            get { return this.rounding; }
            set
            {
                this.rounding = value;
                switch (this.rounding)
                {
                case CounterRounding.None:
                    break;
                case CounterRounding.ByteCount:
                    this.writeValue = this.WriteRoundedSizeValue;
                    break;
                case CounterRounding.SignificantDigits:
                    this.SetMinimumValueForSignificantDigitRounding();
                    this.writeValue = this.WriteRoundedSignificantDigitsValue;
                    break;
                default:
                    throw new ArgumentException("Unknown rounding type specified.", "value");
                }
            }
        }

        /// <summary>
        /// Specifies the factor to use when rounding.
        /// For significant digit rounding this is the number of significant digits to use.
        /// For byte value rounding this is byte value to round against (e.g. 1024 for 1KB boundaries)
        /// Changes to this value are <b>UNSAFE</b> if writes are occurring concurrently.
        /// </summary>
        public long RoundingFactor
        {
            get { return this.roundingFactor; }
            set
            {
                if (value < 0)
                {
                    throw new ArgumentException("Must provide a non-negative rounding factor.", "value");
                }
                this.roundingFactor = value;
                switch (this.rounding)
                {
                case CounterRounding.SignificantDigits:
                    this.SetMinimumValueForSignificantDigitRounding();
                    break;
                }
            }
        }

        public void AddValue(long value, DimensionSpecification dims)
        {
            this.AddValue(value, dims, DateTime.Now);
        }

        public void AddValue(long value, DimensionSpecification dims, DateTime timestamp)
        {
            this.writeValue(value, dims, timestamp);
        }

        private delegate void WriteValue(long value, DimensionSpecification dims, DateTime timestamp);

        #region Counter rounding handling
        private void WriteUnroundedValue(long value, DimensionSpecification dims, DateTime timestamp)
        {
            this.histogram.AddValue(value, dims, timestamp);
        }

        private void WriteRoundedSignificantDigitsValue(long value, DimensionSpecification dims, DateTime timestamp)
        {
            if (this.roundingFactor > 0 && value > this.minimumRoundingValue)
            {
                long factor = 0;
                while (value > this.minimumRoundingValue)
                {
                    factor += 1;
                    var lastDigit = value % 10;
                    if (lastDigit >= 5)
                    {
                        value += (10 - lastDigit);
                    }
                    value /= 10;
                }

                value *= (long)Math.Pow(10, factor);
            }

            this.histogram.AddValue(value, dims, timestamp);
        }

        private void WriteRoundedSizeValue(long value, DimensionSpecification dims, DateTime timestamp)
        {
            if (value < 0)
            {
                value = 0;
            }

            if (value > this.roundingFactor)
            {
                long remainder = value % this.roundingFactor;
                if (remainder >= (this.roundingFactor / 2))
                {
                    remainder = -remainder; // will roll up
                }
                value = value - remainder;
            }

            this.histogram.AddValue(value, dims, timestamp);
        }

        private void SetMinimumValueForSignificantDigitRounding()
        {
            this.minimumRoundingValue = (long)Math.Pow(10, this.roundingFactor);
        }
        #endregion
    }
}

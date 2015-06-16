// ---------------------------------------------------------------------
// <copyright file="CounterEventWriter.cs" company="Microsoft">
//       Copyright 2015 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Client
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.Tracing;

    using MetricSystem.Utilities;

    [EventSource(Name = "MetricSystem-WriteClient", Guid = "{f31d6d54-e868-4e00-a1ea-f168965a09c1}")]
    public sealed unsafe class CounterEventWriter : EventSource
    {
        private const int MaxEventSize = 65000;

        public static readonly CounterEventWriter Write = new CounterEventWriter();

        // Note these are fake events to enable us to do the real work below.
        [Event(1)]
        internal void FakeIncrement(int dataSize, string data)
        {
            this.WriteEvent(1, dataSize, data);
        }

        [Event(2)]
        internal void FakeAddHistogramValue(int dataSize, string data)
        {
            this.WriteEvent(2, dataSize, data);
        }

        [NonEvent]
        public void Increment(string counterName, IDictionary<string, string> dimensions, long amount)
        {
            var buffer = stackalloc byte[MaxEventSize];

            var offset = WriteCommonDataToBuffer(counterName, dimensions, buffer, MaxEventSize);
            offset += ByteConverter.VariableLengthEncoding.Write((ulong)amount, buffer + offset, MaxEventSize - offset);

            var eventData = stackalloc EventData[2];
            eventData[0].DataPointer = (IntPtr)(&offset);
            eventData[0].Size = sizeof(uint);
            eventData[1].DataPointer = (IntPtr)buffer;
            eventData[1].Size = (int)offset;

            this.WriteEventCore(1, 2, eventData);
        }

        [NonEvent]
        public void AddHistogramValue(string counterName, IDictionary<string, string> dimensions, long value, uint count)
        {
            var buffer = stackalloc byte[MaxEventSize];

            var offset = WriteCommonDataToBuffer(counterName, dimensions, buffer, MaxEventSize);
            offset += ByteConverter.VariableLengthEncoding.Write((ulong)value, buffer + offset, MaxEventSize - offset);
            offset += ByteConverter.VariableLengthEncoding.Write(count, buffer + offset, MaxEventSize - offset);

            var eventData = stackalloc EventData[2];
            eventData[0].DataPointer = (IntPtr)(&offset);
            eventData[0].Size = sizeof(uint);
            eventData[1].DataPointer = (IntPtr)buffer;
            eventData[1].Size = (int)offset;

            this.WriteEventCore(2, 2, eventData);
        }

        private static uint WriteCommonDataToBuffer(string counterName, IDictionary<string, string> dimensions,
                                                    byte* buffer, long bufferSize)
        {
            var offset = WriteStringToBuffer(counterName, buffer, bufferSize);
            offset += ByteConverter.VariableLengthEncoding.Write((ulong)dimensions.Count, buffer + offset,
                                                                 bufferSize - offset);
            foreach (var kvp in dimensions)
            {
                offset += WriteStringToBuffer(kvp.Key, buffer + offset, bufferSize - offset);
                offset += WriteStringToBuffer(kvp.Value, buffer + offset, bufferSize - offset);
            }

            return offset;
        }

        private static uint WriteStringToBuffer(string value, byte* buffer, long bufferSize)
        {
            var offset = ByteConverter.VariableLengthEncoding.Write((ulong)value.Length, buffer, bufferSize);
            foreach (char c in value)
            {
                offset += ByteConverter.VariableLengthEncoding.Write(c, buffer + offset, bufferSize - offset);
            }

            return offset;
        }
    }
}

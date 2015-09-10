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

namespace MetricSystem
{
    using System;

    using Bond;
    using Bond.IO.Safe;
    using Bond.Protocols;

    using MetricSystem.Data;

    public static class Schemas
    {
        /// <summary>
        /// Bond dynamically generates and compiles serialization and deserialization methods on first use. 
        /// This method forces this generation to incur the cost deterministically here and not at first actual use
        /// </summary>
        public static bool Precompile()
        {
            return IsInitialized.Value;
        }

        // using Lazy<> ensures the initializer is only called once per process
        private readonly static Lazy<bool> IsInitialized = new Lazy<bool>(CompileAllSchemas);

        private static bool CompileAllSchemas()
        {
            // Benchmarking shows this takes ~2000 ms
            // XXX: sucks that we enumerate this by hand. I has a sad.
            return ForceCompile<BatchCounterQuery>()
                   && ForceCompile<BatchQueryRequest>()
                   && ForceCompile<BatchQueryResponse>()
                   && ForceCompile<CounterInfo>()
                   && ForceCompile<CounterInfoResponse>()
                   && ForceCompile<CounterQueryResponse>()
                   && ForceCompile<DataSample>()
                   && ForceCompile<MetricSystemResponse>()
                   && ForceCompile<RequestDetails>()
                   && ForceCompile<ServerInfo>()
                   && ForceCompile<ServerRegistration>()
                   && ForceCompile<TieredRequest>()
                   && ForceCompile<TieredResponse>()
                   && ForceCompile<TransferQueryRequest>()
                   && ForceCompile<TransferRequest>();
        }

        private static bool ForceCompile<T>() where T : class, new()
        {
            var t1 = new T();
            var buffer = new OutputBuffer();
            var writer = new CompactBinaryWriter<OutputBuffer>(buffer);
            Bond.Serialize.To(writer, t1);

            var reader = new CompactBinaryReader<InputBuffer>(new InputBuffer(buffer.Data));
            var t2 = Bond.Deserialize<T>.From(reader);

            return Comparer.Equal(t1, t2);
        }
    }
}

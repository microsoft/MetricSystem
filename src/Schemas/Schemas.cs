namespace MetricSystem
{
    using System;
    using System.Diagnostics;

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
                   && ForceCompile<PersistedDataBlockLength>()
                   && ForceCompile<PersistedDataHeader>()
                   && ForceCompile<PersistedDataSource>()
                   && ForceCompile<PersistedDataVersion>()
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

namespace MetricSystem.Utilities.UnitTests
{
    using System.IO;

    using NUnit.Framework;

    [TestFixture]
    public class ByteConverterTests
    {
        private byte[] buffer;
        private MemoryStream memoryStream;

        [SetUp]
        public void Setup()
        {
            this.buffer = new byte[100];
            this.memoryStream = new MemoryStream(buffer);
        }

        [Test]
        public void CanReadWriteZeroVariableLength()
        {
            ByteConverter.VariableLengthEncoding.Write(0, this.memoryStream);
            Assert.AreEqual(1, this.memoryStream.Position);

            this.memoryStream.Position = 0;
            var retrievedValue = ByteConverter.VariableLengthEncoding.ReadUInt64(this.memoryStream);
            Assert.AreEqual(0, retrievedValue);
        }

        [Test]
        public void CanReadWriteMaxULongVariableLength()
        {
            ByteConverter.VariableLengthEncoding.Write(ulong.MaxValue, this.memoryStream);
            Assert.AreEqual(10, this.memoryStream.Position);

            this.memoryStream.Position = 0;
            var result = ByteConverter.VariableLengthEncoding.ReadUInt64(this.memoryStream);
            Assert.AreEqual(ulong.MaxValue, result);
        }

        [Test]
        public void CanReadWriteVariableLength()
        {
            const uint magicValue = 0xdeadbeef;
            ByteConverter.VariableLengthEncoding.Write(magicValue, this.memoryStream);

            this.memoryStream.Position = 0;
            var result = ByteConverter.VariableLengthEncoding.ReadUInt64(this.memoryStream);
            Assert.AreEqual(magicValue, result);
        }

        [Test]
        public unsafe void CanReadWriteToBuffer()
        {
            using (var mem = new MemoryStream())
            {
                const long magicValue = 0xdeadbeef;
                ByteConverter.VariableLengthEncoding.Write(magicValue, mem);

                mem.Position = 0;

                fixed (byte* buffer = mem.GetBuffer())
                {
                    long result;
                    ByteConverter.VariableLengthEncoding.ReadInt64(buffer, (uint)mem.Length, out result);
                    Assert.AreEqual(magicValue, result);
                }
            }
        }
    }
}

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

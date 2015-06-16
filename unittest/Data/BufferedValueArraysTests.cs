// ---------------------------------------------------------------------
// <copyright file="BufferedValueStreamTests.cs" company="Microsoft">
//       Copyright 2014 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Data.UnitTests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text;

    using NUnit.Framework;

    [TestFixture]
    public sealed class BufferedValueArraysTests
    {
        [Test]
        public void CanCreateStreamsForAppropriateIntegerTypes()
        {
            new BufferedValueArray.FixedLengthBufferedValueArray<short>(new byte[sizeof(short)], 0, sizeof(short));
            new BufferedValueArray.FixedLengthBufferedValueArray<ushort>(new byte[sizeof(short)], 0, sizeof(ushort));
            new BufferedValueArray.FixedLengthBufferedValueArray<int>(new byte[sizeof(int)], 0, sizeof(int));
            new BufferedValueArray.FixedLengthBufferedValueArray<uint>(new byte[sizeof(uint)], 0, sizeof(uint));
            new BufferedValueArray.FixedLengthBufferedValueArray<long>(new byte[sizeof(long)], 0, sizeof(long));
            new BufferedValueArray.FixedLengthBufferedValueArray<ulong>(new byte[sizeof(ulong)], 0, sizeof(ulong));
        }

        [Test]
        public void CannotCreateStreamsOfOtherTypes()
        {
            try
            {
                var b = new BufferedValueArray.FixedLengthBufferedValueArray<float>(new byte[sizeof(float)], 0, sizeof(float));
                b.ReadValuesInto(new Dictionary<long, uint>(), 0);
                Assert.Fail();
            }
            catch (TypeInitializationException e)
            {
                Assert.AreEqual(e.InnerException.GetType(), typeof(NotSupportedException));
            }

            try
            {
                var b = new BufferedValueArray.FixedLengthBufferedValueArray<string>(new byte[128], 0, 128);
                b.ReadValuesInto(new Dictionary<long, uint>(), 0);
                Assert.Fail();
            }
            catch (TypeInitializationException e)
            {
                Assert.AreEqual(e.InnerException.GetType(), typeof(NotSupportedException));
            }
        }

        [Test]
        public void CannotCreateValueArraysWithNoData()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => new BufferedValueArray.VariableLengthBufferedValueArray(new byte[128], 0, 0));
        }

        [Test]
        public void CanReadVariableSizedArraysInMultiValueStream()
        {
            var ms = new MemoryStream();
            uint count = 0;
            using (var bw = new BinaryWriter(ms, Encoding.UTF8, true))
            {
                uint len = 0;
                bw.Write((uint)0);

                for (var i = 1; i < 11; ++i)
                {
                    for (var j = 0; j < i; ++j)
                    {
                        bw.Write(i);
                        len += sizeof(int);
                        ++count;
                    }
                }

                ms.Position = 0;
                bw.Write(len);
            }

            using (var buf = new BufferedValueArray.FixedLengthBufferedValueArray<int>(ms.GetBuffer(), 0, (int)ms.Length))
            {
                var data = new Dictionary<long, uint>();
                Assert.AreEqual(count, buf.ReadValuesInto(data, 0));

                for (var i = 1; i < 11; ++i)
                {
                    Assert.AreEqual((uint)i, data[i]);
                }
            }
        }
    }
}

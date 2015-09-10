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
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text;

    using Bond;
    using Bond.IO.Unsafe;
    using Bond.Protocols;

    using Microsoft.IO;

    using NUnit.Framework;

    [TestFixture]
    public class BondStreamTests
    {
        private AnotherDerivedThing testObject;
        private RecyclableMemoryStreamManager mgr = new RecyclableMemoryStreamManager(1024, 1024*10, 20*1024);

        [SetUp]
        public void Init()
        {
            testObject = new AnotherDerivedThing
            {
                secretVal = 0xbeef,
                IntVal = -25,
                doubleVal = 1234.4d,
                Name = "bob",
                shortVal = ushort.MaxValue,
                Dict = new Dictionary<string, string> { { "metric", "system" } }
            };
        }

        [Test]
        public void CanReadAndWritePrimitives()
        {
            const string test = "tacos";
            const float a = float.MinValue;
            const int b = int.MaxValue;
            const ulong c = ulong.MaxValue;
            const double d = double.NegativeInfinity;

            using (var str = new MemoryStream())
            using (var writer = new WriterStream(str))
            {
                writer.WriteString(Encoding.UTF8, test, test.Length);
                writer.WriteFloat(a);
                writer.WriteUInt32(b);
                writer.WriteUInt64(c);
                writer.WriteDouble(d);
                writer.WriteVarUInt32(b);
                writer.WriteVarUInt64(c);
                str.Position = 0;

                using (var reader = new ReaderStream(str))
                {
                    Assert.AreEqual(reader.ReadString(Encoding.UTF8, test.Length), test);
                    Assert.AreEqual(reader.ReadFloat(), a);
                    Assert.AreEqual(reader.ReadUInt32(), b);
                    Assert.AreEqual(reader.ReadUInt64(), c);
                    Assert.AreEqual(reader.ReadDouble(), d);
                    Assert.AreEqual(reader.ReadVarUInt32(), b);
                    Assert.AreEqual(reader.ReadVarUInt64(), c);
                }
            }

        }


        [Test]
        public void CanReadAndWriteSimpleProtocol()
        {

            using (var stream = new MemoryStream())
            using (var writerStream = new WriterStream(stream, this.mgr))
            {
                var writer = writerStream.CreateSimpleBinaryWriter();
                writer.Write(this.testObject);

                stream.Position = 0;
                using (var reader = ReaderStream.FromMemoryStreamBuffer(stream, this.mgr))
                {
                    var sbReader = new SimpleBinaryReader<ReaderStream>(reader);
                    var anotherObject = sbReader.Read<AnotherDerivedThing>();

                    Assert.IsTrue(Comparer.Equal(anotherObject, this.testObject));
                }
            }

        }

        [Test]
        public void CanReadAndWriteCompactBinary()
        {

            using (var stream = new MemoryStream())
            using (var writerStream = new WriterStream(stream))
            {
                var writer = writerStream.CreateCompactBinaryWriter();
                writer.Write(this.testObject);

                stream.Position = 0;
                using (var reader = ReaderStream.FromMemoryStreamBuffer(stream, this.mgr))
                {
                    var cbReader = new CompactBinaryReader<ReaderStream>(reader);
                    var anotherObject = cbReader.Read<AnotherDerivedThing>();

                    Assert.IsTrue(Comparer.Equal(anotherObject, this.testObject));
                }
            }
        }

        [Test]
        public void CanMixAndMatchStreamReaders()
        {
            using (var stream = new MemoryStream())
            {
                // use the 'stock' output stream
                var outputStream = new OutputStream(stream);
                var writer = new CompactBinaryWriter<OutputStream>(outputStream);
                writer.Write(this.testObject);
                outputStream.Flush();

                // read with the custom stream
                stream.Position = 0;
                using (var reader = new ReaderStream(stream.GetBuffer(), 0, stream.Length, this.mgr))
                {
                    var cbReader = reader.CreateCompactBinaryReader();
                    var anotherObject = cbReader.Read<AnotherDerivedThing>();

                    Assert.IsTrue(Comparer.Equal(anotherObject, this.testObject));
                }
            }

        }

        [Test]
        public void CanTranscode()
        {
            using (var stream1 = new MemoryStream())
            using (var stream2 = new MemoryStream())
            using (var writerStream1 = new WriterStream(stream1))
            using (var writerStream2 = new WriterStream(stream2))
            {
                // write into compact binary protocol first
                var writer = writerStream1.CreateCompactBinaryWriter();
                writer.Write(this.testObject);
                stream1.Position = 0;
                using (var readerStream1 = ReaderStream.FromMemoryStreamBuffer(stream1, null))
                {
                    var reader = readerStream1.CreateCompactBinaryReader();
                    // re-write as simple protocol
                    var writer2 = writerStream2.CreateSimpleBinaryWriter();
                    Transcode.FromTo(reader, writer2);
                    stream2.Position = 0;

                    using (var readerStream2 = new ReaderStream(stream2))
                    {
                        var reader2 = readerStream2.CreateSimpleBinaryReader();
                        var anotherObject = reader2.Read<AnotherDerivedThing>();

                        Assert.IsTrue(Comparer.Equal(anotherObject, this.testObject));
                    }
                }
            }
        }

        [Test]
        public void CanReadAndWriteToFile()
        {
            var tempFile = Path.GetTempFileName();
            using (var createFileStream = File.Create(tempFile))
            using (var writerStream = new WriterStream(createFileStream))
            {
                var writer = writerStream.CreateCompactBinaryWriter();
                writer.Write(this.testObject);
                createFileStream.Close();
            }

            using (var openFileStream = File.OpenRead(tempFile))
            using (var readerStream = new ReaderStream(openFileStream))
            {
                var reader = readerStream.CreateCompactBinaryReader();
                var anotherObject = reader.Read<AnotherDerivedThing>();

                Assert.IsTrue(Comparer.Equal(anotherObject, this.testObject));
            }

            File.Delete(tempFile);
        }

        [Test]
        public void CanReadWritePolymorphicTypes()
        {
            using (var stream = new MemoryStream())
            using (var writerStream = new WriterStream(stream))
            {
                var writer = writerStream.CreateCompactBinaryWriter();
                writer.Write(this.testObject);

                stream.Position = 0;
                using (var readerStream = new ReaderStream(stream.GetBuffer(), 0, stream.Length))
                {
                    var reader = readerStream.CreateBondedCompactBinaryReader<Base>();

                    var baseThing = reader.Deserialize<Base>();
                    var derivedThing = reader.Deserialize<AnotherDerivedThing>();
                    Assert.AreEqual(baseThing.secretVal, derivedThing.secretVal);
                    Assert.IsTrue(Comparer.Equal<AnotherDerivedThing>(derivedThing, this.testObject));
                }
            }
        }

        [Test]
        public void CanReadAndWriteEmbeddedPolymorphicTypesFromMemory()
        {
            this.VerifyEmbeddedPolymorphicTypes(() => new MemoryStream(), stream =>
                                                                          {
                                                                              stream.Position = 0;
                                                                              return new MemoryStream(
                                                                                  stream.GetBuffer(), 0,
                                                                                  (int)stream.Length, false, true);
                                                                          });
        }

        [Test]
        public void CanReadAndWriteEmbeddedPolymorphicTypesFromFile()
        {
            var fileName = Path.GetTempFileName();
            this.VerifyEmbeddedPolymorphicTypes(() => 
                new FileStream(fileName, FileMode.Open, FileAccess.ReadWrite, FileShare.Read), stream =>
            {
                stream.Close();
                return new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.Read);
            });
        }

        public void VerifyEmbeddedPolymorphicTypes<TStream>(Func<TStream> createWriter, Func<TStream, TStream> createReaderFromWriter) where TStream : Stream
        {
            const int secretValToUse = 0x12345;

            var listOfThings = new ListOfThings();

            this.testObject.secretVal = secretValToUse;
            listOfThings.list.Add(new Bonded<AnotherDerivedThing>(this.testObject));
            listOfThings.list.Add(new Bonded<DerivedThing>(new DerivedThing
                                                           {
                                                               anotherVal = 100,
                                                               secretVal = secretValToUse
                                                           }));

            using (var writeStream = createWriter())
            using (var writerStream = new WriterStream(writeStream))
            {
                var writer = writerStream.CreateCompactBinaryWriter();
                writer.Write(listOfThings);

                using (var readStream = createReaderFromWriter(writeStream))
                using (var reader = new ReaderStream(readStream, null, false))
                {
                    var cbReader = reader.CreateBondedCompactBinaryReader<ListOfThings>();
                    var newThing = cbReader.Deserialize<ListOfThings>();

                    Assert.IsNotNull(newThing);

                    var matchesFound = 0;

                    foreach (var thing in newThing.list)
                    {
                        var baseThing = thing.Deserialize();

                        var another = thing.TryDeserialize<Base, AnotherDerivedThing>();
                        var derived = thing.TryDeserialize<Base, DerivedThing>();

                        Assert.IsNotNull(baseThing);

                        if (another != null)
                        {
                            Assert.IsNull(derived);
                            Assert.IsTrue(Comparer.Equal<AnotherDerivedThing>(another, this.testObject));
                            matchesFound++;
                        }

                        if (derived != null)
                        {
                            Assert.IsNull(another);
                            Assert.AreEqual(100, derived.anotherVal);
                            matchesFound++;
                        }
                    }
                    Assert.AreEqual(2, matchesFound);
                }
            }
        }
    }

    [Schema]
    public class Base
    {
        [Id(1)]
        public int secretVal { get; set; }
    }

    [Schema]
    public class ListOfThings
    {
        [Id(1)]
        public List<IBonded<Base>> list;

        public ListOfThings()
        {
            this.list = new List<IBonded<Base>>();
        }
    }

    [Schema]
    public class DerivedThing : Base
    {
        [Id(1)]
        public int anotherVal { get; set; }
    }

    [Schema]
    public class AnotherDerivedThing : Base
    {
        [Required]
        [Id(1)]
        public string Name { get; set; }

        [Id(2)]
        public Dictionary<string, string> Dict { get; set; }

        [Id(3)]
        public int IntVal { get; set; }

        [Id(11)]
        public double doubleVal { get; set; }

        [Id(15)]
        public ushort shortVal { get; set; }

        public AnotherDerivedThing()
        {
            this.Dict = new Dictionary<string, string>();
            this.Name = string.Empty;
            this.IntVal = 10000;
            this.doubleVal = default(double);
            shortVal = 0;
        }
    }

}

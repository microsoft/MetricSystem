// ---------------------------------------------------------------------
// <copyright file="FileConfigurationSourceTests.cs" company="Microsoft">
//       Copyright 2014 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Configuration.UnitTests
{
    using System;
    using System.IO;
    using System.Text;
    using System.Threading;

    using Newtonsoft.Json.Linq;

    using NUnit.Framework;

    [TestFixture]
    public sealed class FileConfigurationSourceTests
    {
        [SetUp]
        public void Setup()
        {
            this.fullStoragePath = Path.Combine(Directory.GetCurrentDirectory(), StoragePath);
            this.fullSampleFilename = Path.Combine(this.fullStoragePath, SampleFilename);
            if (Directory.Exists(this.fullStoragePath))
            {
                Directory.Delete(this.fullStoragePath, true);
            }
            Directory.CreateDirectory(this.fullStoragePath);
        }

        private void CreateDefaultFile()
        {
            using (var fs = new FileStream(this.fullSampleFilename, FileMode.Create, FileAccess.ReadWrite))
            {
                var content = Encoding.UTF8.GetBytes(ConfigurationSource.EmptyContent);
                fs.Write(content, 0, content.Length);
                fs.Close();
            }
        }

        private const string StoragePath = "TestFiles";

        private const string SampleFilename = "SomeStuffInAFile.json";
        private string fullStoragePath;
        private string fullSampleFilename;

        [Test]
        public void CanMonitorFileWhichDoesNotExist()
        {
            using (var fileSource = new FileConfigurationSource(this.fullSampleFilename))
            {
                fileSource.SourceUpdated += () => Assert.Fail("Should not be called.");
            }
        }

        [Category("QTestSkip")]
        [Test]
        public void CreatingFileCausesUpdate()
        {
            using (var fileSource = new FileConfigurationSource(this.fullSampleFilename))
            using (var updatedEvent = new ManualResetEventSlim(false))
            {
                var updated = false;
                fileSource.SourceUpdated +=
                    () =>
                    {
                        updated = true;
                        updatedEvent.Set();
                    };
                this.CreateDefaultFile();

                // updates are async so give it some time
                updatedEvent.Wait(TimeSpan.FromSeconds(60));
                Assert.IsTrue(updated, "File update did not occur.");
            }
        }

        [Test]
        public void ReadingFileWhichDoesNotExistReturnsEmptyObject()
        {
            using (var fileSource = new FileConfigurationSource(this.fullSampleFilename))
            {
                var obj = fileSource.Read();
                Assert.IsTrue(JToken.DeepEquals(obj, new JObject()));
            }
        }

        [Category("QTestSkip")]
        [Test]
        public void UpdatingExistingFileCausesUpdate()
        {
            this.CreateDefaultFile();
            using (var updated = new ManualResetEventSlim(false))
            using (var fileSource = new FileConfigurationSource(this.fullSampleFilename))
            {
                fileSource.SourceUpdated += updated.Set;
                using (var fs = new FileStream(this.fullSampleFilename, FileMode.Open, FileAccess.ReadWrite))
                {
                    fs.Seek(0, SeekOrigin.Begin);
                    var content = Encoding.UTF8.GetBytes(@"{""jenny"":8675309}");
                    fs.Write(content, 0, content.Length);
                    fs.Close();
                }

                Assert.IsTrue(updated.Wait(TimeSpan.FromSeconds(60)));
            }
        }
    }
}

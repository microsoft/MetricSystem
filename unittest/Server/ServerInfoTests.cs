// ---------------------------------------------------------------------
// <copyright file="ServerInfoTests.cs" company="Microsoft">
//       Copyright 2015 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Server.UnitTests
{
    using System;

    using NUnit.Framework;

    [TestFixture]
    public class ServerInfoTests
    {
        [Test]
        public void ConstructorThrowsErrorIfHostNameIsNull()
        {
            Assert.Throws<ArgumentException>(() => new ServerInfo(new ServerRegistration
                                                                  {
                                                                      Hostname = null,
                                                                      Port = 42
                                                                  }));
        }

        [Test]
        public void ConstructorThrowsErrorIfHostNameIsEmpty()
        {
            Assert.Throws<ArgumentException>(
                                             () =>
                                             new ServerInfo(new ServerRegistration
                                                            {
                                                                Hostname = string.Empty,
                                                                Port = 42
                                                            }));
        }

        [Test]
        public void LastUpdateTimeCannotBeSetBackwards()
        {
            var serverInfo = new ServerInfo(new ServerRegistration {Hostname = "foo", Port = 4200});

            var timeToSet = DateTimeOffset.Now;
            serverInfo.LastUpdateTime = timeToSet;
            Assert.AreEqual(timeToSet, serverInfo.LastUpdateTime);

            serverInfo.LastUpdateTime = timeToSet.Subtract(TimeSpan.FromSeconds(1));
            Assert.AreEqual(timeToSet, serverInfo.LastUpdateTime);
        }
    }
}

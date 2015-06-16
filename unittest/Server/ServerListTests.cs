// ---------------------------------------------------------------------
// <copyright file="ServerListTests.cs" company="Microsoft">
//       Copyright 2015 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Server.UnitTests
{
    using System;
    using System.Collections.Generic;

    using NUnit.Framework;

    [TestFixture]
    public class MetricSystemServerListTests
    {
        [Test]
        public void InsertIntoTableThrowsExceptionIfHostnameIsNull()
        {
            Assert.Throws<ArgumentNullException>(() =>
                                                 {
                                                     var list = new ServerList();
                                                     list.InsertOrUpdate(new ServerRegistration
                                                                         {
                                                                             Hostname = null,
                                                                             Port = 4200
                                                                         });
                                                 });
        }

        [Test]
        public void InsertIntoTableThrowsExceptionIfHostNameIsEmpty()
        {
            Assert.Throws<ArgumentException>(() =>
                                             {
                                                 var list = new ServerList();
                                                 list.InsertOrUpdate(new ServerRegistration
                                                                     {
                                                                         Hostname = string.Empty,
                                                                         Port = 4200
                                                                     });
                                             });
        }

        [Test]
        public void InsertIntoTableInsertsIntoTableIfKeyNotFound()
        {
            var list = new ServerList();
            const string hostname = "somehostname";

            Assert.Throws<KeyNotFoundException>(() => { var server = list[hostname]; });
            list.InsertOrUpdate(new ServerRegistration {Hostname = hostname, Port = 4200});
            Assert.IsNotNull(list[hostname]);
        }

        [Test]
        public void ExpireServersExpiresIfServerHasNotBeenUpdated()
        {
            var list = new ServerList();
            const string hostname = "somehostname";
            list.InsertOrUpdate(new ServerRegistration {Hostname = hostname, Port = 4200});
            var server = list[hostname];
            var updateTime = server.LastUpdateTime;
            list.ExpireServers(updateTime);
            Assert.IsNotNull(list[hostname]);

            list.ExpireServers(updateTime + TimeSpan.FromMinutes(11));
            Assert.Throws<KeyNotFoundException>(() => { var notThere = list[hostname]; });
        }
    }
}

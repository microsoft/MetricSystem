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

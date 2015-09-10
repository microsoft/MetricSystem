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

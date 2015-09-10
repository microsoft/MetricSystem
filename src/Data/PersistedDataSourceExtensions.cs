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

namespace MetricSystem.Data
{
    using System;

    internal static class PersistedDataSourceExtensions
    {
        public static PersistedDataSourceStatus Combine(this PersistedDataSourceStatus left,
                                                        PersistedDataSourceStatus right)
        {
            if (left == right)
            {
                return left;
            }

            switch (left)
            {
                // If the left source was unavailable then the right source trumps this. It can either be
                // Unknown, Available, or Partial.
            case PersistedDataSourceStatus.Unavailable:
                return right;
                // Unknown trumps unavailable, and turns Available to partial
            case PersistedDataSourceStatus.Unknown:
                switch (right)
                {
                case PersistedDataSourceStatus.Unavailable:
                    return left;
                case PersistedDataSourceStatus.Available:
                    return PersistedDataSourceStatus.Partial;
                case PersistedDataSourceStatus.Partial:
                    return right;
                }

                break; // Fall-through to logic fault exception.
                // Available trumps Unavailable, and turns Unknown to partial.
            case PersistedDataSourceStatus.Available:
                switch (right)
                {
                case PersistedDataSourceStatus.Unavailable:
                    return left;
                case PersistedDataSourceStatus.Unknown:
                    return PersistedDataSourceStatus.Partial;
                case PersistedDataSourceStatus.Partial:
                    return right;
                }

                break; // Fall-through to logic fault exception.
                // Partial data is always partial, no matter what the right source is.
            case PersistedDataSourceStatus.Partial:
                return left;
            }

            throw new InvalidOperationException("Somebody is bad at switch statements.");
        }
    }
}

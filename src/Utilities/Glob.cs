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

namespace MetricSystem.Utilities
{
    using System;

    public static class Glob
    {
        /// <summary>
        /// Performs basic glob-style pattern matching on a string. ? = 1 char, * = 0 or more.
        /// Note: Compound unicode symbols are not explicitly handled here. Character comparisons use invariants
        /// (not the current culture).
        /// </summary>
        /// <param name="source">String to match against.</param>
        /// <param name="pattern">Pattern to match with.</param>
        /// <param name="ignoreCase">Whether case should be honored (default is true).</param>
        /// <returns>True if the pattern matches the string.</returns>
        public static bool MatchGlob(this string source, string pattern, bool ignoreCase = true)
        {
            if (pattern == null)
            {
                throw new ArgumentNullException("pattern");
            }

            return source != null && source.MatchGlobInternal(pattern, 0, 0, ignoreCase);
        }

        private static bool MatchGlobInternal(this string source, string pattern, int sourceIndex, int patternIndex,
                                              bool ignoreCase)
        {
            var p = patternIndex;
            var s = sourceIndex;
            while (p < pattern.Length)
            {
                switch (pattern[p])
                {
                case '\\':
                    ++p;
                    if (p == pattern.Length || s == source.Length || !MatchChars(pattern[p], source[s], ignoreCase))
                    {
                        return false; // invalid pattern or failed match
                    }

                    ++p;
                    ++s;
                    continue;
                case '*':
                    return p == pattern.Length - 1 || source.MatchGlobInternal(pattern, s, p + 1, ignoreCase);
                case '?':
                    if (s < source.Length)
                    {
                        ++p;
                        ++s;
                        continue;
                    }
                    return false;
                default:
                    if (s == source.Length || !MatchChars(pattern[p], source[s], ignoreCase))
                    {
                        return false;
                    }

                    ++p;
                    ++s;
                    continue;
                }
            }

            return s == source.Length;
        }

        private static bool MatchChars(char left, char right, bool ignoreCase)
        {
            return ignoreCase
                       ? char.ToLowerInvariant(left) == char.ToLowerInvariant(right)
                       : left == right;
        }
    }
}

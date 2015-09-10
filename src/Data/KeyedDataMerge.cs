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
    using System.Collections.Generic;

    internal interface IMergeableData
    {
        /// <summary>
        /// Clears any existing data.
        /// </summary>
        void Clear();

        /// <summary>
        /// Merges data from a separate source.
        /// </summary>
        /// <param name="other">Separate source to merge from.</param>
        void MergeFrom(IMergeSource other);
    }

    internal static class KeyedDataMerge<TMergeable>
        where TMergeable : IMergeableData, new()
    {
        /// <summary>
        /// Merges a collection of already-sorted pairs of keys/data into an enumeration of keys and a collection of
        /// their associated data.
        /// </summary>
        /// <param name="sources">Sources to merge from.</param>
        /// <returns>An ordered enumeration of keys paired with a collection of associated data.</returns>
        public static IEnumerable<KeyValuePair<Key, TMergeable>> MergeSorted(
            ICollection<IEnumerable<KeyValuePair<Key, IMergeSource>>> sources)
        {
            var enumerators = new IEnumerator<KeyValuePair<Key, IMergeSource>>[sources.Count];
            var i = 0;
            foreach (var source in sources)
            {
                var enumerator = source.GetEnumerator();
                if (enumerator.MoveNext())
                {
                    enumerators[i] = enumerator;
                }
                else
                {
                    enumerator.Dispose();
                    enumerators[i] = null;
                }
                ++i;
            }

            var mergedData = new TMergeable();
            Key keyData = null;
            while (true)
            {
                bool haveData = false;
                for (i = 0; i < enumerators.Length; ++i)
                {
                    if (enumerators[i] == null)
                    {
                        continue;
                    }

                    if (!haveData || keyData > enumerators[i].Current.Key)
                    {
                        if (keyData == null)
                        {
                            keyData = new Key(new uint[enumerators[i].Current.Key.Length]);
                        }
                        enumerators[i].Current.Key.CopyTo(keyData);

                        haveData = true;
                    }
                }

                if (!haveData)
                {
                    break;
                }

                mergedData.Clear();
                for (i = 0; i < enumerators.Length; ++i)
                {
                    while (enumerators[i] != null && enumerators[i].Current.Key == keyData)
                    {
                        mergedData.MergeFrom(enumerators[i].Current.Value);
                        if (!enumerators[i].MoveNext())
                        {
                            enumerators[i].Dispose();
                            enumerators[i] = null;
                        }
                    }
                }

                yield return new KeyValuePair<Key, TMergeable>(keyData, mergedData);
            }
        }
    }
}

// ---------------------------------------------------------------------
// <copyright file="KeyedDataMerge.cs" company="Microsoft">
//       Copyright 2014 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

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

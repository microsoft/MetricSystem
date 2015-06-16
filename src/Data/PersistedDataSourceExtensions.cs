// ---------------------------------------------------------------------
// <copyright file="PersistedDataSourceExtensions.cs" company="Microsoft">
//       Copyright 2014 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

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

// ---------------------------------------------------------------------
// <copyright file="MaybeIntern.cs" company="Microsoft">
//       Copyright 2015 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

// Interning strings saves on memory, and dimension values tend to have heavy duplication. Interning has other
// associated costs however (e.g. lookups to the intern pool) and so may or may not be valuable. This behavior is here
// to allow for testing. If this value remains unchanged for a suitable duration the code can be simplified by picking
// the desired version. -chip
#undef INTERN_STRINGS

namespace MetricSystem.Data
{

    internal static class MaybeInternExtensionMethods
    {
        public static string MaybeIntern(this string value)
        {
#if INTERN_STRINGS
            value = string.Intern(value);
#endif
            return value;
        }
    }
}

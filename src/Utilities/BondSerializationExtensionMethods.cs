// ---------------------------------------------------------------------
// <copyright file="BondSerializationExtensionMethods.cs" company="Microsoft">
//       Copyright 2015 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Utilities
{
    using System.IO;

    using Bond;
    using Bond.IO;
    using Bond.Protocols;

    /// <summary>
    /// Helper methods for serializing and deserializing bond data.
    /// </summary>
    public static class BondSerializationExtensionMethods
    {
        /// <summary>
        /// Serializes an object to a SimpleBinaryWriter stream.
        /// </summary>
        /// <typeparam name="T">Object type to serialize.</typeparam>
        /// <typeparam name="O">Destination stream type.</typeparam>
        /// <param name="writer">Writer to emit to.</param>
        /// <param name="obj">Object to write.</param>
        public static void Write<T, O>(this SimpleBinaryWriter<O> writer, T obj) where O : IOutputStream
        {
            Serialize.To(writer, obj);
        }

        /// <summary>
        /// Serializes an object to a CompactBinaryWriter stream.
        /// </summary>
        /// <typeparam name="T">Object type to serialize.</typeparam>
        /// <typeparam name="O">Destination stream type.</typeparam>
        /// <param name="writer">Writer to emit to.</param>
        /// <param name="obj">Object to write.</param>
        public static void Write<T, O>(this CompactBinaryWriter<O> writer, T obj) where O : IOutputStream
        {
            Serialize.To(writer, obj);
        }

        /// <summary>
        /// Serializes an object to a SimpleJsonWriter stream.
        /// </summary>
        /// <typeparam name="T">Object type to serialize.</typeparam>
        /// <typeparam name="O">Destination stream type.</typeparam>
        /// <param name="writer">Writer to emit to.</param>
        /// <param name="obj">Object to write.</param>
        public static void Write<T>(this SimpleJsonWriter writer, T obj)
        {
            Serialize.To(writer, obj);
            writer.Flush();
        }

        /// <summary>
        /// Deserializes and object from an untagged protocol reader (e.g. SimpleBinaryReader)
        /// </summary>
        /// <typeparam name="T">Type of the object to read.</typeparam>
        /// <param name="reader">Source reader to deserialize from.</param>
        /// <returns>The object read from the stream.</returns>
        public static T Read<T>(this IUntaggedProtocolReader reader)
        {
            return Deserialize<T>.From(reader);
        }

        /// <summary>
        /// Deserializes and object from a tagged protocol reader (e.g. CompactBinaryReader)
        /// </summary>
        /// <typeparam name="T">Type of the object to read.</typeparam>
        /// <param name="reader">Source reader to deserialize from.</param>
        /// <returns>The object read from the stream.</returns>
        public static T Read<T>(this ITaggedProtocolReader reader)
        {
            return Deserialize<T>.From(reader);
        }

        /// <summary>
        /// Attempt to deserialize an object contained within a Bonded field as its derived type.
        /// </summary>
        /// <typeparam name="T">The base type of the Bonded field.</typeparam>
        /// <typeparam name="U">The desired derived type of the field.</typeparam>
        /// <param name="bonded">The Bonded source for the data.</param>
        /// <returns>Non-null object if the data was successfully desieralized, null otherwise.</returns>
        public static U TryDeserialize<T, U>(this IBonded<T> bonded)
            where U : class
        {
            try
            {
                return bonded.Deserialize<U>();
            }
            catch (InvalidDataException)
            {
                return null;
            }
        }
    }
}

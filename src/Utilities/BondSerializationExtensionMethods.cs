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

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

namespace MetricSystem.Utilities.LZ4
{
    internal interface ILZ4CodecProvider
    {
        /// <summary>
        /// Compress data.
        /// </summary>
        /// <param name="inputBuffer">Input buffer to encode.</param>
        /// <param name="inputLength">Length of the input buffer.</param>
        /// <param name="outputBuffer">Output buffer to emit encoded data into.</param>
        /// <param name="outputLength">Length of the output buffer.</param>
        /// <param name="useHighCompression">Whether to use normal or high compression/</param>
        /// <returns>Length of data recorded into the output buffer.</returns>
        unsafe int Compress(byte* inputBuffer, int inputLength, byte* outputBuffer, int outputLength,
                            bool useHighCompression);

        /// <summary>
        /// Decompress data.
        /// </summary>
        /// <param name="inputBuffer">Input buffer to decode.</param>
        /// <param name="inputLength">Length of the input buffer.</param>
        /// <param name="outputBuffer">Output buffer to emit decoded data into.</param>
        /// <param name="outputLength">Length of the output buffer.</param>
        /// <returns>Length of data recorded into the output buffer.</returns>
        unsafe int Decompress(byte* inputBuffer, int inputLength, byte* outputBuffer, int outputLength);
    }
}
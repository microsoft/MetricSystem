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

// Copyright (c) 2013, Milosz Krajewski
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without modification, are permitted provided 
// that the following conditions are met:
//
// * Redistributions of source code must retain the above copyright notice, this list of conditions 
//   and the following disclaimer.
//
// * Redistributions in binary form must reproduce the above copyright notice, this list of conditions 
//   and the following disclaimer in the documentation and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED 
// WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR 
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE 
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT 
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS 
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, 
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN 
// IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

//  LZ4 - Fast LZ compression algorithm
//  Copyright (C) 2011-2012, Yann Collet.
//  BSD 2-Clause License (http://www.opensource.org/licenses/bsd-license.php)
//
//  Redistribution and use in source and binary forms, with or without
//  modification, are permitted provided that the following conditions are
//  met:
//
//      * Redistributions of source code must retain the above copyright
//  notice, this list of conditions and the following disclaimer.
//      * Redistributions in binary form must reproduce the above
//  copyright notice, this list of conditions and the following disclaimer
//  in the documentation and/or other materials provided with the
//  distribution.
//
//  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
//  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
//  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
//  A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
//  OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
//  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
//  LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
//  DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
//  THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
//  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
//  OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//
//  You can contact the author at :
//  - LZ4 homepage : http://fastcompression.blogspot.com/p/lz4.html
//  - LZ4 source repository : http://code.google.com/p/lz4/

namespace MetricSystem.Utilities.LZ4
{
    using System;

    internal unsafe class LZ4NetCodecProvider : ILZ4CodecProvider
    {
        public int Compress(byte* inputBuffer, int inputLength, byte* outputBuffer, int outputLength,
                          bool useHighCompression)
        {
            if (!useHighCompression)
            {
                if (inputLength < Limit64K)
                {
                    var hashTable = stackalloc ushort[Hash64KTableSize];
                    return LZ4_compress64kCtx(hashTable, inputBuffer, outputBuffer, inputLength, outputLength);
                }
                else
                {
                    var hashTable = stackalloc uint[HashTableSize];
                    return LZ4_compressCtx(hashTable, inputBuffer, outputBuffer, inputLength, outputLength);
                }
            }
            var length = LZ4_compressHC(inputBuffer, outputBuffer, inputLength, outputLength);
            return length <= 0 ? -1 : length;
        }

        public int Decompress(byte* inputBuffer, int inputLength, byte* outputBuffer, int outputLength)
        {
            var length = LZ4_uncompress(inputBuffer, outputBuffer, outputLength);
            if (length != inputLength)
            {
                throw new ArgumentException("LZ4 block is corrupted, or invalid length has been given.");
            }

            return outputLength;
        }

        private static void BufferCopy(byte* src, byte* dst, int len)
        {
            while (len > 7)
            {
                *(ulong*)dst = *(ulong*)src;
                dst += 8;
                src += 8;
                len -= 8;
            }
            if (len > 3)
            {
                *(uint*)dst = *(uint*)src;
                dst += 4;
                src += 4;
                len -= 4;
            }
            if (len > 1)
            {
                *(ushort*)dst = *(ushort*)src;
                dst += 2;
                src += 2;
                len -= 2;
            }
            if (len > 0)
            {
                *dst = *src;
            }
        }

        private static void BufferFill(byte* dst, int len, byte val)
        {
            if (len > 7)
            {
                ulong mask = val;
                mask |= mask << 8;
                mask |= mask << 16;
                mask |= mask << 32;
                do
                {
                    *(ulong*)dst = mask;
                    dst += 8;
                    len -= 8;
                } while (len > 7);
            }

            while (len-- > 0)
            {
                *dst++ = val;
            }
        }

        private static int LZ4_compressCtx(uint* hash_table, byte* src, byte* dst, int src_len, int dst_maxlen)
        {
            byte* _p;

            fixed (int* debruijn64 = &DebruijnTable[0])
            {
                // r93
                var src_p = src;
                var src_base = src_p;
                var src_anchor = src_p;
                var src_end = src_p + src_len;
                var src_mflimit = src_end - MFLimit;

                var dst_p = dst;
                var dst_end = dst_p + dst_maxlen;

                var src_LASTLITERALS = src_end - LastLiterals;
                var src_LASTLITERALS_1 = src_LASTLITERALS - 1;

                var src_LASTLITERALS_3 = src_LASTLITERALS - 3;
                var src_LASTLITERALS_STEPSIZE_1 = src_LASTLITERALS - (StepSize - 1);
                var dst_LASTLITERALS_1 = dst_end - (1 + LastLiterals);
                var dst_LASTLITERALS_3 = dst_end - (2 + 1 + LastLiterals);

                int length;
                uint h, h_fwd;

                // Init
                if (src_len < MinLength)
                {
                    goto _last_literals;
                }

                // First Byte
                hash_table[((((*(uint*)(src_p))) * 2654435761u) >> HashAdjust)] = (uint)(src_p - src_base);
                src_p++;
                h_fwd = ((((*(uint*)(src_p))) * 2654435761u) >> HashAdjust);

                // Main Loop
                while (true)
                {
                    var findMatchAttempts = (1 << SkipStrength) + 3;
                    var src_p_fwd = src_p;
                    byte* src_ref;
                    byte* dst_token;

                    // Find a match
                    do
                    {
                        h = h_fwd;
                        var step = findMatchAttempts++ >> SkipStrength;
                        src_p = src_p_fwd;
                        src_p_fwd = src_p + step;

                        if (src_p_fwd > src_mflimit)
                        {
                            goto _last_literals;
                        }

                        h_fwd = ((((*(uint*)(src_p_fwd))) * 2654435761u) >> HashAdjust);
                        src_ref = src_base + hash_table[h];
                        hash_table[h] = (uint)(src_p - src_base);
                    } while ((src_ref < src_p - MaxDistance) || ((*(uint*)(src_ref)) != (*(uint*)(src_p))));

                    // Catch up
                    while ((src_p > src_anchor) && (src_ref > src) && (src_p[-1] == src_ref[-1]))
                    {
                        src_p--;
                        src_ref--;
                    }

                    // Encode Literal length
                    length = (int)(src_p - src_anchor);
                    dst_token = dst_p++;

                    if (dst_p + length + (length >> 8) > dst_LASTLITERALS_3)
                    {
                        return 0; // Check output limit
                    }

                    if (length >= RunMask)
                    {
                        var len = length - RunMask;
                        *dst_token = (RunMask << MLBits);
                        if (len > 254)
                        {
                            do
                            {
                                *dst_p++ = 255;
                                len -= 255;
                            } while (len > 254);
                            *dst_p++ = (byte)len;
                            BufferCopy(src_anchor, dst_p, (length));
                            dst_p += length;
                            goto _next_match;
                        }
                        *dst_p++ = (byte)len;
                    }
                    else
                    {
                        *dst_token = (byte)(length << MLBits);
                    }

                    // Copy Literals
                    _p = dst_p + (length);
                    {
                        do
                        {
                            *(ulong*)dst_p = *(ulong*)src_anchor;
                            dst_p += 8;
                            src_anchor += 8;
                        } while (dst_p < _p);
                    }
                    dst_p = _p;

                    _next_match:

                    // Encode Offset
                    *(ushort*)dst_p = (ushort)(src_p - src_ref);
                    dst_p += 2;

                    // Start Counting
                    src_p += MinMatch;
                    src_ref += MinMatch; // MinMatch already verified
                    src_anchor = src_p;

                    while (src_p < src_LASTLITERALS_STEPSIZE_1)
                    {
                        var diff = (*(long*)(src_ref)) ^ (*(long*)(src_p));
                        if (diff == 0)
                        {
                            src_p += StepSize;
                            src_ref += StepSize;
                            continue;
                        }
                        src_p += debruijn64[(((ulong)((diff) & -(diff)) * 0x0218A392CDABBD3FL)) >> 58];
                        goto _endCount;
                    }

                    if ((src_p < src_LASTLITERALS_3) && ((*(uint*)(src_ref)) == (*(uint*)(src_p))))
                    {
                        src_p += 4;
                        src_ref += 4;
                    }
                    if ((src_p < src_LASTLITERALS_1) && ((*(ushort*)(src_ref)) == (*(ushort*)(src_p))))
                    {
                        src_p += 2;
                        src_ref += 2;
                    }
                    if ((src_p < src_LASTLITERALS) && (*src_ref == *src_p))
                    {
                        src_p++;
                    }

                    _endCount:

                    // Encode MatchLength
                    length = (int)(src_p - src_anchor);

                    if (dst_p + (length >> 8) > dst_LASTLITERALS_1)
                    {
                        return 0; // Check output limit
                    }

                    if (length >= MLMask)
                    {
                        *dst_token += MLMask;
                        length -= MLMask;
                        for (; length > 509; length -= 510)
                        {
                            *dst_p++ = 255;
                            *dst_p++ = 255;
                        }
                        if (length > 254)
                        {
                            length -= 255;
                            *dst_p++ = 255;
                        }
                        *dst_p++ = (byte)length;
                    }
                    else
                    {
                        *dst_token += (byte)length;
                    }

                    // Test end of chunk
                    if (src_p > src_mflimit)
                    {
                        src_anchor = src_p;
                        break;
                    }

                    // Fill table
                    hash_table[((((*(uint*)(src_p - 2))) * 2654435761u) >> HashAdjust)] = (uint)(src_p - 2 - src_base);

                    // Test next position

                    h = ((((*(uint*)(src_p))) * 2654435761u) >> HashAdjust);
                    src_ref = src_base + hash_table[h];
                    hash_table[h] = (uint)(src_p - src_base);

                    if ((src_ref > src_p - (MaxDistance + 1)) && ((*(uint*)(src_ref)) == (*(uint*)(src_p))))
                    {
                        dst_token = dst_p++;
                        *dst_token = 0;
                        goto _next_match;
                    }

                    // Prepare next loop
                    src_anchor = src_p++;
                    h_fwd = ((((*(uint*)(src_p))) * 2654435761u) >> HashAdjust);
                }

                _last_literals:

                // Encode Last Literals
                var lastRun = (int)(src_end - src_anchor);
                if (dst_p + lastRun + 1 + ((lastRun + 255 - RunMask) / 255) > dst_end)
                {
                    return 0;
                }
                if (lastRun >= RunMask)
                {
                    *dst_p++ = (RunMask << MLBits);
                    lastRun -= RunMask;
                    for (; lastRun > 254; lastRun -= 255)
                    {
                        *dst_p++ = 255;
                    }
                    *dst_p++ = (byte)lastRun;
                }
                else
                {
                    *dst_p++ = (byte)(lastRun << MLBits);
                }
                BufferCopy(src_anchor, dst_p, (int)(src_end - src_anchor));
                dst_p += src_end - src_anchor;

                // End
                return (int)(dst_p - dst);
            }
        }

        private static int LZ4_compress64kCtx(
            ushort* hash_table,
            byte* src,
            byte* dst,
            long src_len,
            long dst_maxlen)
        {
            byte* _p;

            fixed (int* debruijn64 = &DebruijnTable[0])
            {
                // r93
                var src_p = src;
                var src_anchor = src_p;
                var src_base = src_p;
                var src_end = src_p + src_len;
                var src_mflimit = src_end - MFLimit;

                var dst_p = dst;
                var dst_end = dst_p + dst_maxlen;

                var src_LASTLITERALS = src_end - LastLiterals;
                var src_LASTLITERALS_1 = src_LASTLITERALS - 1;

                var src_LASTLITERALS_3 = src_LASTLITERALS - 3;

                var src_LASTLITERALS_STEPSIZE_1 = src_LASTLITERALS - (StepSize - 1);
                var dst_LASTLITERALS_1 = dst_end - (1 + LastLiterals);
                var dst_LASTLITERALS_3 = dst_end - (2 + 1 + LastLiterals);

                int len, length;

                uint h, h_fwd;

                // Init
                if (src_len < MinLength)
                {
                    goto _last_literals;
                }

                // First Byte
                src_p++;
                h_fwd = ((((*(uint*)(src_p))) * 2654435761u) >> Hash64KAdjust);

                // Main Loop
                while (true)
                {
                    var findMatchAttempts = (1 << SkipStrength) + 3;
                    var src_p_fwd = src_p;
                    byte* src_ref;
                    byte* dst_token;

                    // Find a match
                    do
                    {
                        h = h_fwd;
                        var step = findMatchAttempts++ >> SkipStrength;
                        src_p = src_p_fwd;
                        src_p_fwd = src_p + step;

                        if (src_p_fwd > src_mflimit)
                        {
                            goto _last_literals;
                        }

                        h_fwd = ((((*(uint*)(src_p_fwd))) * 2654435761u) >> Hash64KAdjust);
                        src_ref = src_base + hash_table[h];
                        hash_table[h] = (ushort)(src_p - src_base);
                    } while ((*(uint*)(src_ref)) != (*(uint*)(src_p)));

                    // Catch up
                    while ((src_p > src_anchor) && (src_ref > src) && (src_p[-1] == src_ref[-1]))
                    {
                        src_p--;
                        src_ref--;
                    }

                    // Encode Literal length
                    length = (int)(src_p - src_anchor);
                    dst_token = dst_p++;

                    if (dst_p + length + (length >> 8) > dst_LASTLITERALS_3)
                    {
                        return 0; // Check output limit
                    }

                    if (length >= RunMask)
                    {
                        len = length - RunMask;
                        *dst_token = (RunMask << MLBits);
                        if (len > 254)
                        {
                            do
                            {
                                *dst_p++ = 255;
                                len -= 255;
                            } while (len > 254);
                            *dst_p++ = (byte)len;
                            BufferCopy(src_anchor, dst_p, (length));
                            dst_p += length;
                            goto _next_match;
                        }
                        *dst_p++ = (byte)len;
                    }
                    else
                    {
                        *dst_token = (byte)(length << MLBits);
                    }

                    // Copy Literals
                    {
                        _p = dst_p + (length);
                        {
                            do
                            {
                                *(ulong*)dst_p = *(ulong*)src_anchor;
                                dst_p += 8;
                                src_anchor += 8;
                            } while (dst_p < _p);
                        }
                        dst_p = _p;
                    }

                    _next_match:

                    // Encode Offset
                    *(ushort*)dst_p = (ushort)(src_p - src_ref);
                    dst_p += 2;

                    // Start Counting
                    src_p += MinMatch;
                    src_ref += MinMatch; // MinMatch verified
                    src_anchor = src_p;

                    while (src_p < src_LASTLITERALS_STEPSIZE_1)
                    {
                        var diff = (*(long*)(src_ref)) ^ (*(long*)(src_p));
                        if (diff == 0)
                        {
                            src_p += StepSize;
                            src_ref += StepSize;
                            continue;
                        }
                        src_p += debruijn64[(((ulong)((diff) & -(diff)) * 0x0218A392CDABBD3FL)) >> 58];
                        goto _endCount;
                    }

                    if ((src_p < src_LASTLITERALS_3) && ((*(uint*)(src_ref)) == (*(uint*)(src_p))))
                    {
                        src_p += 4;
                        src_ref += 4;
                    }
                    if ((src_p < src_LASTLITERALS_1) && ((*(ushort*)(src_ref)) == (*(ushort*)(src_p))))
                    {
                        src_p += 2;
                        src_ref += 2;
                    }
                    if ((src_p < src_LASTLITERALS) && (*src_ref == *src_p))
                    {
                        src_p++;
                    }

                    _endCount:

                    // Encode MatchLength
                    len = (int)(src_p - src_anchor);

                    if (dst_p + (len >> 8) > dst_LASTLITERALS_1)
                    {
                        return 0; // Check output limit
                    }

                    if (len >= MLMask)
                    {
                        *dst_token += MLMask;
                        len -= MLMask;
                        for (; len > 509; len -= 510)
                        {
                            *dst_p++ = 255;
                            *dst_p++ = 255;
                        }
                        if (len > 254)
                        {
                            len -= 255;
                            *dst_p++ = 255;
                        }
                        *dst_p++ = (byte)len;
                    }
                    else
                    {
                        *dst_token += (byte)len;
                    }

                    // Test end of chunk
                    if (src_p > src_mflimit)
                    {
                        src_anchor = src_p;
                        break;
                    }

                    // Fill table
                    hash_table[((((*(uint*)(src_p - 2))) * 2654435761u) >> Hash64KAdjust)] =
                        (ushort)(src_p - 2 - src_base);

                    // Test next position

                    h = ((((*(uint*)(src_p))) * 2654435761u) >> Hash64KAdjust);
                    src_ref = src_base + hash_table[h];
                    hash_table[h] = (ushort)(src_p - src_base);

                    if ((*(uint*)(src_ref)) == (*(uint*)(src_p)))
                    {
                        dst_token = dst_p++;
                        *dst_token = 0;
                        goto _next_match;
                    }

                    // Prepare next loop
                    src_anchor = src_p++;
                    h_fwd = ((((*(uint*)(src_p))) * 2654435761u) >> Hash64KAdjust);
                }

                _last_literals:

                // Encode Last Literals
                var lastRun = (int)(src_end - src_anchor);
                if (dst_p + lastRun + 1 + (lastRun - RunMask + 255) / 255 > dst_end)
                {
                    return 0;
                }
                if (lastRun >= RunMask)
                {
                    *dst_p++ = (RunMask << MLBits);
                    lastRun -= RunMask;
                    for (; lastRun > 254; lastRun -= 255)
                    {
                        *dst_p++ = 255;
                    }
                    *dst_p++ = (byte)lastRun;
                }
                else
                {
                    *dst_p++ = (byte)(lastRun << MLBits);
                }
                BufferCopy(src_anchor, dst_p, (int)(src_end - src_anchor));
                dst_p += src_end - src_anchor;

                // End
                return (int)(dst_p - dst);
            }
        }

        private static int LZ4_uncompress(
            byte* src,
            byte* dst,
            int dst_len)
        {
            fixed (int* dec32table = &DecoderTable32[0])
            {
                fixed (int* dec64table = &DecoderTable64[0])
                {
                    // r93
                    var src_p = src;
                    byte* dst_ref;

                    var dst_p = dst;
                    var dst_end = dst_p + dst_len;
                    byte* dst_cpy;

                    var dst_LASTLITERALS = dst_end - LastLiterals;
                    var dst_COPYLENGTH = dst_end - CopyLength;
                    var dst_COPYLENGTH_STEPSIZE_4 = dst_end - CopyLength - (StepSize - 4);

                    byte token;

                    // Main Loop
                    while (true)
                    {
                        int length;

                        // get runlength
                        token = *src_p++;
                        if ((length = (token >> MLBits)) == RunMask)
                        {
                            int len;
                            for (; (len = *src_p++) == 255; length += 255)
                            {
                                /* do nothing */
                            }
                            length += len;
                        }

                        // copy literals
                        dst_cpy = dst_p + length;

                        if (dst_cpy > dst_COPYLENGTH)
                        {
                            if (dst_cpy != dst_end)
                            {
                                goto _output_error; // Error : not enough place for another match (min 4) + 5 literals
                            }
                            BufferCopy(src_p, dst_p, (length));
                            src_p += length;
                            break; // EOF
                        }
                        do
                        {
                            *(ulong*)dst_p = *(ulong*)src_p;
                            dst_p += 8;
                            src_p += 8;
                        } while (dst_p < dst_cpy);
                        src_p -= (dst_p - dst_cpy);
                        dst_p = dst_cpy;

                        // get offset
                        dst_ref = (dst_cpy) - (*(ushort*)(src_p));
                        src_p += 2;
                        if (dst_ref < dst)
                        {
                            goto _output_error; // Error : offset outside destination buffer
                        }

                        // get matchlength
                        if ((length = (token & MLMask)) == MLMask)
                        {
                            for (; *src_p == 255; length += 255)
                            {
                                src_p++;
                            }
                            length += *src_p++;
                        }

                        // copy repeated sequence
                        if ((dst_p - dst_ref) < StepSize)
                        {
                            var dec64 = dec64table[dst_p - dst_ref];

                            dst_p[0] = dst_ref[0];
                            dst_p[1] = dst_ref[1];
                            dst_p[2] = dst_ref[2];
                            dst_p[3] = dst_ref[3];
                            dst_p += 4;
                            dst_ref += 4;
                            dst_ref -= dec32table[dst_p - dst_ref];
                            (*(uint*)(dst_p)) = (*(uint*)(dst_ref));
                            dst_p += StepSize - 4;
                            dst_ref -= dec64;
                        }
                        else
                        {
                            *(ulong*)dst_p = *(ulong*)dst_ref;
                            dst_p += 8;
                            dst_ref += 8;
                        }
                        dst_cpy = dst_p + length - (StepSize - 4);

                        if (dst_cpy > dst_COPYLENGTH_STEPSIZE_4)
                        {
                            if (dst_cpy > dst_LASTLITERALS)
                            {
                                goto _output_error; // Error : last 5 bytes must be literals
                            }
                            while (dst_p < dst_COPYLENGTH)
                            {
                                *(ulong*)dst_p = *(ulong*)dst_ref;
                                dst_p += 8;
                                dst_ref += 8;
                            }

                            while (dst_p < dst_cpy)
                            {
                                *dst_p++ = *dst_ref++;
                            }
                            dst_p = dst_cpy;
                            continue;
                        }

                        {
                            do
                            {
                                *(ulong*)dst_p = *(ulong*)dst_ref;
                                dst_p += 8;
                                dst_ref += 8;
                            } while (dst_p < dst_cpy);
                        }
                        dst_p = dst_cpy; // correction
                    }

                    // end of decoding
                    return (int)((src_p) - src);

                    // write overflow error detected
                    _output_error:
                    return (int)(-((src_p) - src));
                }
            }
        }

        private static LZ4HC_Data_Structure LZ4HC_Create(byte* src)
        {
            var hc4 = new LZ4HC_Data_Structure
                      {
                          hashTable = new int[HashHCTableSize],
                          chainTable = new ushort[MaxD]
                      };

            fixed (ushort* ct = &hc4.chainTable[0])
            {
                BufferFill((byte*)ct, MaxD * sizeof(ushort), 0xFF);
            }

            hc4.src_base = src;
            hc4.nextToUpdate = src + 1;

            return hc4;
        }

        private static int LZ4_compressHC(byte* input, byte* output, int inputLength, int outputLength)
        {
            return LZ4_compressHCCtx(LZ4HC_Create(input), input, output, inputLength, outputLength);
        }

        // Update chains up to ip (excluded)
        private static void LZ4HC_Insert(LZ4HC_Data_Structure hc4, byte* src_p)
        {
            fixed (ushort* chainTable = hc4.chainTable)
            {
                fixed (int* hashTable = hc4.hashTable)
                {
                    var src_base = hc4.src_base;
                    while (hc4.nextToUpdate < src_p)
                    {
                        var p = hc4.nextToUpdate;
                        var delta =
                            (int)((p) - (hashTable[((((*(uint*)(p))) * 2654435761u) >> HashHCAdjust)] + src_base));
                        if (delta > MaxDistance)
                        {
                            delta = MaxDistance;
                        }
                        chainTable[((int)p) & MaxDMask] = (ushort)delta;
                        hashTable[((((*(uint*)(p))) * 2654435761u) >> HashHCAdjust)] = (int)(p - src_base);
                        hc4.nextToUpdate++;
                    }
                }
            }
        }

        private static int LZ4HC_CommonLength(byte* p1, byte* p2, byte* src_LASTLITERALS)
        {
            fixed (int* debruijn64 = DebruijnTable)
            {
                var p1t = p1;

                while (p1t < src_LASTLITERALS - (StepSize - 1))
                {
                    var diff = (*(long*)(p2)) ^ (*(long*)(p1t));
                    if (diff == 0)
                    {
                        p1t += StepSize;
                        p2 += StepSize;
                        continue;
                    }
                    p1t += debruijn64[(((ulong)((diff) & -(diff)) * 0x0218A392CDABBD3FL)) >> 58];
                    return (int)(p1t - p1);
                }
                if ((p1t < (src_LASTLITERALS - 3)) && ((*(uint*)(p2)) == (*(uint*)(p1t))))
                {
                    p1t += 4;
                    p2 += 4;
                }
                if ((p1t < (src_LASTLITERALS - 1)) && ((*(ushort*)(p2)) == (*(ushort*)(p1t))))
                {
                    p1t += 2;
                    p2 += 2;
                }
                if ((p1t < src_LASTLITERALS) && (*p2 == *p1t))
                {
                    p1t++;
                }
                return (int)(p1t - p1);
            }
        }

        private static int LZ4HC_InsertAndFindBestMatch(
            LZ4HC_Data_Structure hc4, byte* src_p, byte* src_LASTLITERALS, ref byte* matchpos)
        {
            fixed (ushort* chainTable = hc4.chainTable)
            {
                fixed (int* hashTable = hc4.hashTable)
                {
                    var src_base = hc4.src_base;
                    var nbAttempts = MaxAttempts;
                    int repl = 0, ml = 0;
                    ushort delta = 0;

                    // HC4 match finder
                    LZ4HC_Insert(hc4, src_p);
                    var src_ref = (hashTable[((((*(uint*)(src_p))) * 2654435761u) >> HashHCAdjust)] + src_base);

                    // Detect repetitive sequences of length <= 4
                    if (src_ref >= src_p - 4) // potential repetition
                    {
                        if ((*(uint*)(src_ref)) == (*(uint*)(src_p))) // confirmed
                        {
                            delta = (ushort)(src_p - src_ref);
                            repl =
                                ml =
                                LZ4HC_CommonLength(src_p + MinMatch, src_ref + MinMatch, src_LASTLITERALS) + MinMatch;
                            matchpos = src_ref;
                        }
                        src_ref = ((src_ref) - chainTable[((int)src_ref) & MaxDMask]);
                    }

                    while ((src_ref >= src_p - MaxDistance) && (nbAttempts != 0))
                    {
                        nbAttempts--;
                        if (*(src_ref + ml) == *(src_p + ml))
                        {
                            if ((*(uint*)(src_ref)) == (*(uint*)(src_p)))
                            {
                                var mlt = LZ4HC_CommonLength(src_p + MinMatch, src_ref + MinMatch, src_LASTLITERALS) +
                                          MinMatch;
                                if (mlt > ml)
                                {
                                    ml = mlt;
                                    matchpos = src_ref;
                                }
                            }
                        }
                        src_ref = ((src_ref) - chainTable[((int)src_ref) & MaxDMask]);
                    }

                    // Complete table
                    if (repl != 0)
                    {
                        var ptr = src_p;

                        var end = src_p + repl - (MinMatch - 1);
                        while (ptr < end - delta)
                        {
                            chainTable[((int)ptr) & MaxDMask] = delta; // Pre-Load
                            ptr++;
                        }
                        do
                        {
                            chainTable[((int)ptr) & MaxDMask] = delta;
                            hashTable[((((*(uint*)(ptr))) * 2654435761u) >> HashHCAdjust)] = (int)(ptr - src_base);
                            // Head of chain
                            ptr++;
                        } while (ptr < end);
                        hc4.nextToUpdate = end;
                    }

                    return ml;
                }
            }
        }

        private static int LZ4HC_InsertAndGetWiderMatch(
            LZ4HC_Data_Structure hc4, byte* src_p, byte* startLimit, byte* src_LASTLITERALS, int longest,
            ref byte* matchpos, ref byte* startpos)
        {
            fixed (ushort* chainTable = hc4.chainTable)
            {
                fixed (int* hashTable = hc4.hashTable)
                {
                    fixed (int* debruijn64 = DebruijnTable)
                    {
                        var src_base = hc4.src_base;
                        var nbAttempts = MaxAttempts;
                        var delta = (int)(src_p - startLimit);

                        // First Match
                        LZ4HC_Insert(hc4, src_p);
                        var src_ref = (hashTable[((((*(uint*)(src_p))) * 2654435761u) >> HashHCAdjust)] + src_base);

                        while ((src_ref >= src_p - MaxDistance) && (nbAttempts != 0))
                        {
                            nbAttempts--;
                            if (*(startLimit + longest) == *(src_ref - delta + longest))
                            {
                                if ((*(uint*)(src_ref)) == (*(uint*)(src_p)))
                                {
                                    var reft = src_ref + MinMatch;
                                    var ipt = src_p + MinMatch;
                                    var startt = src_p;

                                    while (ipt < src_LASTLITERALS - (StepSize - 1))
                                    {
                                        var diff = (*(long*)(reft)) ^ (*(long*)(ipt));
                                        if (diff == 0)
                                        {
                                            ipt += StepSize;
                                            reft += StepSize;
                                            continue;
                                        }
                                        ipt += debruijn64[(((ulong)((diff) & -(diff)) * 0x0218A392CDABBD3FL)) >> 58];
                                        goto _endCount;
                                    }
                                    if ((ipt < (src_LASTLITERALS - 3)) && ((*(uint*)(reft)) == (*(uint*)(ipt))))
                                    {
                                        ipt += 4;
                                        reft += 4;
                                    }
                                    if ((ipt < (src_LASTLITERALS - 1)) && ((*(ushort*)(reft)) == (*(ushort*)(ipt))))
                                    {
                                        ipt += 2;
                                        reft += 2;
                                    }
                                    if ((ipt < src_LASTLITERALS) && (*reft == *ipt))
                                    {
                                        ipt++;
                                    }

                                    _endCount:
                                    reft = src_ref;

                                    while ((startt > startLimit) && (reft > hc4.src_base) && (startt[-1] == reft[-1]))
                                    {
                                        startt--;
                                        reft--;
                                    }

                                    if ((ipt - startt) > longest)
                                    {
                                        longest = (int)(ipt - startt);
                                        matchpos = reft;
                                        startpos = startt;
                                    }
                                }
                            }
                            src_ref = ((src_ref) - chainTable[((int)src_ref) & MaxDMask]);
                        }

                        return longest;
                    }
                }
            }
        }

        private static int LZ4_encodeSequence(
            ref byte* src_p, ref byte* dst_p, ref byte* src_anchor, int matchLength, byte* src_ref, byte* dst_end)
        {
            int len;
            var dst_token = (dst_p)++;

            // Encode Literal length
            var length = (int)(src_p - src_anchor);
            if ((dst_p + length + (2 + 1 + LastLiterals) + (length >> 8)) > dst_end)
            {
                return 1; // Check output limit
            }
            if (length >= RunMask)
            {
                *dst_token = (RunMask << MLBits);
                len = length - RunMask;
                for (; len > 254; len -= 255)
                {
                    *(dst_p)++ = 255;
                }
                *(dst_p)++ = (byte)len;
            }
            else
            {
                *dst_token = (byte)(length << MLBits);
            }

            // Copy Literals
            var _p = dst_p + (length);
            do
            {
                *(ulong*)dst_p = *(ulong*)src_anchor;
                dst_p += 8;
                src_anchor += 8;
            } while (dst_p < _p);
            dst_p = _p;

            // Encode Offset
            *(ushort*)dst_p = (ushort)(src_p - src_ref);
            dst_p += 2;

            // Encode MatchLength
            len = (matchLength - MinMatch);
            if (dst_p + (1 + LastLiterals) + (length >> 8) > dst_end)
            {
                return 1; // Check output limit
            }
            if (len >= MLMask)
            {
                *dst_token += MLMask;
                len -= MLMask;
                for (; len > 509; len -= 510)
                {
                    *(dst_p)++ = 255;
                    *(dst_p)++ = 255;
                }
                if (len > 254)
                {
                    len -= 255;
                    *(dst_p)++ = 255;
                }
                *(dst_p)++ = (byte)len;
            }
            else
            {
                *dst_token += (byte)len;
            }

            // Prepare next loop
            src_p += matchLength;
            src_anchor = src_p;

            return 0;
        }

        private static int LZ4_compressHCCtx(
            LZ4HC_Data_Structure ctx,
            byte* src,
            byte* dst,
            int src_len,
            int dst_maxlen)
        {
            var src_p = src;
            var src_anchor = src_p;
            var src_end = src_p + src_len;
            var src_mflimit = src_end - MFLimit;
            var src_LASTLITERALS = (src_end - LastLiterals);

            var dst_p = dst;
            var dst_end = dst_p + dst_maxlen;

            byte* src_ref = null;
            byte* start2 = null;
            byte* ref2 = null;
            byte* start3 = null;
            byte* ref3 = null;

            src_p++;

            // Main Loop
            while (src_p < src_mflimit)
            {
                var ml = LZ4HC_InsertAndFindBestMatch(ctx, src_p, src_LASTLITERALS, ref src_ref);
                if (ml == 0)
                {
                    src_p++;
                    continue;
                }

                // saved, in case we would skip too much
                var start0 = src_p;
                var ref0 = src_ref;
                var ml0 = ml;

                _Search2:
                var ml2 = src_p + ml < src_mflimit
                              ? LZ4HC_InsertAndGetWiderMatch(ctx, src_p + ml - 2, src_p + 1, src_LASTLITERALS, ml,
                                                             ref ref2, ref start2)
                              : ml;

                if (ml2 == ml) // No better match
                {
                    if (LZ4_encodeSequence(ref src_p, ref dst_p, ref src_anchor, ml, src_ref, dst_end) != 0)
                    {
                        return 0;
                    }
                    continue;
                }

                if (start0 < src_p && start2 < src_p + ml0)
                {
                    src_p = start0;
                    src_ref = ref0;
                    ml = ml0;
                }

                // Here, start0==ip
                if ((start2 - src_p) < 3) // First Match too small : removed
                {
                    ml = ml2;
                    src_p = start2;
                    src_ref = ref2;
                    goto _Search2;
                }

                _Search3:
                // Currently we have :
                // ml2 > ml1, and
                // ip1+3 <= ip2 (usually < ip1+ml1)
                if ((start2 - src_p) < OptimalML)
                {
                    var new_ml = ml;
                    if (new_ml > OptimalML)
                    {
                        new_ml = OptimalML;
                    }
                    if (src_p + new_ml > start2 + ml2 - MinMatch)
                    {
                        new_ml = (int)(start2 - src_p) + ml2 - MinMatch;
                    }
                    var correction = new_ml - (int)(start2 - src_p);
                    if (correction > 0)
                    {
                        start2 += correction;
                        ref2 += correction;
                        ml2 -= correction;
                    }
                }

                // Now, we have start2 = ip+new_ml, with new_ml=min(ml, OptimalML=18)

                var ml3 = start2 + ml2 < src_mflimit
                              ? LZ4HC_InsertAndGetWiderMatch(ctx, start2 + ml2 - 3, start2, src_LASTLITERALS, ml2,
                                                             ref ref3, ref start3)
                              : ml2;

                if (ml3 == ml2) // No better match : 2 sequences to encode
                {
                    // ip & ref are known; Now for ml
                    if (start2 < src_p + ml)
                    {
                        ml = (int)(start2 - src_p);
                    }

                    // Now, encode 2 sequences
                    if (LZ4_encodeSequence(ref src_p, ref dst_p, ref src_anchor, ml, src_ref, dst_end) != 0)
                    {
                        return 0;
                    }
                    src_p = start2;
                    if (LZ4_encodeSequence(ref src_p, ref dst_p, ref src_anchor, ml2, ref2, dst_end) != 0)
                    {
                        return 0;
                    }
                    continue;
                }

                if (start3 < src_p + ml + 3) // Not enough space for match 2 : remove it
                {
                    if (start3 >= src_p + ml) // can write Seq1 immediately ==> Seq2 is removed, so Seq3 becomes Seq1
                    {
                        if (start2 < src_p + ml)
                        {
                            var correction = (int)(src_p + ml - start2);
                            start2 += correction;
                            ref2 += correction;
                            ml2 -= correction;
                            if (ml2 < MinMatch)
                            {
                                start2 = start3;
                                ref2 = ref3;
                                ml2 = ml3;
                            }
                        }

                        if (LZ4_encodeSequence(ref src_p, ref dst_p, ref src_anchor, ml, src_ref, dst_end) != 0)
                        {
                            return 0;
                        }
                        src_p = start3;
                        src_ref = ref3;
                        ml = ml3;

                        start0 = start2;
                        ref0 = ref2;
                        ml0 = ml2;
                        goto _Search2;
                    }

                    start2 = start3;
                    ref2 = ref3;
                    ml2 = ml3;
                    goto _Search3;
                }

                // OK, now we have 3 ascending matches; let's write at least the first one
                // ip & ref are known; Now for ml
                if (start2 < src_p + ml)
                {
                    if (start2 - src_p < MLMask)
                    {
                        if (ml > OptimalML)
                        {
                            ml = OptimalML;
                        }
                        if (src_p + ml > start2 + ml2 - MinMatch)
                        {
                            ml = (int)(start2 - src_p) + ml2 - MinMatch;
                        }
                        var correction = ml - (int)(start2 - src_p);
                        if (correction > 0)
                        {
                            start2 += correction;
                            ref2 += correction;
                            ml2 -= correction;
                        }
                    }
                    else
                    {
                        ml = (int)(start2 - src_p);
                    }
                }

                if (LZ4_encodeSequence(ref src_p, ref dst_p, ref src_anchor, ml, src_ref, dst_end) != 0)
                {
                    return 0;
                }

                src_p = start2;
                src_ref = ref2;
                ml = ml2;

                start2 = start3;
                ref2 = ref3;
                ml2 = ml3;

                goto _Search3;
            }

            // Encode Last Literals
            var lastRun = (int)(src_end - src_anchor);
            if ((dst_p - dst) + lastRun + 1 + ((lastRun + 255 - RunMask) / 255) > (uint)dst_maxlen)
            {
                return 0; // Check output limit
            }
            if (lastRun >= RunMask)
            {
                *dst_p++ = (RunMask << MLBits);
                lastRun -= RunMask;
                for (; lastRun > 254; lastRun -= 255)
                {
                    *dst_p++ = 255;
                }
                *dst_p++ = (byte)lastRun;
            }
            else
            {
                *dst_p++ = (byte)(lastRun << MLBits);
            }
            BufferCopy(src_anchor, dst_p, (int)(src_end - src_anchor));
            dst_p += src_end - src_anchor;

            // End
            return (int)((dst_p) - dst);
        }

        private class LZ4HC_Data_Structure
        {
            public ushort[] chainTable;
            public int[] hashTable;
            public byte* nextToUpdate;
            public byte* src_base;
        };

        #region LZ4 algorithm constants
        // Power-of-two factor for memory usage. Modern Intel CPUs provide an L1 cache of 32KB data / 32KB instruction
        // so this fits within that.
        private const int MemoryUsage = 14;

        // Detection level of data which is not compressible. Higher values will increase the effort to compress, while
        // lower values will decrease the effort and may yield better performance on data which is not compressible.
        // The current value is as-recommended from the original implementation.
        private const int NotCompressibleDetectionlevel = 6;

        // The following are a raft of effectively undocumented constants from the original implementation. ENJOY!
        private const int MinMatch = 4;
        private const int SkipStrength = NotCompressibleDetectionlevel > 2 ? NotCompressibleDetectionlevel : 2;
        private const int CopyLength = 8;
        private const int LastLiterals = 5;
        private const int MFLimit = CopyLength + MinMatch;
        private const int MinLength = MFLimit + 1;
        private const int MaxDLog = 16;
        private const int MaxD = 1 << MaxDLog;
        private const int MaxDMask = MaxD - 1;
        private const int MaxDistance = (1 << MaxDLog) - 1;
        private const int MLBits = 4;
        private const int MLMask = (1 << MLBits) - 1;
        private const int RunBits = 8 - MLBits;
        private const int RunMask = (1 << RunBits) - 1;
        private const int StepSize = 8;

        private const int Limit64K = (1 << 16) + (MFLimit - 1);

        private const int HashLog = MemoryUsage - 2;
        private const int HashTableSize = 1 << HashLog;
        private const int HashAdjust = (MinMatch * 8) - HashLog;

        private const int Hash64KLog = HashLog + 1;
        private const int Hash64KTableSize = 1 << Hash64KLog;
        private const int Hash64KAdjust = (MinMatch * 8) - Hash64KLog;

        private const int HashHCLog = MaxDLog - 1;
        private const int HashHCTableSize = 1 << HashHCLog;
        private const int HashHCAdjust = (MinMatch * 8) - HashHCLog;

        private static readonly int[] DecoderTable32 = {0, 3, 2, 3, 0, 0, 0, 0};
        private static readonly int[] DecoderTable64 = {0, 0, 0, -1, 0, 1, 2, 3};

        private static readonly int[] DebruijnTable =
        {
            0, 0, 0, 0, 0, 1, 1, 2, 0, 3, 1, 3, 1, 4, 2, 7,
            0, 2, 3, 6, 1, 5, 3, 5, 1, 3, 4, 4, 2, 5, 6, 7,
            7, 0, 1, 2, 3, 3, 4, 6, 2, 6, 5, 5, 3, 4, 5, 6,
            7, 1, 2, 4, 6, 4, 4, 5, 7, 2, 6, 5, 7, 6, 7, 7
        };

        private const int MaxAttempts = 256;
        private const int OptimalML = (MLMask - 1) + MinMatch;
        #endregion
    }
}
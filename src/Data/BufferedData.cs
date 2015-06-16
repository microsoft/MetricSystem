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

#undef COLLECT_STACKS

// Turning this on may help catch resource leaks quickly.
#undef THROW_ON_FINALIZE

namespace MetricSystem.Data
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using System.Reflection;
    using System.Reflection.Emit;
    using System.Runtime.InteropServices;

    /// <summary>
    /// Class which supports reading a provided integer value type from byte buffers.
    /// </summary>
    /// <typeparam name="TValue">The type of value to generate read/write methods for (must be 2/4/8 byte integral).</typeparam>
    /// <remarks>
    /// This base class generates two dynamic methods to work around limitations in C# generics around being able to
    /// retrieve pointers to generic parameters in order to very rapidly read or write those values.
    /// </remarks>
    internal abstract unsafe class BufferedData<TValue> : IDisposable
    {
        #region dynamic method generator 
        protected static readonly int FixedLengthValueSize = GetFixedLengthValueSize();
        protected static readonly BufferValueWriter WriteFixedLengthValueToBuffer = CreateFixedSizeBufferValueWriter();
        protected static readonly BufferValueReader ReadFixedLengthValueFromBuffer = CreateFixedSizeBufferValueReader();

        private static int GetFixedLengthValueSize()
        {
            var valueType = typeof(TValue);
            if (valueType == typeof(short) || valueType == typeof(ushort))
            {
                return 2;
            }
            if (valueType == typeof(int) || valueType == typeof(uint))
            {
                return 4;
            }
            if (valueType == typeof(long) || valueType == typeof(ulong))
            {
                return 8;
            }

            throw new NotSupportedException("Unsupported value type.");
        }

        private static BufferValueWriter CreateFixedSizeBufferValueWriter()
        {
            var writer = new DynamicMethod("BufferValueWriter", MethodAttributes.Static | MethodAttributes.Public,
                                           CallingConventions.Standard, typeof(void),
                                           new[] {typeof(TValue), typeof(byte*)},
                                           typeof(BufferedData<TValue>), false);
            var writerIL = writer.GetILGenerator();
            writerIL.Emit(OpCodes.Ldarg_1);
            writerIL.Emit(OpCodes.Ldarg_0);

            switch (GetFixedLengthValueSize())
            {
            case 2:
                writerIL.Emit(OpCodes.Stind_I2);
                break;
            case 4:
                writerIL.Emit(OpCodes.Stind_I4);
                break;
            case 8:
                writerIL.Emit(OpCodes.Stind_I8);
                break;
            default:
                throw new NotSupportedException("Unsupported value type.");
            }

            writerIL.Emit(OpCodes.Ret);

            return (BufferValueWriter)writer.CreateDelegate(typeof(BufferValueWriter));
        }

        private static BufferValueReader CreateFixedSizeBufferValueReader()
        {
            var reader = new DynamicMethod("BufferValueReader", MethodAttributes.Static | MethodAttributes.Public,
                                           CallingConventions.Standard, typeof(long), new[] {typeof(byte*)},
                                           typeof(BufferedData<TValue>), false);
            var readerIL = reader.GetILGenerator();
            readerIL.Emit(OpCodes.Ldarg_0);

            // all values are read out as longs. Anything shorter needs Conv.I8 applied
            switch (GetFixedLengthValueSize())
            {
            case 2:
                readerIL.Emit(OpCodes.Ldind_I2);
                readerIL.Emit(OpCodes.Conv_I8);
                break;
            case 4:
                readerIL.Emit(OpCodes.Ldind_I4);
                readerIL.Emit(OpCodes.Conv_I8);
                break;
            case 8:
                readerIL.Emit(OpCodes.Ldind_I8);
                break;
            default:
                throw new NotSupportedException("Unsupported value type.");
            }

            readerIL.Emit(OpCodes.Ret);

            return (BufferValueReader)reader.CreateDelegate(typeof(BufferValueReader));
        }

        protected delegate long BufferValueReader(byte* buffer);

        protected delegate void BufferValueWriter(TValue value, byte* buffer);
        #endregion

        protected readonly int BufferLength;
        protected readonly int InitialDataLength;
        private readonly string allocationStack;
        private readonly byte[] sourceBuffer;
        private readonly int sourceOffset;
        protected byte* DataBuffer;
        private GCHandle dataBufferHandle;
        private bool disposed;

        protected BufferedData(byte[] buffer, int startOffset, int length)
        {
            this.allocationStack =
#if COLLECT_STACKS
                Environment.StackTrace;
#else
                string.Empty;
#endif

            this.sourceBuffer = buffer;

            // null buffers are acceptable when creating empty objects.
            if (this.sourceBuffer != null)
            {
                this.dataBufferHandle = GCHandle.Alloc(this.sourceBuffer, GCHandleType.Pinned);
                this.sourceOffset = startOffset;
                this.InitialDataLength = length;

                this.DataBuffer = (byte*)this.dataBufferHandle.AddrOfPinnedObject() + this.sourceOffset;
                this.BufferLength = this.sourceBuffer.Length - this.sourceOffset;
                if (this.BufferLength <= 0)
                {
                    throw new ArgumentException("Invalid buffer length or offset provided.");
                }
            }
        }

        protected abstract int ValidDataLength { get; }

        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        [SuppressMessage("Microsoft.Usage", "CA1801:ReviewUnusedParameters", MessageId = "disposing",
            Justification = "All bout dat pattern, boss.")]
        private void Dispose(bool disposing)
        {
            this.disposed = true;
            this.FreeHandle();
        }

        private void FreeHandle()
        {
            if (this.dataBufferHandle.IsAllocated)
            {
                this.dataBufferHandle.Free();
            }

            this.DataBuffer = null;
        }

        public void Serialize(Stream destinationStream)
        {
            this.CheckDisposed();

            if (this.ValidDataLength > 0)
            {
                destinationStream.Write(this.sourceBuffer, this.sourceOffset, this.ValidDataLength);
            }
        }

        public bool ValidateOffset(long offset)
        {
            this.CheckDisposed();

            return offset < this.ValidDataLength;
        }

        [SuppressMessage("Microsoft.Design", "CA1065:DoNotRaiseExceptionsInUnexpectedLocations",
            Justification = "Intentional decision to catch and halt on finalization for this type.")]
        ~BufferedData()
        {
            Events.Write.ObjectFinalized(this.GetType().ToString(), this.allocationStack);
            this.Dispose(false);
#if THROW_ON_FINALIZE
            throw new InvalidOperationException(
                "Finalization of buffered data indicates a leak in the application. Allocation stack: " +
                this.allocationStack);
#endif
        }

        public override string ToString()
        {
            return this.sourceBuffer != null ? this.dataBufferHandle.AddrOfPinnedObject().ToString() : "(null)";
        }

        protected void CheckDisposed()
        {
            if (this.disposed)
            {
                throw new ObjectDisposedException(this.GetType().ToString());
            }
        }
    }
}

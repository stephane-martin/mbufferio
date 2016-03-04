# -*- coding: utf-8 -*-

# noinspection PyUnresolvedReferences
from libc.stdint cimport uint8_t, uintptr_t, int64_t, uint64_t, uint32_t
# noinspection PyUnresolvedReferences
from cpython.ref cimport PyObject
from cpython.mem cimport PyMem_Malloc, PyMem_Free
# noinspection PyUnresolvedReferences
from cpython.buffer cimport PyObject_CheckBuffer, PyObject_GetBuffer, PyBuffer_Release
from cpython.buffer cimport PyBUF_SIMPLE, PyBUF_WRITABLE, PyBUF_STRIDES, PyBUF_ND
from cpython.unicode cimport PyUnicode_AsUTF8String, PyUnicode_Check
from cpython.bytes cimport PyBytes_Check
# noinspection PyUnresolvedReferences
from cython.view cimport memoryview as cy_memoryview
from libc.string cimport memcpy


cdef class MBufferIO(object):
    """
    MBufferIO objects behave like streams of the io module. They are especially useful when you have to deal with
    some API that only accepts file-like objects, or bytes object, or buffers...

    A MBufferIO can be initialized with a 'src' object. In that case, the MBufferIO is just a reference to the
    'src' object and read/writes will be forwarded directly to 'src'.

    If the 'src' referenced object is too small for some write operation, MBufferIO will copy the 'src' to an
    internal buffer, and from that point 'src' and the MBufferIO will be completely separated.

    MBufferIO supports the new buffer protocol.

    MBufferIO objects are iterable (iteration returns the content line by line).

    MBufferIO are pickleable (just the MBufferIO content is pickled though, not the current reading position).

    The MBufferIO content can be accessed through slices.

    MBufferIO supports the usual stream methods (read, write, etc).


    Attributes
    ----------
    is_a_reference: bool
        True if the MBufferIO is reference to a 'src' object. False if the MBufferIO is an independant copy.

    """
    def __cinit__(self, object src=None, int64_t startpos=0, int64_t length=-1, bint copy=0):
        if startpos < 0:
            startpos = 0
        cdef int res = 0
        self.view_count = 0
        self.original_obj = src
        self.offset = 0
        self.closed = 0
        if src is None:
            # empty MBufferIO object
            self.src_view = NULL
            self.readonly = 0
            self.startpos = 0
            self.length = 0
            self.copy_buf_size = 4096
            self.copy_buf_pointer = <char*> PyMem_Malloc(self.copy_buf_size)
            if self.copy_buf_pointer == NULL:
                raise MemoryError("Could not allocate initial memory for the copy_buf")
            self.buf_pointer = self.copy_buf_pointer
            self.is_a_reference = 0
            return

        if not PyObject_CheckBuffer(src):
            src = bytes(src)
            copy = 1

        # build a MBufferIO from an existing buffer
        self.src_view = <Py_buffer*> PyMem_Malloc(sizeof(Py_buffer))
        if self.src_view == NULL:
            raise MemoryError("Could not allocate memory for the Py_buffer")
        if PyObject_GetBuffer(src, self.src_view, PyBUF_SIMPLE) == -1:
            PyMem_Free(self.src_view)
            raise RuntimeError("PyObject_GetBuffer failed")
        cdef int64_t original_length = self.src_view.len
        if startpos > original_length:
            startpos = original_length
        cdef int64_t max_orig_length = max(original_length - startpos, 0)
        self.length = max_orig_length if length < 0 else min(max_orig_length, length)
        if copy:
            # copy the original object (self.length bytes)
            self.readonly = 0
            self.is_a_reference = 0
            self.startpos = 0
            self.copy_buf_size = max(4096, up_power2(self.length))
            self.copy_buf_pointer = <char*> PyMem_Malloc(self.copy_buf_size)
            if self.copy_buf_pointer == NULL:
                raise MemoryError("Could not allocate initial memory for the copy_buf")
            self.buf_pointer = self.copy_buf_pointer
            if self.length > 0:
                memcpy(self.buf_pointer, self.src_view.buf + startpos, self.length)

            # dont keep the reference to the original buffer
            PyBuffer_Release(self.src_view)
            PyMem_Free(self.src_view)
            self.src_view = NULL
            self.original_obj = None
        else:
            # direct reference to the original buffer
            self.readonly = 1 if self.src_view.readonly else 0
            self.is_a_reference = 1
            self.startpos = startpos
            self.buf_pointer = <char*> self.src_view.buf
            self.copy_buf_pointer = NULL
            self.copy_buf_size = 0

    def __init__(self, object src=None, int64_t startpos=0, int64_t length=-1, bint copy=0):
        """
        __init__(src=None, int64_t startpos=0, int64_t length=-1, bint copy=0)

        Parameters
        ----------
        src: an object that supports the buffer protocol
            The object that the MBufferIO references (or copies)
        startpos: int64_t
            start to read the 'src' content with this offset
        length: int64_t
            how many bytes to read/reference from 'src'. -1 for all 'src' content.
        copy: bool
            if True, make a copy of 'src' content to the MBufferIO (the MBufferIO is not a reference)
        """

    def __dealloc__(self):
        self.close()
        if self.copy_buf_pointer is not NULL:
            PyMem_Free(self.copy_buf_pointer)
            self.copy_buf_pointer = NULL

    cpdef close(self):
        """
        close(self)
        Close the MBufferIO. All further operations will fail.
        """
        if self.view_count > 0:
            raise ValueError("Can not modify the buffer when there are active views")
        # Once the file is closed, any operation on the file (e.g. reading or writing) will raise a ValueError.
        self.closed = 1
        if self.src_view is not NULL:
            PyBuffer_Release(self.src_view)
            PyMem_Free(self.src_view)
            self.src_view = NULL

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    cpdef detach(self, int64_t how_many_more_bytes=0):
        """
        detach(int64_t how_many_more_bytes=0)
        Detach from the object that MBufferIO represents.

        If the MBufferIO is a reference, copy the referenced object to the internal buffer. From then on, 'src' and
        the MBufferIO are independant.

        If the MBufferIO is already a copy, grow the internal buffer.

        Parameters
        ----------
        how_many_more_bytes: int64_t
            Allocate at least 'how_many_more_bytes' bytes more than necessary to make the copy
        """
        if self.is_a_reference:
            self.is_a_reference = 0
            self.copy_buf_size = max(4096, up_power2(self.length + how_many_more_bytes))
            self.copy_buf_pointer = <char*> PyMem_Malloc(self.copy_buf_size)
            if self.copy_buf_pointer == NULL:
                raise MemoryError(u"Could not allocate enough memory when copying buf")
            memcpy(self.copy_buf_pointer, self.buf_pointer + self.startpos, self.length)
            self.buf_pointer = self.copy_buf_pointer
            self.startpos = 0
            self.readonly = 0
            if self.src_view is not NULL:
                # release the view on the original object
                PyBuffer_Release(self.src_view)
                PyMem_Free(self.src_view)
                self.src_view = NULL
                self.original_obj = None
            return

        elif ((self.length + how_many_more_bytes) <= (self.copy_buf_size - self.startpos)) and (self.readonly == 0):
            # self is already a copy, we have enough space and is writeable
            return

        else:
            # grow copy_buf
            self.copy_buf_size = max(4096, up_power2(self.copy_buf_size + how_many_more_bytes))
            self.copy_buf_pointer = <char*> PyMem_Malloc(self.copy_buf_size)
            if self.copy_buf_pointer == NULL:
                raise MemoryError(u"Could not reallocate enough memory")
            memcpy(self.copy_buf_pointer, self.buf_pointer + self.startpos, self.length)
            PyMem_Free(self.buf_pointer)
            self.buf_pointer = self.copy_buf_pointer
            self.startpos = 0
            self.readonly = 0
            return


    cpdef write(self, object obj_to_write):
        """
        write(object obj_to_write)
        Write the content of 'obj_to_write' into the MBufferIO.

        If the MBufferIO is a reference, and the referenced object is big enough, write 'obj_to_write'
        directly to the referenced object.

        If the MBufferIO is a reference, and the referenced object is not big enough, make a copy of the referenced
        object to the internal buffer, then write 'obj_to_write' to the internal buffer.

        If the MBufferIO is a copy, write to the internal buffer.

        Parameters
        ----------
        obj_to_write: an object that supports the buffer protocol
            Object to write

        Returns
        -------
        how many bytes were written
        """
        if self.closed:
            raise ValueError(u"I/O operation on closed file.")

        cdef int res = 0
        cdef char* obj_addr = NULL
        cdef int64_t obj_length = 0
        cdef Py_buffer* obj_view = NULL
        cdef int64_t how_many_more_bytes = 0

        if PyUnicode_Check(obj_to_write):
            obj_to_write = PyUnicode_AsUTF8String(obj_to_write)

        # lets get the object address and length
        if isinstance(obj_to_write, bytes):
            obj_addr = <char*> obj_to_write
            obj_length = len(obj_to_write)
        elif isinstance(obj_to_write, cy_memoryview):
            obj_addr = <char*> (<cy_memoryview>obj_to_write).get_item_pointer([])
            obj_length = len(obj_to_write)
        elif PyObject_CheckBuffer(obj_to_write):
            obj_view = <Py_buffer*> PyMem_Malloc(sizeof(Py_buffer))
            if obj_view == NULL:
                raise MemoryError
            res = PyObject_GetBuffer(obj_to_write, obj_view, PyBUF_SIMPLE)
            if res == -1:
                PyMem_Free(obj_view)
                raise RuntimeError("PyObject_GetBuffer failed")
            obj_addr = <char*> obj_view.buf
            obj_length = obj_view.len
        else:
            obj_to_write = bytes(obj_to_write)
            obj_addr = <char*> obj_to_write
            obj_length = len(obj_to_write)

        try:
            if obj_length == 0:
                return 0

            how_many_more_bytes = max(0, obj_length + self.offset - self.length)
            # we may have to grow the buffer...
            if (how_many_more_bytes > 0) or bool(self.readonly):
                if self.view_count > 0:
                    raise ValueError("Can not modify the buffer when there are active views")
                self.detach(how_many_more_bytes)

            # copy the content of obj_to_write
            memcpy(self.buf_pointer + self.startpos + self.offset, obj_addr, obj_length)
            self.offset += obj_length
            self.length += how_many_more_bytes
            return obj_length
        finally:
            if obj_view is not NULL:
                PyBuffer_Release(obj_view)
                PyMem_Free(obj_view)

    cpdef seek(self, int64_t pos, int whence=0):
        """
        seek(int64_t pos, int whence=0)
        Change the stream position to the given byte offset. offset is interpreted relative to the position indicated
        by whence.

        Parameters
        ----------
        pos: int64_t
            offset
        whence: int
            0 (start of the stream), 1 (current stream position) or 2 (end of the stream)

        Returns
        -------
        the new absolute position
        """
        if self.closed:
            raise ValueError(u"I/O operation on closed file.")
        cdef int64_t frm
        cdef int64_t final_pos
        if whence == 0:         # Start of stream
            frm = 0
        elif whence == 1:
            frm = self.offset   # current position
        elif whence == 2:
            frm = self.length   # end of stream
        else:
            raise ValueError("invalid value for whence parameter")
        final_pos = frm + pos
        final_pos = max(0, final_pos)
        final_pos = min(self.length, final_pos)
        self.offset = final_pos
        return final_pos

    cpdef bytes read(self, int64_t n=-1):
        """
        read(int64_t n=-1)
        Read up to n bytes from the MBufferIO and return them.

        Parameters
        ----------
        n: int64_t
            how many bytes to read or -1 to read all that's available

        Returns
        -------
        read bytes
        """
        if self.closed:
            raise ValueError(u"I/O operation on closed file.")
        if n == -1:
            return self.readall()
        if n == 0 or self.offset >= self.length:
            return b''

        cdef int64_t to_read = min(n, self.length - self.offset)
        cdef int64_t current_offset
        current_offset, self.offset = self.offset, self.offset + to_read
        return <bytes> (self.buf_pointer[self.startpos + current_offset:self.startpos + self.offset])

    cpdef bytes readl(self, int64_t n=-1):
        """
        readl(self, int64_t n=-1)
        Synonym for the 'read' method
        """
        if self.closed:
            raise ValueError(u"I/O operation on closed file.")
        return self.read(n)

    cpdef bytes readline(self, int64_t limit=-1):
        """
        readline(int64_t limit=-1)
        Read and return one line from the stream. If 'limit' is specified, at most 'limit' bytes will be read.

        Parameters
        ----------
        limit: maximum number of bytes to read (or -1 for no limit)

        Returns
        -------
        Bytes read
        """
        if self.closed:
            raise ValueError(u"I/O operation on closed file.")
        if self.offset == self.length or limit == 0:
            return b''

        if limit == 1:
            return self.read(1)

        cdef int64_t current_position = self.offset
        cdef int64_t max_length = min(self.length - self.offset, limit) if limit > 1 else (self.length - self.offset)
        cdef int64_t len_to_read = 1
        cdef int64_t current_offset
        while self.buf_pointer[self.startpos + current_position] != b"\n" and len_to_read < max_length:
            current_position += 1
            len_to_read += 1
        current_offset, self.offset = self.offset, self.offset + len_to_read
        return <bytes> (self.buf_pointer[self.startpos + current_offset:self.startpos + self.offset])

    cpdef bytes readall(self):
        """
        readall()
        Read and return all the bytes from the stream

        Returns
        -------
        bytes read
        """
        if self.closed:
            raise ValueError(u"I/O operation on closed file.")
        if self.length == self.offset:
            return b''
        cdef int64_t current_offset
        current_offset, self.offset = self.offset, self.length
        return <bytes> (self.buf_pointer[self.startpos + current_offset:self.startpos + self.offset])

    def __str__(self):
        if self.closed:
            return b''
        return <bytes> self.buf_pointer[self.startpos:self.startpos + self.length]

    cpdef bytes getvalue(self):
        """
        getvalue()
        Return bytes containing the entire contents of the MBufferIO.

        Returns
        -------
        bytes
        """
        if self.closed:
            raise ValueError(u"I/O operation on closed file.")
        return self.__str__()

    def __len__(self):
        if self.closed:
            return 0
        return self.length

    cpdef extend(self, object obj_to_write):
        """
        extend(obj_to_write)`
        Writes 'obj_to_write' to the end of the MBufferIO

        Parameters
        ----------
        obj_to_write: an object that supports the buffer protocol

        Returns
        -------
        Number of bytes written
        """
        if self.closed:
            raise ValueError(u"I/O operation on closed file.")
        self.seek(0, 2)
        return self.write(obj_to_write)

    cpdef readinto(self, object destination):
        """
        readinto(destination)
        Read up to len(destination) bytes from the MBufferIO into 'destination' and return the number of bytes read.

        Parameters
        ----------
        destination: any object that supports the buffer protocol

        Returns
        -------
        number of read bytes
        """
        if not PyObject_CheckBuffer(destination):
            raise TypeError("exportto: parameter 'destination' must support the buffer protocol")

        cdef Py_buffer* dest_view
        cdef int64_t dest_len
        cdef char* dest_addr

        dest_view = <Py_buffer*> PyMem_Malloc(sizeof(Py_buffer))
        if dest_view == NULL:
            raise MemoryError
        if PyObject_GetBuffer(destination, dest_view, PyBUF_SIMPLE) == -1:
            PyMem_Free(dest_view)
            raise RuntimeError("PyObject_GetBuffer failed")
        dest_addr = <char*> dest_view.buf
        dest_len = min(<int64_t> dest_view.len, self.length - self.offset)
        try:
            if dest_len == 0:
                return 0
            if dest_view.readonly:
                raise TypeError("destination is read-only")

            memcpy(dest_addr, self.buf_pointer + self.startpos + self.offset, dest_len)
            self.offset += dest_len
            return dest_len

        finally:
            if dest_view is not NULL:
                PyBuffer_Release(dest_view)
                PyMem_Free(dest_view)



    cpdef exportto(self, object destination):
        """
        exportto(destination)
        Copy the content of the MBufferIO to 'destination'. At most len(destination) bytes will be copied.

        Parameters
        ----------
        destination: a writeable object that supports the buffer protocol
            where to copy the MBufferIO

        Returns
        -------
        How many bytes copied
        """

        if not PyObject_CheckBuffer(destination):
            raise TypeError("exportto: parameter 'destination' must support the buffer protocol")

        cdef Py_buffer* dest_view
        cdef int64_t dest_len
        cdef char* dest_addr

        dest_view = <Py_buffer*> PyMem_Malloc(sizeof(Py_buffer))
        if dest_view == NULL:
            raise MemoryError
        if PyObject_GetBuffer(destination, dest_view, PyBUF_SIMPLE) == -1:
            PyMem_Free(dest_view)
            raise RuntimeError("PyObject_GetBuffer failed")
        dest_addr = <char*> dest_view.buf
        dest_len = min(<int64_t> dest_view.len, self.length)
        try:
            if dest_len == 0:
                return 0
            if dest_view.readonly:
                raise TypeError("destination is read-only")

            memcpy(dest_addr, self.buf_pointer + self.startpos, dest_len)

            return dest_len
        finally:
            if dest_view is not NULL:
                PyBuffer_Release(dest_view)
                PyMem_Free(dest_view)

    cpdef tobytearray(self):
        """
        tobytearray()
        Returns a copy of the MBufferIO content as a bytearray
        """
        if self.length == 0:
            return bytearray()
        temp = bytearray(self.length)
        self.exportto(temp)
        return temp

    cpdef tobytes(self):
        """
        tobytes()
        Returns a copy of the MBufferIO content as a 'bytes' object
        """
        return bytes(self)

    cpdef fileno(self):
        """
        fileno()
        Always raise IOError
        """
        raise IOError(u"The IO object does not use a file descriptor")

    cpdef flush(self):
        """
        flush()
        Do nothing
        """
        if self.closed:
            raise ValueError(u"I/O operation on closed file.")

    cpdef isatty(self):
        """
        isatty()
        Returns False
        """
        return False

    cpdef readable(self):
        """
        readable()
        True if the MBufferIO is not closed
        """
        return not self.closed

    cpdef seekable(self):
        """
        seekable()
        True if the MBufferIO is not closed
        """
        return not self.closed

    cpdef tell(self):
        """
        tell()
        Return the current stream position
        """
        if self.closed:
            raise ValueError(u"I/O operation on closed file.")
        return self.offset

    cpdef writable(self):
        """
        writable()
        If the MBufferIO is a reference, tells if the referenced object is writeable.

        If the MBufferIO is a copy, returns True.
        """
        if self.closed:
            return False
        return not self.readonly

    cpdef writelines(self, lines):
        """
        writelines(lines)
        Write a list of lines to the MBufferIO.

        Parameters
        ----------
        lines: an iterable
        """
        if self.closed:
            raise ValueError(u"I/O operation on closed file.")
        if self.view_count > 0:
            raise ValueError("Can not modify the buffer when there are active views")
        # Write a list of lines to the stream. Line separators are not added, so it is usual for each of the lines
        # provided to have a line separator at the end.
        for line in lines:
            self.write(line)

    cpdef readlines(self, int64_t hint=-1):
        """
        readlines(int64_t hint=-1)
        Read and return a list of lines from the stream. hint can be specified to control the number of lines read:
        no more lines will be read if the total size (in bytes/characters) of all lines so far exceeds hint.

        Parameters
        ----------
        hint: int64_t

        Returns
        -------
        a list of 'bytes' objects
        """
        # Read and return a list of lines from the stream. hint can be specified to control the number of lines read:
        # no more lines will be read if the total size (in bytes/characters) of all lines so far exceeds hint.
        if self.closed:
            raise ValueError(u"I/O operation on closed file.")
        lines = []
        cdef bytes line
        cdef int64_t bytes_read = 0
        while True:
            line = self.readline()
            bytes_read += len(line)
            lines.append(line)
            if 0 <= hint < bytes_read or self.offset >= self.length:
                break
        return lines

    def __iter__(self):
        if self.closed:
            raise ValueError(u"I/O operation on closed file.")
        while self.offset < self.length:
            yield self.readline()

    def __getbuffer__(self, Py_buffer *pybuf, int flags):
        if self.closed:
            raise BufferError("the object is closed")
        if pybuf == NULL:
            raise BufferError("pybuf is NULL")
        if bool(flags & PyBUF_WRITABLE) and bool(self.readonly):
            raise BufferError('read only object')
        if bool(flags & PyBUF_STRIDES) or bool(flags & PyBUF_ND):
            raise BufferError

        self.shape[0] = self.length
        pybuf.buf = self.buf_pointer + self.startpos
        pybuf.len = self.length
        pybuf.readonly = 1 if self.readonly else 0
        pybuf.format = "B"
        pybuf.ndim = 1
        pybuf.shape = self.shape
        pybuf.strides = NULL
        pybuf.suboffsets = NULL
        pybuf.itemsize = 1
        pybuf.internal = NULL
        pybuf.obj = self
        self.view_count += 1

    def __releasebuffer__(self, Py_buffer *pybuf):
        self.view_count -= 1

    def __getitem__(self, item):
        cdef int64_t slice_length
        cdef int64_t i, j

        if self.closed:
            raise RuntimeError("The buffer is closed")

        if isinstance(item, slice):
            start, stop, stride = item.indices(self.length)
            i = start
            j = stop
            if stride != 1:
                raise ValueError("does not support step != 1")
            slice_length = max(j - i, 0)
            if self.original_obj is None:
                return MBufferIO(self, i, slice_length)     # startpos in handled in __getbuffer__
            else:
                return MBufferIO(self.original_obj, self.startpos + i, slice_length)

        i = item
        if i >= self.length or i < (-self.length):
            raise IndexError

        if i < 0:
            i = self.length + i

        return <bytes> (self.buf_pointer[self.startpos + i])

    cpdef murmur128(self, prefix=b'', to_unicode=False):
        """
        murmur128(prefix=b'', to_unicode=False)
        Returns the hexadecimal encoded murmur128 hash of the MBufferIO content.

        Parameters
        ----------
        prefix: bytes
            an optional prefix
        to_unicode: bool
            If True, return the hash as unicode. If False, return the hash as 'bytes'.

        Returns
        -------
        The hash
        """
        prefix = make_utf8(prefix)
        if not self:
            return prefix
        retbuf = bytearray(16)
        cdef int res = qhashmurmur3_128(<void*>(self.buf_pointer + self.startpos), <size_t> self.length, <void *> (<char*> retbuf))
        if res == 0:
            return None
        result = prefix + bytes(retbuf).encode('hex')
        if to_unicode:
            return unicode(result)
        return result

    def __hash__(self):
        if not self:
            return 0
        return qhashmurmur3_32(<void*> (self.buf_pointer + self.startpos), <int> self.length)

    def __richcmp__(self, other, op):
        cdef int ope = op
        if ope == 3:
            return not self == other
        if ope == 2:
            if self.closed:
                if isinstance(other, MBufferIO):
                    return other.closed
                return False
            if isinstance(other, MBufferIO):
                if other.closed:
                    return False
                if hash(self) != hash(other):
                    return False
                return bytes(self) == bytes(other)
            return False
        raise ValueError("operation not supported")

    def __reduce__(self):
        if self.closed:
            raise RuntimeError("Can't reduce a closed MBufferIO")
        return MBufferIO, bytes(self)

    def __nonzero__(self):
        if self.closed:
            return False
        return self.length != 0

    def __copy__(self):
        if self.closed:
            raise RuntimeError(u"Can't copy a closed MBufferIO")
        if self.original_obj is None:
            return MBufferIO(self)
        return MBufferIO(self.original_obj, self.startpos, self.length)


cpdef murmur128(obj, prefix=b'', to_unicode=False):
    cdef size_t length
    cdef void* buf
    cdef int res
    prefix = make_utf8(prefix)
    if PyUnicode_Check(obj):
        obj = PyUnicode_AsUTF8String(obj)
    if not PyObject_CheckBuffer(obj):
        if hasattr(obj, '__hash__'):
            obj = bytes(obj.__hash__())
        else:
            obj = bytes(obj)
    prefix = bytes(prefix)
    retbuf = bytearray(16)

    cdef Py_buffer* view = <Py_buffer*> PyMem_Malloc(sizeof(Py_buffer))
    if view == NULL:
        raise MemoryError("Could not allocate memory for the Py_buffer")
    res = PyObject_GetBuffer(obj, view, PyBUF_SIMPLE)
    if res == -1:
        PyMem_Free(view)
        raise RuntimeError("PyObject_GetBuffer failed")
    length = <size_t> view.len
    buf = <void*> view.buf
    try:
        res = qhashmurmur3_128(buf, length, <void *> (<char*> retbuf))
        if res == 0:
            return None
        result = prefix + bytes(retbuf).encode('hex')
        if to_unicode:
            return unicode(result)
        return result
    finally:
        if view is not NULL:
            PyBuffer_Release(view)
            PyMem_Free(view)


cpdef umurmur128(obj, prefix=b''):
    return murmur128(obj, prefix, to_unicode=True)

cpdef unicode make_unicode(s):
    if s is None:
        return u''
    if PyUnicode_Check(s):
        return s
    if PyBytes_Check(s):
        return s.decode('utf-8')
    if hasattr(s, '__bytes__'):
        return s.__bytes__().decode('utf-8')
    if hasattr(s, '__unicode__'):
        return s.__unicode__()
    return make_unicode(unicode(s))


cpdef bytes make_utf8(s):
    if s is None:
        return b''
    if PyBytes_Check(s):
        return s
    if PyUnicode_Check(s):
        return PyUnicode_AsUTF8String(s)
    if hasattr(s, '__bytes__'):
        return s.__bytes__()
    if hasattr(s, '__unicode__'):
        return PyUnicode_AsUTF8String(s.__unicode__())
    return make_utf8(bytes(s))

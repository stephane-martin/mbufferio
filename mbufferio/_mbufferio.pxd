# -*- coding: utf-8 -*-


# noinspection PyUnresolvedReferences
from libc.stdint cimport uint8_t, uintptr_t, int64_t, uint64_t, uint32_t
# noinspection PyUnresolvedReferences
from cpython.ref cimport PyObject


cdef extern from "murmur.h" nogil:
    uint32_t qhashmurmur3_32(void *data, int nbytes)
    int qhashmurmur3_128(void *data, size_t nbytes, void *retbuf)


cdef class MBufferIO(object):
    cdef Py_ssize_t shape[1]
    cdef readonly int view_count
    cdef char* buf_pointer
    cdef char* copy_buf_pointer
    cdef readonly int64_t copy_buf_size
    cdef Py_buffer* src_view
    cdef readonly int64_t length
    cdef readonly int64_t startpos
    cdef readonly int64_t offset
    cdef readonly bint closed
    cdef readonly bint readonly
    cdef readonly bint is_a_reference
    cdef object original_obj

    cpdef bytes read(self, int64_t n=?)
    cpdef bytes readl(self, int64_t n=?)
    cpdef bytes readline(self, int64_t limit=?)
    cpdef bytes readall(self)
    cpdef close(self)
    cpdef seek(self, int64_t pos, int whence=?)
    cpdef write(self, object obj_to_write)
    cpdef extend(self, object obj_to_write)
    cpdef bytes getvalue(self)
    cpdef readinto(self, object destination)
    cpdef exportto(self, object destination)
    cpdef fileno(self)
    cpdef flush(self)
    cpdef isatty(self)
    cpdef readable(self)
    cpdef seekable(self)
    cpdef tell(self)
    cpdef writable(self)
    cpdef writelines(self, lines)
    cpdef readlines(self, int64_t hint=?)
    cpdef tobytearray(self)
    cpdef tobytes(self)
    cpdef detach(self, int64_t how_many_more_bytes=?)
    cpdef murmur128(self, prefix=?, to_unicode=?)


cpdef inline uint64_t up_power2(uint64_t v):
    v -= 1
    v |= v >> 1
    v |= v >> 2
    v |= v >> 4
    v |= v >> 8
    v |= v >> 16
    v |= v >> 32
    v += 1
    return v


cpdef murmur128(obj, prefix=?, to_unicode=?)
cpdef umurmur128(obj, prefix=?)
cpdef unicode make_unicode(s)
cpdef bytes make_utf8(s)

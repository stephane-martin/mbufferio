History
=======

v0.5.7
------
- expose utility functions "make_utf8, make_unicode, umurmur128"

v0.5.6
------
- fix: memoryview(MBufferIO()) used to raise BufferError

v0.5.5
------
- fix: simple MBUfferIO() raised a TypeError

v0.5.3, v0.5.4
--------------
- Allow to make MBufferIO objects from memory views that have been built by PyMemoryView_FromMemory() or PyMemoryView_FromBuffer().

v0.5.2
------
- Added a "from_mview" class method.

v0.5.1
------
- Removed the oldbuffer method, for compatibility with Python 3.

v0.5
----
- First release on github.


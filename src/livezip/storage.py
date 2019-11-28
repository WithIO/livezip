from abc import ABC, abstractmethod
from struct import calcsize, pack
from typing import Iterator
from zlib import crc32

from .models import CompressionMethod
from .stream import DataStream


class CompactFile(ABC):
    """
    Basic interface for a storage method. The idea is that we want to keep our
    goal of having a predictable output size, however we also want to be able
    to encode files in specific way. The goal of this interface is to notify
    the encoder that the output is going to be of that specific length and that
    the data is this. As long as you can respect this contract then it works.

    The intended purpose is to store the file as non-compressed deflate, which
    allow to circumvent some Apple bugs (versus just non-compressed file).

    If you wanted to have a compression method that really compresses then you
    would need to compress the files beforehand and cache the compressed files,
    then you could feed the compressed files into a custom storage.
    """

    @property
    @abstractmethod
    def compressed_size(self) -> int:
        """
        Number of bytes of the compressed data
        """

        raise NotImplementedError

    @property
    @abstractmethod
    def uncompressed_size(self) -> int:
        """
        Number of bytes of the original data
        """

        raise NotImplementedError

    @property
    @abstractmethod
    def compression_method(self) -> CompressionMethod:
        """
        Compression method to assign this to in the ZIP file.
        """

        raise NotImplementedError

    @property
    @abstractmethod
    def crc32(self) -> int:
        """
        Returns the CRC32 of the file, called only once `get_data()` ran.
        """

        raise NotImplementedError

    @abstractmethod
    def get_data(self) -> Iterator[bytes]:
        """
        Iterates over data chunks of arbitrary length
        """

        raise NotImplementedError


class DeflateStore(CompactFile):
    """
    Stores the data into DEFLATE format, only using non-compressed blocks.

    Notes
    -----
    If you don't mind the small overhead, it's much simpler to decode for ZIP
    tools and easier to recover if the file is corrupt because deflates lets
    you know the size of the file (well more or less) so you don't have to
    scan the whole file to find what comes next. Also some clients just don't
    know how to do without it.
    """

    BLOCK_SIZE = 0xFFFF
    BLOCK_HEADER = "<BHH"

    def __init__(self, data: DataStream, size: int):
        self.data = data
        self._size = size
        self._crc32 = 0

    @property
    def blocks(self) -> int:
        """
        Number of blocks that will be output.

        Notes
        -----
        As the deflate algorithm allows unlimited data streams, we're not sure
        if the floating precision will be enough for our needs. That's why all
        operations in that function leverage the arbitrary integer size of
        Python and no operation is done in floating space. That's why we are
        not using `ceil()` and implement it manually instead.

        While the size of the integer used here is unknown and could in theory
        eat up a lot of memory if unchecked, the current hardware limitations
        make it very unlikely that this integer goes beyond a few bytes.
        """

        blocks = self._size // self.BLOCK_SIZE

        if self._size % self.BLOCK_SIZE:
            blocks += 1

        return blocks

    @property
    def compressed_size(self) -> int:
        """
        Output size is the regular size of data plus the size of each block's
        header
        """

        return self.blocks * calcsize(self.BLOCK_HEADER) + self._size

    @property
    def uncompressed_size(self) -> int:
        return self._size

    @property
    def compression_method(self) -> CompressionMethod:
        return CompressionMethod.deflate

    @property
    def crc32(self) -> int:
        """
        The CRC32 is computed during `get_data()`
        """

        return self._crc32

    def get_data(self) -> Iterator[bytes]:
        """
        Yields all the data formatted inside proper DEFLATE blocks
        """

        last_offset = (self.blocks - 1) * self.BLOCK_SIZE

        self.data.open()

        try:
            for i in range(0, self.uncompressed_size, self.BLOCK_SIZE):
                if i == last_offset:
                    block_format = 0b00000001
                else:
                    block_format = 0b00000000

                data = self.data.read(self.BLOCK_SIZE)
                args = [block_format, len(data), len(data) ^ self.BLOCK_SIZE]
                header = pack(self.BLOCK_HEADER, *args)

                self._crc32 = crc32(data, self._crc32)

                yield header + data
        finally:
            self.data.close()
            del self.data


class Store(CompactFile):
    """
    Just stores the raw uncompressed file
    """

    READ_SIZE = 1024 ** 2  # 1 Mio

    def __init__(self, data: DataStream, size: int):
        self.data = data
        self._size = size
        self._crc32 = 0

    @property
    def compressed_size(self) -> int:
        return self._size

    @property
    def uncompressed_size(self) -> int:
        return self._size

    @property
    def compression_method(self) -> CompressionMethod:
        return CompressionMethod.uncompressed

    @property
    def crc32(self) -> int:
        """
        Computed during get_data()
        """

        return self._crc32

    def get_data(self) -> Iterator[bytes]:
        self.data.open()

        try:
            for _ in range(0, self.uncompressed_size, self.READ_SIZE):
                data = self.data.read(self.READ_SIZE)
                self._crc32 = crc32(data, self._crc32)
                yield data
        finally:
            self.data.close()
            del self.data

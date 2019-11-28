from abc import ABC
from struct import calcsize
from typing import Any, Iterator, NamedTuple, Sequence

from pendulum import DateTime

from .models import (
    GP_LANGUAGES_ENCODING,
    GP_STREAM,
    CentralDirectoryFile,
    DataDescriptor,
    EndOfCentralDirectoryRecord,
    LocalFileHeader,
    Zip64EndOfCentralDirectoryLocator,
    Zip64EndOfCentralDirectoryRecord,
    Zip64ExtraField,
)
from .storage import CompactFile

VERSION_NEEDED = (4, 5)
VERSION_USED = (4, 5)


class ZipFile(NamedTuple):
    """
    Represents a file that is going to be put in a Zip archive. It contains
    all the information required to generate the said archive.
    """

    # File path within the zip, separated by slashes. Must not contain leading
    # slash, point, drive letter, etc.
    path: str

    # Storage object which will indicate file size, compression method, etc.
    data: CompactFile

    # Modification date to be set in the zip
    modification_date: DateTime

    # Indicates if this file contains text or contains binary data
    is_binary: bool

    # A comment to attach to this file specifically
    comment: str = ""


class Segment(ABC):
    """
    Utility class which helps to generate the various in a streaming manner:
    The get_length() method indicates the predicted length of the data without
    requiring the data to be generated. Then the data can be retrieved with
    get_data(). And finally all segments might want to query other segments,
    this is why there is an access to the encoder object.
    """

    def __init__(self, encoder: "ZipEncoder"):
        self.encoder = encoder

    def get_length(self) -> int:
        """
        Computes the length that this segment is going to have

        Returns
        -------
        Predicted length of the data
        """

        raise NotImplementedError

    def get_reference(self) -> Any:
        """
        Generates a reference (which must be hashable) for this segment so that
        other segments can reference it.

        Returns
        -------
        Any hashable value
        """

        raise NotImplementedError

    def get_data(self) -> Iterator[bytes]:
        """
        Iterates over the data chunks for this segment.

        Returns
        -------
        An iterator of all the bytes strings.
        """

        raise NotImplementedError


class LocalFileHeaderSegment(Segment):
    """
    Represents the "local file header"
    """

    def __init__(self, encoder: "ZipEncoder", file_id: int, file: ZipFile):
        super().__init__(encoder)

        self.file_id = file_id
        self.file = file

    @property
    def _struct(self):
        """
        We're streaming the data so at this point we don't know yet the CRC.
        This means that we set the GP_STREAM flag, which states that CRC and
        sizes should be zero. Anyways since we're doing ZIP64, those values
        are useless. The CRC can be found in the data descriptor segment which
        comes after the file as well as in the central directory entries. The
        sizes can be found in the central directory entry as well as in the
        data descriptor field (unless the file is too big, in which case only
        the ZIP64 entry is reliable).
        """

        return LocalFileHeader(
            version_needed=VERSION_NEEDED,
            general_purpose=(GP_LANGUAGES_ENCODING | GP_STREAM),
            compression_method=self.file.data.compression_method,
            last_modification=self.file.modification_date,
            crc32=0,
            compressed_size=0,
            uncompressed_size=0,
            file_name=self.file.path,
            extra_fields=[],
        )

    def get_length(self) -> int:
        """
        Since there is potential extra fields we can't get the length without
        generating the data
        """

        return len(self._struct.pack())

    def get_data(self) -> Iterator[bytes]:
        """
        There's only one chunk of data here
        """

        yield self._struct.pack()

    def get_reference(self) -> Any:
        """
        Embed the file ID in the reference so that other segments regarding
        that file can get the offset.
        """

        return "file_header", self.file_id


class FileDataSegment(Segment):
    """
    The data itself.
    """

    def __init__(self, encoder: "ZipEncoder", file_id: int, file: ZipFile):
        super().__init__(encoder)

        self.file_id = file_id
        self.file = file

    @property
    def crc32(self) -> int:
        """
        Proxy to know the file's CRC32 from storage
        """

        return self.file.data.crc32

    def get_length(self) -> int:
        """
        We rely on the information provided by the file
        """

        return self.file.data.compressed_size

    def get_data(self) -> Iterator[bytes]:
        """
        Returns the raw data, simply add some checks to verify that the
        retrieved data is exactly of the length predicted by the file's meta
        information. If it were not to be the case, it would break the file
        and thus an exception will be raised.

        Only once this function completed the crc32 attribute of this segment
        will be valid.

        Returns
        -------
        An iterator of bytes contained by the file

        Raises
        ------
        ValueError
            If the size of read data doesn't match the size of announced data
            then an error will arise. Cound also happen if the max file size
            is reached.
        """

        read = 0

        # noinspection PyTypeChecker
        for data in self.file.data.get_data():
            read += len(data)

            if read > self.file.data.compressed_size:
                raise ValueError(f'Received too much data for "{self.file.path}"')

            if not data:
                return

            yield data

        if read != self.file.data.compressed_size:
            raise ValueError(
                f'Received a different file size for "{self.file.path}" '
                f"than what was announced"
            )

    def get_reference(self) -> Any:
        """
        References this data segment
        """

        return "file_data", self.file_id


class DataDescriptorSegment(Segment):
    """
    Describes the data. That's mostly useless because this doesn't handle
    64-bits files, yet we can't have the CRC at the time of writing the header
    so we have to set the streaming flag and thus this segment is expected even
    if all the information is available in 64-bits in the central directory.

    Long story short: this brings no information but is required for streaming.
    """

    def __init__(self, encoder: "ZipEncoder", file_id: int, file: ZipFile):
        super().__init__(encoder)

        self.file_id = file_id
        self.file = file

    def get_length(self) -> int:
        """
        We know the length because the format is simple.
        """

        return calcsize("<IIII")

    def get_data(self) -> Iterator[bytes]:
        """
        Generates the data with the right offset to the file.
        """

        file_data = self.encoder.get_segment(("file_data", self.file_id))
        assert isinstance(file_data, FileDataSegment)

        yield DataDescriptor(
            crc32=file_data.crc32,
            compressed_size=self.file.data.compressed_size,
            uncompressed_size=self.file.data.uncompressed_size,
        ).pack()

    def get_reference(self) -> Any:
        """
        References this segment
        """

        return "file_descriptor", self.file_id


class CentralDirectoryFileSegment(Segment):
    """
    Registration of a file in the central directory
    """

    def __init__(self, encoder: "ZipEncoder", file_id: int, file: ZipFile):
        super().__init__(encoder)

        self.file_id = file_id
        self.file = file

    @property
    def _struct(self):
        """
        The fun thing about ZIP64 is that you must only use it when you need
        it, meaning that you can't put the stupid extra field if nothing is
        above the fucking limit. This gives the fancy logic with extra you can
        see down here.

        Other than that, at the moment when the data of this is being read,
        the CRC is already computed so we can use it.
        """

        file_data = self.encoder.get_segment(("file_data", self.file_id))
        assert isinstance(file_data, FileDataSegment)

        header_offset = self.encoder.get_offset(("file_header", self.file_id))

        extra = []

        if (
            self.file.data.compressed_size > 0xFFFF
            or self.file.data.uncompressed_size > 0xFFFF
            or header_offset > 0xFFFF
        ):
            extra.append(
                Zip64ExtraField(
                    original_size=self.file.data.uncompressed_size,
                    compressed_size=self.file.data.compressed_size,
                    header_offset=header_offset,
                    disk_start=0,
                )
            )

        return CentralDirectoryFile(
            version_made_by=VERSION_USED,
            version_needed_to_extract=VERSION_NEEDED,
            general_purpose=(GP_LANGUAGES_ENCODING | GP_STREAM),
            compression_method=self.file.data.compression_method,
            last_modification=self.file.modification_date,
            crc32=file_data.crc32,
            compressed_size=self.file.data.compressed_size,
            uncompressed_size=self.file.data.uncompressed_size,
            file_name=self.file.path,
            extra_fields=extra,
            comment=self.file.comment,
            disk_number_start=0,
            internal_file_attributes=(1 if self.file.is_binary else 0),
            external_file_attributes=0,
            relative_offset_of_local_header=header_offset,
        )

    def get_reference(self) -> Any:
        """
        References this segment
        """

        return "cd_file", self.file_id

    def get_length(self) -> int:
        """
        Because of all the variable bullshit we have to compute the length
        based on the actually generated struct.
        """

        return len(self._struct.pack())

    def get_data(self) -> Iterator[bytes]:
        """
        Generates and yields the segment's data in one go.
        """

        yield self._struct.pack()


class Zip64EndOfCentralDirectoryRecordSegment(Segment):
    """
    Just like the ZIP version but in 64 bits. If it is not required to be
    present then it will generate an empty output. Those zip decoders are
    really picky, also this isn't really part of the spec to my knowledge but
    apparently fuck you.
    """

    @property
    def is_required(self):
        """
        If any of the values is overflowing in the 32-bits version then we need
        to output this segment but if not we need not to. This makes the test.
        """

        offset_cd = self.encoder.get_offset(("cd_file", 0))
        offset_eocd = self.encoder.get_offset("eocd64_record")

        return (
            len(self.encoder.files) >= 0xFFFF
            or offset_cd >= 0xFFFFFFFF
            or offset_eocd >= 0xFFFFFFFF
        )

    @property
    def _struct(self):
        """
        Generates the data
        """

        offset_cd = self.encoder.get_offset(("cd_file", 0))
        offset_eocd = self.encoder.get_offset("eocd64_record")

        return Zip64EndOfCentralDirectoryRecord(
            version_made_by=VERSION_USED,
            version_needed_to_extract=VERSION_NEEDED,
            number_of_this_disk=0,
            number_of_the_disk_with_start=0,
            number_of_entries=len(self.encoder.files),
            number_of_entries_on_this_disk=len(self.encoder.files),
            size_of_central_directory=(offset_eocd - offset_cd),
            central_directory_offset=offset_cd,
        )

    def get_data(self) -> Iterator[bytes]:
        """
        Outputs the segment only if required (see above)
        """

        if self.is_required:
            yield self._struct.pack()

    def get_length(self) -> int:
        """
        If the segment is not required, announce a 0 length
        """

        if self.is_required:
            return len(self._struct.pack())

        return 0

    def get_reference(self) -> Any:
        """
        References the segment
        """

        return "eocd64_record"


class Zip64EndOfCentralDirectoryLocatorSegment(Segment):
    """
    That segment is just there to locate the 64-bits central directory.
    Happily, the 64-bits directory does not always appear, so this must also
    not appear using the same condition.
    """

    @property
    def is_required(self):
        """
        Checks if the 64 directory appears or not
        """

        record = self.encoder.get_segment("eocd64_record")
        assert isinstance(record, Zip64EndOfCentralDirectoryRecordSegment)
        return record.is_required

    @property
    def _struct(self):
        """
        Generates the data
        """

        offset = self.encoder.get_offset("eocd64_record")

        return Zip64EndOfCentralDirectoryLocator(
            number_of_the_disk_with_start=0,
            offset_to_end_of_central_directory_record=offset,
            number_of_disks=1,
        )

    def get_reference(self) -> Any:
        """
        References this segment
        """

        return "eocd64_locator"

    def get_length(self) -> int:
        """
        There is only a length if required
        """

        if self.is_required:
            return calcsize("<IIQI")

        return 0

    def get_data(self) -> Iterator[bytes]:
        """
        No data if not required
        """

        if self.is_required:
            yield self._struct.pack()


class EndOfCentralDirectoryRecordSegment(Segment):
    """
    Indicates the end of central directory.
    """

    @property
    def _struct(self):
        offset_cd = self.encoder.get_offset(("cd_file", 0))
        offset_eocd = self.encoder.get_offset("eocd64_record")

        return EndOfCentralDirectoryRecord(
            number_of_this_disk=0,
            number_of_the_disk_with_start=0,
            number_of_entries_on_this_disk=len(self.encoder.files),
            number_of_entries=len(self.encoder.files),
            size_of_central_directory=(offset_eocd - offset_cd),
            central_directory_offset=offset_cd,
            comment=self.encoder.comment,
        )

    def get_data(self) -> Iterator[bytes]:
        yield self._struct.pack()

    def get_length(self) -> int:
        return len(self._struct.pack())

    def get_reference(self) -> Any:
        return "eocd"


class ZipEncoder:
    """
    Encodes zips while streaming. There is basically 3 stages

    1. make_segments() generates all the segments required for this zip
    2. compute_offsets() will compute the offset of all segments based on the
       length of other segments but without generating the data itself
    3. get_data() iterates through all the data

    There is a shortcut function prepare() that does step 1 and 2. After this
    the file_size attribute is set with the final file size, which can be
    announced over HTTP by example.

    This all works because the data is uncompressed. The choice of not
    compressing data comes from 1. the fact that it's much simpler and you can
    know the file size in advance and 2. because the goal of this lib is to
    stream huge assets like video and images which can't benefit from a deflate
    compression on top of their original compression. You might think "but
    what about text files" but fuck text files.

    In case you want to implement compression for some reason, then you're
    pretty much in trouble. My guess is that you can't have the file size in
    advance anymore, also you will have to re-compute the offsets a second
    time once all files are compressed. Another option for compression would be
    to pre-computed compressed individual files and provide a custom storage
    method that allows to use them.

    An important thing is that all segments can be queried at most times and
    will generate an output to the best of their knowledge. Meaning that they
    might now know the content of the files but if their offset is known then
    they can already be generate or at least know their own length. Since the
    zip format allows to stream data and thus allows to write data that depends
    only on past data (for offsets or checksums) this model is pretty
    efficient.
    """

    def __init__(self, files: Sequence[ZipFile], comment: str = ""):
        if not files:
            raise ValueError("Unexpected empty files list")

        self.files = files
        self.comment = comment
        self.segments = None
        self.offsets = {}
        self.indexed_segments = {}
        self.file_size = 0

    def get_segment(self, reference: Any) -> Segment:
        """
        Returns a segment that matches a given reference. This requires the
        offsets to have been computed to work.

        Parameters
        ----------
        reference
            Reference that you want to reach

        Returns
        -------
        Found reference

        Raises
        ------
        KeyError
            If the reference is not found
        """

        return self.indexed_segments[reference]

    def get_offset(self, reference: Any) -> int:
        """
        Returns the offset of a given reference. This requires the offsets to
        have been computed to work.

        Parameters
        ----------
        reference
            Reference whose offset you want

        Returns
        -------
        The offset of the reference

        Raises
        ------
        KeyError
            If the reference is not found
        """

        return self.offsets[reference]

    def make_segments(self) -> None:
        """
        Generates the list of segments according to the zip specification. It's
        pretty self-explicit.

        Some ZIP64 segments are not mandatory but this is their responsibility
        to have a 0-length output when we don't need them.
        """

        out = []

        for i, file in enumerate(self.files):
            out += [
                LocalFileHeaderSegment(self, i, file),
                FileDataSegment(self, i, file),
                DataDescriptorSegment(self, i, file),
            ]

        for i, file in enumerate(self.files):
            out.append(CentralDirectoryFileSegment(self, i, file))

        out += [
            Zip64EndOfCentralDirectoryRecordSegment(self),
            Zip64EndOfCentralDirectoryLocatorSegment(self),
            EndOfCentralDirectoryRecordSegment(self),
        ]

        self.segments = out

    def compute_offsets(self) -> None:
        """
        Computes all the offsets of all the segments by adding the length of
        all segments.
        """

        offset = 0

        for segment in self.segments:
            self.offsets[segment.get_reference()] = offset
            self.indexed_segments[segment.get_reference()] = segment
            offset += segment.get_length()

        self.file_size = offset

    def prepare(self) -> None:
        """
        Runs the preliminary steps of the zip generation. After this, you
        should have the .file_size attribute ready and you can call get_data()
        to generate the data of the zip.
        """

        self.make_segments()
        self.compute_offsets()

    def get_data(self) -> Iterator[bytes]:
        """
        Returns byte strings of various length forming the data of the zip.
        """

        for segment in self.segments:
            for chunk in segment.get_data():
                yield chunk

from enum import Enum
from struct import calcsize, pack
from typing import List, NamedTuple, Tuple, Union

from pendulum import DateTime, parse

DOS_START = parse("1980-01-01T00:00:00.000000Z")
DOS_STOP = parse("2099-12-31T23:59:59.999999Z")

GP_LANGUAGES_ENCODING = 1 << 11
GP_STREAM = 1 << 3


def make_dos_date_time(date: DateTime) -> Tuple[int, int]:
    """
    Encodes a date/time object into the DOS binary format.

    Parameters
    ----------
    date
        Date to encode. It must be between DOS_START and DOS_STOP (1980 to
        2099), otherwise it will be replaced by the closest date in range.

    Returns
    -------
        A DOS-encoded date tuple. First item is the time and second is the
        date. As the DOS format doesn't let you set a time zone, the output is
        in the UTC time zone.
    """

    date = date.in_timezone("UTC")
    date = max(DOS_START, date)
    date = min(DOS_STOP, date)

    dos_date = (date.year - 1980) << 9 | date.month << 5 | date.day
    dos_time = date.hour << 11 | date.minute << 5 | (date.second // 2)

    return dos_time, dos_date


def encode_version(major: int, minor: int):
    """
    Encodes the version number into something that the binary ZIP format
    understands.

    Parameters
    ----------
    major
        Major version number (must not exceed 6553)
    minor
        Minor version number (must not exceed 9)

    Returns
    -------
    Encoded version number

    Raises
    ------
    ValueError
        If the major or minor values are inadequate
    """

    if major < 0 or minor < 0:
        raise ValueError(f"Negative version number was provided")

    if minor >= 10:
        raise ValueError(f'Minor "{minor}" cannot exceed 10.')

    version = major * 10 + minor

    if version > 0xFFFF:
        raise ValueError(f"Version {major}.{minor} is too high to be encoded")

    return version


def max_o(x, n, prevent=False):
    """
    Ensures that x fits on n bits (not bytes).

    - If prevent is True then an exception is raised
    - Otherwise just set all bits to 1

    Parameters
    ----------
    x
        Number to test
    n
        Number of bits available
    prevent
        If true then an exception will rise in case of overflow

    Returns
    -------
    A value which fits within the bit number constraint

    Raises
    ------
    ValueError
        If the value overflows and prevent is true
    """

    if x >= 1 << n:
        if prevent:
            raise ValueError
        else:
            return (1 << n) - 1

    return x


def max_2(x, prevent=False):
    """
    Fits x in 2 bytes (not bits).

    See Also
    --------
    max_o
    """

    return max_o(x, 16, prevent)


def max_4(x, prevent=False):
    """
    Fits x in 4 bytes (not bits).

    See Also
    --------
    max_o
    """

    return max_o(x, 32, prevent)


def max_8(x, prevent=False):
    """
    Fits x in 8 bytes (not bits).

    See Also
    --------
    max_o
    """

    return max_o(x, 64, prevent)


class CompressionMethod(Enum):
    """
    Supported compression methods
    """

    uncompressed = 0
    deflate = 8


class Zip64ExtraField(NamedTuple):
    """
    Extra field which holds the 64 bits information about a file

    See Also
    --------
    Section 4.5.3 of zip_spec.txt
    """

    original_size: int
    compressed_size: int
    header_offset: int
    disk_start: int

    def pack(self):
        """
        Only the fields that need to be in 64 bits must be present, so the
        logic here is a bit peculiar since we only add the fields that are
        overflowing in 32-bits (thus we need to check if they overflow and then
        append them to the output).
        """

        fields = [
            (self.original_size, "Q", 32, 64),
            (self.compressed_size, "Q", 32, 64),
            (self.header_offset, "Q", 32, 64),
            (self.disk_start, "I", 16, 32),
        ]

        fmt = "<HH"
        data = [0x0001, 0x0]

        for value, field_fmt, length, length_64 in fields:
            if value >= (1 << length):
                fmt += field_fmt
                data.append(max_o(value, length_64, prevent=True))

        data[1] = calcsize(fmt) - calcsize("<HH")

        return pack(fmt, *data)


ExtraField = Union[Zip64ExtraField]


class LocalFileHeader(NamedTuple):
    """
    Local file descriptor

    See Also
    --------
    Section 4.3.7 of zip_spec.txt
    """

    version_needed: Tuple[int, int]
    general_purpose: int
    compression_method: CompressionMethod
    last_modification: DateTime
    crc32: int
    compressed_size: int
    uncompressed_size: int
    file_name: str
    extra_fields: List[ExtraField]

    def pack(self) -> bytes:
        extra = b"".join(x.pack() for x in self.extra_fields)
        file_name = self.file_name.encode("utf-8")

        data = [
            0x04034B50,
            encode_version(*self.version_needed),
            self.general_purpose,
            self.compression_method.value,
            *make_dos_date_time(self.last_modification),
            self.crc32,
            max_4(self.compressed_size),
            max_4(self.uncompressed_size),
            max_2(len(file_name), prevent=True),
            max_2(len(extra), prevent=True),
        ]

        out = pack("<IHHHHHIIIHH", *data)

        return out + file_name + extra


class DataDescriptor(NamedTuple):
    """
    Data descriptor

    See Also
    --------
    Section 4.3.9 of zip_spec.txt
    """

    crc32: int
    compressed_size: int
    uncompressed_size: int

    def pack(self) -> bytes:
        return pack(
            "<IIII",
            0x08074B50,
            self.crc32,
            max_4(self.compressed_size),
            max_4(self.uncompressed_size),
        )


class CentralDirectoryFile(NamedTuple):
    """
    Central directory

    See Also
    --------
    Section 4.3.12 of zip_spec.txt
    """

    version_made_by: Tuple[int, int]
    version_needed_to_extract: Tuple[int, int]
    general_purpose: int
    compression_method: CompressionMethod
    last_modification: DateTime
    crc32: int
    compressed_size: int
    uncompressed_size: int
    file_name: str
    extra_fields: List[ExtraField]
    comment: str
    disk_number_start: int
    internal_file_attributes: int
    external_file_attributes: int
    relative_offset_of_local_header: int

    def pack(self) -> bytes:
        extra = b"".join(x.pack() for x in self.extra_fields)
        file_name = self.file_name.encode("utf-8")
        comment = self.comment.encode("utf-8")

        data = [
            0x02014B50,
            encode_version(*self.version_made_by),
            encode_version(*self.version_needed_to_extract),
            self.general_purpose,
            self.compression_method.value,
            *make_dos_date_time(self.last_modification),
            self.crc32,
            max_4(self.compressed_size),
            max_4(self.uncompressed_size),
            max_2(len(file_name), prevent=True),
            max_2(len(extra), prevent=True),
            max_2(len(comment), prevent=True),
            max_2(self.disk_number_start, prevent=True),
            self.internal_file_attributes,
            self.external_file_attributes,
            max_4(self.relative_offset_of_local_header),
        ]

        out = pack("<IHHHHHHIIIHHHHHII", *data)

        return out + file_name + extra + comment


class Zip64EndOfCentralDirectoryRecord(NamedTuple):
    """
    ZIP64 end of central directory record

    See Also
    --------
    Section 4.3.14 of zip_spec.txt
    """

    version_made_by: Tuple[int, int]
    version_needed_to_extract: Tuple[int, int]
    number_of_this_disk: int
    number_of_the_disk_with_start: int
    number_of_entries_on_this_disk: int
    number_of_entries: int
    size_of_central_directory: int
    central_directory_offset: int

    def pack(self) -> bytes:
        fmt = "<IQHHIIQQQQ"

        data = [
            0x06064B50,
            calcsize(fmt) - calcsize("<IQ"),
            encode_version(*self.version_made_by),
            encode_version(*self.version_needed_to_extract),
            max_4(self.number_of_this_disk, prevent=True),
            max_4(self.number_of_the_disk_with_start, prevent=True),
            max_8(self.number_of_entries_on_this_disk, prevent=True),
            max_8(self.number_of_entries, prevent=True),
            max_8(self.size_of_central_directory, prevent=True),
            max_8(self.central_directory_offset, prevent=True),
        ]

        return pack(fmt, *data)


class Zip64EndOfCentralDirectoryLocator(NamedTuple):
    """
    ZIP64 end of central directory locator

    See Also
    --------
    Section 4.3.15 of zip_spec.txt
    """

    number_of_the_disk_with_start: int
    offset_to_end_of_central_directory_record: int
    number_of_disks: int

    def pack(self) -> bytes:
        data = [
            0x07064B50,
            max_4(self.number_of_the_disk_with_start, prevent=True),
            max_8(self.offset_to_end_of_central_directory_record, prevent=True),
            max_4(self.number_of_disks, prevent=True),
        ]

        return pack("<IIQI", *data)


class EndOfCentralDirectoryRecord(NamedTuple):
    """
    End of central directory

    See Also
    --------
    Section 4.3.16 of zip_spec.txt
    """

    number_of_this_disk: int
    number_of_the_disk_with_start: int
    number_of_entries_on_this_disk: int
    number_of_entries: int
    size_of_central_directory: int
    central_directory_offset: int
    comment: str

    def pack(self) -> bytes:
        comment = self.comment.encode("utf-8")

        data = [
            0x06054B50,
            max_2(self.number_of_this_disk),
            max_2(self.number_of_the_disk_with_start),
            max_2(self.number_of_entries_on_this_disk),
            max_2(self.number_of_entries),
            max_4(self.size_of_central_directory),
            max_4(self.central_directory_offset),
            max_2(len(comment), prevent=True),
        ]

        out = pack("<IHHHHIIH", *data)

        return out + comment

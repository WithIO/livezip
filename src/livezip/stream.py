from abc import ABC, abstractmethod
from typing import BinaryIO, Callable, Optional, Text, Union
from urllib.request import urlopen


class DataStream(ABC):
    """
    Basic interface of a class that will allow you to stream data into a zip
    file. Everything is asynchronous.
    """

    @abstractmethod
    def open(self) -> None:
        """
        Use this to open whichever resources you need to open
        """

        raise NotImplementedError

    @abstractmethod
    def read(self, length: int) -> bytes:
        """
        Allows to read the next X bytes from the stream. It's important to
        respect the length parameter: each call should read exactly
        `min(remaining_data, length)` bytes.

        Parameters
        ----------
        length
            Number of bytes to return

        Returns
        -------
        `length` bytes or less if that's the end of the stream
        """

        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        """
        Use this to close the resources you need to close
        """

        raise NotImplementedError


class UrlStream(DataStream):
    """
    Streams the content found at the specified URL.
    """

    #: urlopen() timeout, in seconds
    TIMEOUT = 5

    def __init__(self, url: Callable[[], Text]):
        """
        Constructs the object

        Parameters
        ----------
        url
            The URL parameter is a callable that will be evaluated at the
            moment of `open()`.
        """

        self.url = url
        self.r = None

    def open(self) -> None:
        """
        Opens the specified URL for reading
        """

        self.r = urlopen(self.url(), timeout=self.TIMEOUT)

    def read(self, size: int) -> bytes:
        """
        Reads size bytes from the HTTP request.

        Parameters
        ----------
        size
            Number of bytes to read.

        Returns
        -------
        `size` bytes or less if that's the last bytes from the stream.
        """

        return self.r.read(size)

    def close(self):
        """
        Freeing the client's resources
        """

        self.r.close()


class FileStream(DataStream):
    """
    Naive implementation of a file stream.
    """

    def __init__(self, file_path: Union[Text, int]):
        self.file_path = file_path
        self.f: Optional[BinaryIO] = None

    def open(self) -> None:
        self.f = open(self.file_path, "rb")

    def read(self, length: int) -> bytes:
        return self.f.read(length)

    def close(self) -> None:
        if self.f:
            self.f.close()

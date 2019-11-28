from argparse import ArgumentParser, Namespace
from enum import Enum
from os import path
from typing import Optional, Sequence, Text, Type

from pendulum import from_timestamp

from .encode import ZipEncoder, ZipFile
from .storage import CompactFile, DeflateStore, Store
from .stream import FileStream


class StoreType(Enum):
    """
    Used to limit parsed values from CLI "-store" argument.
    """

    store = "store"
    deflate = "deflate"

    def get_store(self) -> Type[CompactFile]:
        """
        Returns the class corresponding to this store name
        """

        if self.value == "store":
            return Store
        elif self.value == "deflate":
            return DeflateStore


def parse_args(argv: Optional[Sequence[Text]] = None) -> Namespace:
    """
    Parses the arguments from CLI

    Parameters
    ----------
    argv
        List of arguments received in CLI

    Returns
    -------
    The parsed namespace
    """

    parser = ArgumentParser(
        description="Generates a ZIP file using livezip and its streaming method"
    )

    parser.add_argument(
        "-f",
        "--force",
        help="Overwrites the archive file if it already exists",
        action="store_true",
    )
    parser.add_argument(
        "-c", "--comment", help="A nice comment message for the file", default=""
    )
    parser.add_argument(
        "-s",
        "--store",
        help=(
            'Chooses the storage method. Valid choices are "store", '
            '"deflate". Default is "deflate".'
        ),
        default="deflate",
        type=StoreType,
    )
    parser.add_argument("archive", help="File containing the archive")
    parser.add_argument("file", help="Files to add to the archive", nargs="+")

    return parser.parse_args(argv)


def main(argv: Optional[Sequence[Text]] = None) -> None:
    """
    Adds all the files from CLI arguments into an archive. This is just for
    testing purposes, not really meant to be a useful zipping tool.

    Parameters
    ----------
    argv
        Optional CLI arguments. If not specified will take from the system
        argv, but you can also specify them as you want if you want to call
        this programmatically from Python code.
    """

    args = parse_args(argv)

    if not args.force:
        if path.exists(args.archive):
            print("Provided archive path already exists!")
            exit(1)

    files = []
    store: Type[CompactFile] = args.store.get_store()

    for file in args.file:
        files.append(
            ZipFile(
                path=file,
                data=store(FileStream(file), path.getsize(file)),
                modification_date=from_timestamp(path.getmtime(file)),
                is_binary=True,
                comment=f"Read from {file}",
            )
        )

    encoder = ZipEncoder(files, args.comment)
    print("Loaded all files...")

    encoder.prepare()
    print(f"Index prepared, output will be {encoder.file_size} octets")

    with open(args.archive, "wb") as f:
        for data in encoder.get_data():
            f.write(data)

    print(f"All done!")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Bye bye")
        exit(1)
    except RuntimeError:
        exit(1)

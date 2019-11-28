# LiveZip

Memory-bound streamable implementation of ZIP64. Aimed at streaming zips full
of multimedia assets (videos, images, etc).

## Specifications

The requirements are the following:

- **Memory-bound** &mdash; Given a list of N files, the memory stays bound
  regardless of the size of each file. You can put an upper limit on memory
  use by putting an upper limit on the number of files (the memory used for
  each file is of the order of 1 Kio).
- **Streamable** &mdash; The output can take in a stream of files and produce a
  stream of data as output. You never need to write anything on disk.
- **Predictable** &mdash; You know the size of the output file before streaming
  it, meaning that you can announce to your client (through HTTP by example)
  the size of what you are going to produce.
  
## Usage

### Example
  
An [example implementation](./__main__.py) can be used from the command line:

```
python -m livezip files.zip file1.txt file2.txt
```

### With Django

The recommendation if you need to generate zip files on the fly using Django
(or other Python framework) is:

1. Deploy two different `gunicorn` services (each with their own pool of
   workers). One of them will handle regular requests and the other one will
   handle zip streaming. You can adjust the size of both worker pools according
   to your needs
2. Put a `nginx` (or other) in front. Create a specific routing rule that will
   route requests either to the regular service either to the zip-dedicated
   service
3. Use `StreamingHttpResponse` to send your response

Example:

```python
def make_zip(request: HttpRequest, ...):
    files = [
        ...  # your ZipFile generation here
    ]

    encoder = ZipEncoder(files)
    encoder.prepare()

    response = StreamingHttpResponse(streaming_content=encoder.get_data())
    response["Content-Length"] = f"{encoder.file_size}"
    response["Content-Type"] = "application/zip"
    response["Content-Disposition"] = f"attachment; filename=example.zip"

    return response
```

### Storage

One of the main difference between livezip and other zip libraries is that the
user is responsible for files compression. Indeed, in order to predict the
output file size you need to know the size of compressed data.

Using this model it would be easy to actually compress files beforehand and
then cache them, however the current implementation only provides two
non-compressing methods.

- `livezip.storage.Store` &mdash; Stores the raw uncompressed data
- `livezip.storage.DeflateStore` &mdash; Stores the uncompressed data inside
  DEFLATE blocks
  
Afterwards, all you've got to do is to provide a `livezip.storage.DataStream`
implementation which will read all the data of the file asynchronously.
  
See [main.py](./__main__.py) for an example of use.


### Data streams

Files are read from data streams. They must implement the
`livezip.stream.DataStream` interface, which is loosely inspired from
BinaryIO but is not exactly compatible. Indeed, `DataStream` objects are opened
after they are instantiated. Example:

```python
# A BinaryIO
f = open('/tmp/file.txt', 'r')

# With a file stream
f = UrlStream(lambda: 'https://example.com/file.txt')
f.open()
```

This allows two things:

1. The ZipEncoder can open and close files on demand without knowing anything
   about the resource identifier or anything of that sort. This is helpful to
   avoid opening all your sockets at once when generating the files list
2. You can resolve resource identifiers at opening time. By example if you sign
   your S3 URLs to enable the download, since signatures are time-limited you
   can delay the signature until open time to be sure that the URL didn't
   expire
   
Two streams are provided:

- `livezip.stream.FileStream` &mdash; Streams the content from a file
- `livezip.stream.UrlStream` &mdash; Streams the content from an URL

## Complexity analysis

The intent of this library is to allow you to stream huge files into a zip
without having to retain memory about said files nor about the zip's content.
While it is mostly possible to write the zip archive in a streaming manner,
each zip file contains an index that summarizes files position and checksum,
meaning that this index can only be computed knowing the files content.
Basically, you can stream the content but the index has to stay in memory. In
any case, if you're going to generate a zip file it's likely that you will have
to work with the files list in memory.

Let's consider those numbers:

- `k` &mdash; Number of files in the zip
- `n` &mdash; Combined size of all files

For files big enough, the complexities are:

- Memory &mdash; **O(k)**
- Time &mdash; **O(n)**

In short:

- The execution time is directly proportional to how big your files are (aka
  the in/out bandwidth available)
- The real limit is the number of files. You need to hold the full list in RAM.
  Also, the `prepare()` step of the encoder will take some time proportionally
  to the number of files. **The more files you need to send simultaneously the
  bigger machine you need.**

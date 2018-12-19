class Error(RuntimeError):
    pass


class HttpError(Error):
    def __init__(self, resp, content, uri=None):
        self.resp = resp
        self.content = content
        self.uri = uri

    def __repr__(self):
        if self.uri:
            return u'<HttpError %s when requesting %s returned "%s">' % (
                self.resp.status,
                self.uri,
                self.content.strip(),
            )
        else:
            return u'<HttpError %s "%s">' % (
                self.resp.status,
                self.content,
            )


def humanize_natural_size(value):
    return value


class MediaPartialDownloader(object):
    DEFAULT_CHUNK_SIZE = 10 * 1024 * 1024  # 10MB

    def __init__(self, request, chunk_size=DEFAULT_CHUNK_SIZE):
        """Constructor.

        Args:
          request: googleapiclient.http.HttpRequest, the media request to perform in
            chunks.
          chunk_size: int, File will be downloaded in chunks of this many bytes.
        """

        self._request = request
        self._uri = request.uri
        self._chunk_size = chunk_size
        self._progress = 0
        self._total_size = None
        self._done = False

    def fetch_next_chunk(self, fd):
        headers = {
            'range': 'bytes=%d-%d' % (
                self._progress,
                self._progress + self._chunk_size - 1
            )
        }
        http = self._request.http

        resp, content = http.request(
            uri=self._uri,
            method='GET',
            headers=headers
        )

        if resp.status in [200, 206]:
            if 'content-location' in resp and resp['content-location'] != self._uri:
                self._uri = resp['content-location']

            content_length = len(content)
            self._progress += content_length
            fd.write(content)

            if 'content-range' in resp:
                content_range = resp['content-range']
                length = content_range.rsplit('/', 1)[1]
                self._total_size = int(length)
            elif 'content-length' in resp:
                self._total_size = int(resp['content-length'])

            if self._total_size is None or self._progress == self._total_size:
                self._done = True

            return DownloadProgress(
                fetch_content_size=content_length,
                resumable_progress=self._progress,
                total_size=self._total_size,
                done=self._done,
            )
        else:
            raise HttpError(resp, content, self._uri)


class DownloadProgress(object):
    def __init__(self, fetch_content_size, resumable_progress, total_size, done):
        self._fetch_content_size = fetch_content_size
        self._resumable_progress = resumable_progress
        self._total_size = total_size
        self._done = done

    def __repr__(self):
        return u'<{class_name} {attrs}>'.format(
            class_name=self.__class__.__name__,
            attrs=', '.join([
                u'fetch_content_size={}'.format(self.fetch_content_size),
                u'progress_rate={}%'.format(self.progress_rate),
                u'progress={}'.format(humanize_natural_size(self.resumable_progress)),
                u'total_size={}'.format(humanize_natural_size(self.total_size)),
            ])
        )

    @property
    def done(self):
        return self._done

    @property
    def total_size(self):
        return self._total_size

    @property
    def resumable_progress(self):
        return self._resumable_progress

    @property
    def fetch_content_size(self):
        return self._fetch_content_size

    @property
    def progress_rate(self):
        if self.total_size is not None and self.total_size != 0:
            rate = float(self.resumable_progress) / float(self.total_size) * 100.0
            return float('%.1f' % rate)
        else:
            return 0.0

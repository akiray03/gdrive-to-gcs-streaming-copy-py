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


class MediaPartialUploader(object):
    def __init__(self, http, resumable_url, content_type, total_size):
        self._http = http
        self._resumable_url = resumable_url
        self._content_type = content_type
        self._total_size = total_size
        self._progress = 0
        self._done = False

    @property
    def done(self):
        return self._done

    def upload_next_chunk(self, payload, chunk_size):

        headers = {
            'Content-Length': str(chunk_size)
        }
        if chunk_size != self._total_size:
            headers['Content-Range'] = 'bytes {from_}-{to}/{total_size}'.format(
                from_=self._progress,
                to=self._progress + chunk_size - 1,
                total_size=self._total_size,
            )

        resp, content = self._http.request(
            uri=self._resumable_url,
            method='PUT',
            headers=headers,
            body=payload
        )

        if resp.status in [200, 201]:
            self._done = True
        elif resp.status == 308:
            self._progress += chunk_size
        else:
            raise HttpError(resp, content, self._resumable_url)

        return UploadProgress(
            uploaded_content_size=chunk_size,
            resumable_progress=self._progress,
            total_size=self._total_size,
            done=self._done,
        )


class UploadProgress(object):
    def __init__(self, uploaded_content_size, resumable_progress, total_size, done):
        self._uploaded_content_size = uploaded_content_size
        self._resumable_progress = resumable_progress
        self._total_size = total_size
        self._done = done

    def __repr__(self):
        return u'<{class_name} {attrs}>'.format(
            class_name=self.__class__.__name__,
            attrs=', '.join([
                u'uploaded_content_size={}'.format(self.uploaded_content_size),
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
    def uploaded_content_size(self):
        return self._uploaded_content_size

    @property
    def progress_rate(self):
        if self.total_size is not None and self.total_size != 0:
            rate = float(self.resumable_progress) / float(self.total_size) * 100.0
            return float('%.1f' % rate)
        else:
            return 0.0

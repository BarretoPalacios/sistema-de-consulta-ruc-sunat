class UpdateError(Exception):
    pass


class DownloadError(UpdateError):
    pass


class ParseError(UpdateError):
    pass


class DatabaseError(UpdateError):
    pass


class PublishError(UpdateError):
    pass

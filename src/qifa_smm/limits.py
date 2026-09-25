import io


class FileLimitError(ValueError):
    pass


class LimitedBuffer(io.BytesIO):
    def __init__(self, limit: int):
        super().__init__()
        self.limit = limit

    def write(self, data: bytes) -> int:
        if self.tell() + len(data) > self.limit:
            raise FileLimitError("Файл превышает допустимый размер")
        return super().write(data)

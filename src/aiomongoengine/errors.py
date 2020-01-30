import re


class InvalidDocumentError(ValueError):
    pass


class MultipleObjectsReturned(ValueError):
    pass


class DoesNotExist(ValueError):
    pass


class LoadReferencesRequiredError(RuntimeError):
    pass


class PartlyLoadedDocumentError(ValueError):
    pass


pattern = (r"(?P<error_code>.+?)\s(?P<error_type>.+?):\s*(?P<index_name>.+?)"
           r"\s+(?P<error>.+?)")
PYMONGO_ERROR_REGEX = re.compile(pattern)


class UniqueKeyViolationError(RuntimeError):
    def __init__(self, message, error_code, error_type, index_name,
                 instance_type):
        super(UniqueKeyViolationError, self).__init__(message)

        self.error_code = error_code
        self.error_type = error_type
        self.index_name = index_name
        self.instance_type = instance_type

    def __str__(self):
        return f'The index "{self.index_name}" was violated when trying to ' \
               f'save this "{self.instance_type.__name__}" (error code:' \
               f' {self.error_code}).'

    @classmethod
    def from_pymongo(cls, err, instance_type):
        match = PYMONGO_ERROR_REGEX.match(err)

        if not match:
            return None

        groups = match.groupdict()

        return UniqueKeyViolationError(
            message=err, error_code=groups['error_code'],
            error_type=groups['error_type'],
            index_name=groups['index_name'], instance_type=instance_type
        )

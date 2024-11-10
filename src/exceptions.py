class RentRollError(Exception):
    """Base exception for RentRoll application"""
    pass

class PreprocessorError(RentRollError):
    """Raised when there's an error in preprocessing"""
    pass

class GroupingError(RentRollError):
    """Raised when there's an error in row grouping"""
    pass

class ConfigurationError(RentRollError):
    """Raised when there's a configuration error"""
    pass 
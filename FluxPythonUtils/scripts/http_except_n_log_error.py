import logging
from fastapi import HTTPException


# Decorator function to http try except along with logging the exception
def http_except_n_log_error(status_code: int = 500):  # 500 - internal server error
    def decorator_function(original_function):
        def wrapper_function(*args, **kwargs):
            try:
                result = original_function(*args, **kwargs)
            except Exception as e:
                logging.exception(e)
                raise HTTPException(status_code=status_code, detail=str(e))
            return result
        return wrapper_function
    return decorator_function

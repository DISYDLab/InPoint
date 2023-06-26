# from flask_restful import APIException 

# class InvalidParameter(APIException): 
#    status_code = 204
#    detail = 'Invalid parameters'
# from flask_restful import Api

# class InvalidParameter(Exception):
#     pass




# exception Exception
# All built-in, non-system-exiting exceptions are derived from this class. 
# All user-defined exceptions should also be derived from this class.
class InvalidParameter(Exception):
    """Exception raised for errors during an invalid query.
    Attributes:
        message -- explanation of the error,
        status -- HTTPresponse status code
    """
    name = "InvalidParameter"
    def __init__(self, message, status):
        # Call the base class constructor with the parameters it needs
        super().__init__(message)   
        self.status = status

class InternalServerError(Exception):
    pass

class SchemaValidationError(Exception):
    pass

class UnauthorizedError(Exception):
    pass

class NullPointerException(Exception):
    pass

errors = {
    "InternalServerError": {
        "message": "Something went wrong",
        "status": 500
    },
     "SchemaValidationError": {
         "message": "Request is missing required fields",
         "status": 400
     },
     "UnauthorizedError": {
         "message": "Invalid username or password",
         "status": 401
     },
    # "InvalidParameter": {
    #      "message": 'Invalid parameters',
    #      "status": 204
    #  },
         "NullPointerException": {
         "message": 'Invalid parameters',
         "status": 400
     }


}


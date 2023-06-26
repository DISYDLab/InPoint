# from flask_restful import APIException 

# class InvalidParameter(APIException): 
#    status_code = 204
#    detail = 'Invalid parameters'
# from flask_restful import Api

class InvalidParameter(Exception):
    pass

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
    "InvalidParameter": {
         "message": 'Invalid parameters',
         "status": 204
     },
         "NullPointerException": {
         "message": 'Invalid parameters',
         "status": 400
     }


}


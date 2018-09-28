from flask import request, Response

import json


def authenticated(password):
    def decorator(fn):
        def internal(*args, **kwargs):
            expected = request.headers.get('AUTH', 'secret')
            if expected == password:
                return fn(*args, **kwargs)
            else:
                return Response(json.dumps({'error': 'forbidden'}), status=401, content_type='application/json')

        return internal

    return decorator

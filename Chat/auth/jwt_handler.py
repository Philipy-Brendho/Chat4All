import datetime
from typing import Optional
import jwt # type: ignore

SECRET_KEY = "senhasupersecreta"
ALGORITHM = "HS256"
EXPIRES_MIN = 60 * 24

def create_token(username: str) -> str:
    payload = {
        "sub": username,
        "exp": datetime.datetime.utcnow() + datetime.timedelta(minutes=EXPIRES_MIN),
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

def decode_token(token: str) -> Optional[str]:
    try:
        data = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return data.get("sub")
    except Exception:
        return None

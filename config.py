import dotenv
import os

dotenv.load_dotenv()

URL_MONGO = os.environ.get("URL_MONGO")
TOKEN = os.environ.get("TOKEN")
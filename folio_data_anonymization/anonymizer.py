import os
import logging
from folio_data_anonymization.users import Users

logger = logging.getLogger(__name__)

data: list = ["circulation", "inventory", "organizations", "users"]


def anonymize(**kwargs) -> str:
    logging.basicConfig(filename="anonymizer.log", level=logging.INFO)
    data = kwargs.get("data", [])
    for d in data:
        match d:
            case "users":
                logger.info("Anonymizing users")
                users_task = Users()
                users_task.anonymize_users()
            case _:
                logger.info(f"No anonymizer for {d}.")

if __name__ == "__main__":
    anonymize(data=data)


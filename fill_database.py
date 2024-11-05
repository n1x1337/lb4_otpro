import requests
import logging
from dotenv import load_dotenv
from neo4j import GraphDatabase
import os

# Загрузка переменных окружения из файла .env
load_dotenv()

# Получаем ACCESS_TOKEN и параметры Neo4j из переменных окружения
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

DEFAULT_USER_ID = "296664349"

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("VK_Neo4j_Fill")

# Подключение к Neo4j
class Neo4jDatabase:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def execute_query(self, query, parameters=None):
        with self.driver.session() as session:
            return session.run(query, parameters)

# Функция для выполнения запросов к VK API
def vk_api_request(method, params):
    base_url = "https://api.vk.com/method/"
    params.update({
        "access_token": ACCESS_TOKEN,
        "v": "5.131"
    })
    response = requests.get(f"{base_url}{method}", params=params)
    response.raise_for_status()
    data = response.json()
    if "error" in data:
        logger.error(f"VK API Error: {data['error']['error_msg']}")
        return None
    return data["response"]

# Функция для получения всех фолловеров с постраничным запросом
def fetch_all_followers(user_id):
    followers = []
    offset = 0
    count = 100  # Максимум 100 фолловеров за запрос

    while True:
        response = vk_api_request("users.getFollowers", {
            "user_id": user_id,
            "fields": "first_name,last_name,sex,city,screen_name",
            "offset": offset,
            "count": count
        })
        if response and "items" in response:
            followers.extend(response["items"])
            offset += count
            if offset >= response["count"]:  # Проверяем, все ли фолловеры загружены
                break
        else:
            break  # Прерываем цикл, если нет данных или произошла ошибка

    return followers

# Функция для сбора фолловеров и подписок на два уровня вглубь
def fetch_followers_and_subscriptions(user_id, depth=2):
    results = {"users": {}, "groups": {}}
    api_request_count = 0

    def fetch_level(uid, current_depth):
        nonlocal api_request_count
        if current_depth > depth:
            return

        # Инициализация записи пользователя в results
        if uid not in results["users"]:
            results["users"][uid] = {"followers": [], "subscriptions": []}

        # Получаем всех фолловеров пользователя
        followers = fetch_all_followers(uid)
        api_request_count += 1
        logger.info(f"VK API Request {api_request_count}: users.getFollowers for user {uid}")

        # Если есть фолловеры, добавляем их в результат
        results["users"][uid]["followers"].extend(followers)
        for follower in followers:
            follower_id = follower["id"]
            if follower_id not in results["users"]:
                results["users"][follower_id] = follower
                results["users"][follower_id]["followers"] = []
                results["users"][follower_id]["subscriptions"] = []
            fetch_level(follower_id, current_depth + 1)

        # Получаем подписки (сообщества)
        subscriptions = vk_api_request("users.getSubscriptions", {"user_id": uid, "extended": 1})
        api_request_count += 1
        logger.info(f"VK API Request {api_request_count}: users.getSubscriptions for user {uid}")

        # Если есть подписки, добавляем их в результат
        if subscriptions:
            results["users"][uid]["subscriptions"].extend(subscriptions.get("items", []))
            for subscription in subscriptions.get("items", []):
                if subscription.get("type") in {"group", "page", "event"}:
                    group_id = subscription["id"]
                    results["groups"][group_id] = {
                        "name": subscription.get("name", "Неизвестное сообщество"),
                        "screen_name": subscription.get("screen_name"),
                        "type": subscription["type"]
                    }

    fetch_level(user_id, 1)
    return results

def save_data_to_neo4j(db, data):
    # Сохранение пользователей

    for user_id, user_data in data["users"].items():
        db.execute_query(
            """
            MERGE (u:User {id: $id})
            SET u.screen_name = $screen_name,
                u.name = $name,
                u.sex = $sex,
                u.home_town = $home_town
            """,
            {
                "id": user_id,
                "name": f"{user_data.get('first_name', '')} {user_data.get('last_name', '')}",
                "screen_name": user_data.get("screen_name"),
                "sex": user_data.get("sex"),
                "home_town": user_data.get("city", {}).get("title", "")
            }
        )
    for group_id, group_data in data["groups"].items():
        db.execute_query(
            """
            MERGE (g:Group {id: $id})
            SET g.name = $name,
                g.screen_name = $screen_name
            """,
            {
                "id": group_id,
                "name": group_data.get("name", "Неизвестное сообщество"),
                "screen_name": group_data.get("screen_name", "")
            }
        )
    for user_id, user_data in data["users"].items():
        for follower in user_data["followers"]:
            db.execute_query(
                """
                MATCH (follower:User {id: $follower_id})
                MATCH (user:User {id: $user_id})
                MERGE (follower)-[:FOLLOWS]->(user)
                """,
                {
                    "follower_id": follower['id'],
                    "user_id": user_id
                }
            )
    for user_id, user_data in data["users"].items():
        for subscription in user_data["subscriptions"]:
            if subscription.get("type") in {"group", "page", "event"}:
                group_id = subscription["id"]
                db.execute_query(
                    """
                    MATCH (user:User {id: $user_id})
                    MATCH (group:Group {id: $group_id})
                    MERGE (user)-[:SUBSCRIBES]->(group)
                    """,
                    {
                        "user_id": user_id,
                        "group_id": group_id
                    }
                )

# Выполнение заполнения базы данных
db = Neo4jDatabase(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
try:
    data = fetch_followers_and_subscriptions(DEFAULT_USER_ID)
    save_data_to_neo4j(db, data)
finally:
    db.close()

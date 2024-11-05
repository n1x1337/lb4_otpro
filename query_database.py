import argparse
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

# Загрузка переменных окружения из файла .env
load_dotenv()

# Получаем параметры Neo4j из переменных окружения
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

# Класс для работы с Neo4j
class Neo4jDatabase:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def execute_query(self, query, parameters=None):
        with self.driver.session() as session:
            return list(session.run(query, parameters))  # Преобразуем результат в список

def query_neo4j_data(db, query_type):
    if query_type == "total_users":
        result = db.execute_query("MATCH (u:User) RETURN count(u) AS total_users")
        total_users = result[0]["total_users"] if result else 0
        print(f"Всего пользователей: {total_users}")

    elif query_type == "total_groups":
        result = db.execute_query("MATCH (g:Group) RETURN count(g) AS total_groups")
        total_groups = result[0]["total_groups"] if result else 0
        print(f"Всего групп: {total_groups}")

    elif query_type == "top_users":
        print("Топ 5 пользователей по количеству фолловеров:")
        result = db.execute_query(
            "MATCH (u:User)<-[:FOLLOWS]-(f:User) "
            "RETURN u.name AS name, count(f) AS followers_count "
            "ORDER BY followers_count DESC LIMIT 5"
        )
        if not result:
            print("Нет данных для топа пользователей.")
        else:
            for record in result:
                print(f"{record['name']} - {record['followers_count']} фолловеров")

    elif query_type == "top_groups":
        print("Топ 5 самых популярных групп:")
        result = db.execute_query(
            "MATCH (g:Group)<-[:SUBSCRIBES]-(u:User) "
            "RETURN g.name AS name, count(u) AS subscribers_count "
            "ORDER BY subscribers_count DESC LIMIT 5"
        )
        if not result:
            print("Нет данных для топа групп.")
        else:
            for record in result:
                print(f"{record['name']} - {record['subscribers_count']} подписчиков")

    elif query_type == "mutual_followers":
        print("Пользователи, которые фолловеры друг друга:")
        result = db.execute_query(
            "MATCH (u1:User)-[:FOLLOWS]->(u2:User) "
            "WHERE (u2)-[:FOLLOWS]->(u1) "
            "RETURN u1.name AS User1, u2.name AS User2"
        )

        if not result:
            print("Нет взаимных фолловеров")
        else:
            for record in result:
                print(f"{record['User1']} и {record['User2']} - взаимные фолловеры")
    else:
        print("Неверный тип запроса. Доступные запросы: total_users, total_groups, top_users, top_groups, mutual_followers")

# Основной код для выполнения запросов
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Neo4j Query Tool")
    parser.add_argument(
        "query_type",
        type=str,
        help="Тип запроса: total_users, total_groups, top_users, top_groups, mutual_followers"
    )
    args = parser.parse_args()

    # Подключение к базе данных Neo4j
    db = Neo4jDatabase(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    try:
        query_neo4j_data(db, args.query_type)
    finally:
        db.close()

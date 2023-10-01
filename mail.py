import asyncio
import aiohttp
from more_itertools import chunked  # chunked разбирает последовательности на последовательности поменьше
from models import Base, SwapiPeople, engine, Session

MAX_REQUESTS_CHUNK = 5


async def insert_people(people_list_json):
    people_list = []
    for person in people_list_json:
        # Получаем список фильмов и преобразуем его в строку
        films = ', '.join(person.get('films', []))
        species = ', '.join(person.get('species', []))
        starships = ', '.join(person.get('starships', []))
        vehicles = ', '.join(person.get('vehicles', []))

        # Создаем экземпляр SwapiPeople с необходимыми атрибутами
        swapi_person = SwapiPeople(birth_year=person.get('birth_year'),
                                   eye_color=person.get('eye_color'),
                                   films=films,
                                   gender=person.get("gender"),
                                   hair_color=person.get("hair_color"),
                                   height=person.get("height"),
                                   homeworld=person.get("homeworld"),
                                   mass=person.get("mass"),
                                   name=person.get("name"),
                                   skin_color=person.get("skin_color"),
                                   species=species,
                                   starships=starships,
                                   vehicles=vehicles
                                   )

        people_list.append(swapi_person)

    async with Session() as session:
        session.add_all(people_list)
        await session.commit()


async def get_people(people_id):
    session = aiohttp.ClientSession()
    response = await session.get(f'https://swapi.dev/api/people/{people_id}')
    json_data = await response.json()
    await session.close()
    return json_data


# Метод gather в asyncio нужен для одновременного запуска нескольких корутин (асинхронных функций) и ожидания их
# завершения. Он позволяет собрать все корутины в одну группу и запустить их параллельно,
# вместо последовательного выполнения.
async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    for person_ids_chunk in chunked(range(1, 100), MAX_REQUESTS_CHUNK):
        person_coros = [get_people(person_id) for person_id in person_ids_chunk]
        people = await asyncio.gather(*person_coros)
        await insert_people(people)


if __name__ == "__main__":
    asyncio.run(main())

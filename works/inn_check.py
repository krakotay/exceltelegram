import os
from dadata import Dadata
import polars as pl
import numpy as np
from thefuzz import fuzz
from tqdm import tqdm
from dotenv import load_dotenv
load_dotenv()

def send_table(old: list[str], new: list[str], inn: list[str], header: str = 'Замены в тексте') -> str:
    # Формируем таблицу с использованием текста
    table = f"*{header}:*\n"
    table += "```markdown\n"  # Кодовый блок для выравнивания
    table += "| ИНН | Старая версия | Найденная в базе версия |\n"
    table += "| - |-|- |\n"

    for o, n, i in zip(old, new, inn):
        table += f"| {i} | {o} | {n} |\n"
    table += "```\n"

    return table

DADATA_KEY = os.environ["DADATA_KEY"]
SIMILARITY_THRESHOLD = 90

def normalize_list_length(lst, target_length):
    """Расширяет список до заданной длины пустыми значениями."""
    return lst + [""] * (target_length - len(lst))


def check_by_inn(df: pl.DataFrame) -> tuple[pl.DataFrame, str, str, str, str]:
    print("Начало функции check_by_inn")
    
    # Результаты
    replace_map = {}
    found = []
    replaced = []
    found_inns = []

    found_but_unchanged_names = []
    found_but_unchanged_inns = []
    best_sim = []
    not_found_inn = []
    not_found_name = []
    wrong_inn = []
    wrong_name = []

    # Работа с каждой строкой DataFrame
    for row in tqdm(df.rows(), desc="Обработка строк"):
        inn: str = row[1]
        org_name: str = row[2]

        # Проверяем, что ИНН имеет длину 10 символов
        # if len(inn) == 12:
        #     org_name = f'ИП {org_name.lower()}'
        if len(inn) == 10 or len(inn) == 12:
            with Dadata(DADATA_KEY) as dadata:
                suggestions = dadata.suggest("party", inn)

            # Если не нашлось ни одной организации
            if not suggestions:
                not_found_inn.append(inn)
                not_found_name.append(org_name)
                continue
            if len(inn) == 12:
                suggested_names: list[str] = [item["value"][3:] for item in suggestions]
            else:
            # Получаем списки названий организаций и ИНН
                suggested_names: list[str] = [item["value"] for item in suggestions]

            # Сравниваем названия и ИНН
            name_similarity = [fuzz.ratio(suggested_name.lower(), org_name.lower()) for suggested_name in suggested_names]

            # Выбираем лучшее совпадение
            best_name_idx = np.argmax(name_similarity) if name_similarity else 0
            best_name_similarity = name_similarity[best_name_idx] if name_similarity else 0

            # Анализируем результаты
            best_name = suggested_names[best_name_idx]
            if best_name_similarity >= SIMILARITY_THRESHOLD:
                replace_map[org_name] = best_name
                found.append(org_name)
                replaced.append(best_name)
                found_inns.append(inn)
            else:
                found_but_unchanged_names.append(org_name)
                found_but_unchanged_inns.append(inn)
                best_sim.append(best_name)
        else:
            wrong_inn.append(inn)
            wrong_name.append(org_name)

    # Применяем замены в DataFrame
    updated_df = df.with_columns(df[df.columns[2]].replace(replace_map))
    fixed_table = send_table(found, replaced, found_inns)
    unfixed_table = send_table(found_but_unchanged_names, best_sim, found_but_unchanged_inns, header='Неудачные замены в тексте')
    not_found_table = send_table(not_found_name, ["не найдено"] * len(not_found_name), not_found_inn, header='Не удалось найти')
    wrong_table = send_table(wrong_name, ["не найдено"] * len(wrong_name), wrong_inn, header='Неправильная длина ИНН')
    return updated_df, fixed_table, unfixed_table, not_found_table, wrong_table


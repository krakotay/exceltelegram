import asyncio
import logging
import tempfile
import polars as pl
from typing import List
from aiogram import Bot, Dispatcher
from aiogram.types import Message
from aiogram.filters import Command
import os
from datetime import datetime
from dotenv import load_dotenv
from aiogram_media_group import media_group_handler
from aiogram import F, types
from aiogram.types import FSInputFile
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiolimiter import AsyncLimiter

# from aiogram.utils.keyboard import InlineKeyboardBuilder

# import pandas as pd

# from check import check_df
from inn_check import check_by_inn
from format import format_excel
from merge import merge_tables_to_excel
from async_app import qwen_ocr, get_head

load_dotenv()

os.environ["POLARS_FMT_MAX_COLS"] = str(100)
os.environ["POLARS_FMT_MAX_ROWS"] = str(1000)

BOT_TOKEN = os.getenv("TOKEN")
admins = [461923889, 1002688109]

QWEN7B = "Qwen2-VL-7B-Instruct"
QWEN72B = "Qwen2-VL-72B-Instruct"
qwen = QWEN7B

CURRENT_MODE = "SALES_HEADERS"
HEADERS = {
    "SALES_HEADERS": [
        "№ п/п",
        "ИНН",
        "Наименование",
        "Счета-фактуры",
        "Стоимость продаж с НДС в руб. и коп. (стр. 160)",
        "Стоимость продаж облагаемых налогом всего (без суммы НДС, стр. 170 + 175 + 180 + 190)",
        "Сумма НДС всего (стр. 200 + 210)",
        "Стоимость продаж, освобождаемых от налога (стр. 220)",
        "Доля продаж (стр. 160 + 220)",
    ],
    "SHOP_HEADERS": [
        "№ п/п",
        "ИНН",
        "Наименование",
        "Счета-фактуры",
        "Стоимость покупок с НДС (стр. 170)",
        "Сумма НДС (стр. 180)",
        "Удельный вес вычетов",
    ],
}

# Настройка логирования
logging.basicConfig(level=logging.INFO)

# Создаем папку для сохранения файлов, если её нет
if not os.path.exists("downloaded_images"):
    os.makedirs("downloaded_images")

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
pending_data = {}


@dp.message(F.media_group_id, F.content_type.in_({"photo"}), F.from_user.id.in_(admins))
@media_group_handler
async def album_handler(messages: List[types.Message]):
    headers = HEADERS[CURRENT_MODE]
    await messages[0].answer(
        f"\
Текущий режим: *{'Продажи' if CURRENT_MODE == 'SALES_HEADERS' else 'Покупки'}*\n\
Размер модели: *{'Большая' if qwen == QWEN72B else 'маленькая'}*",
        parse_mode="Markdown",
    )
    await messages[0].answer(f"Текущие заголовки:\n{', '.join(headers)}")

    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    folder_path = f"downloaded_images/{current_time}"
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    images_list = []
    for i, msg in enumerate(messages):
        photo = msg.photo[-1]
        file_path = f"{folder_path}/{i}.jpg"
        images_list.append(file_path)
        await bot.download(photo.file_id, destination=file_path)

    cap = "NoneType"
    for mess in messages:
        if mess.caption:
            cap = mess.caption.replace("\n", " ")
            break

    text = f"Изображений {len(messages)}\nCaption = {cap}"
    await messages[-1].answer(text)

    # Проверяем несоответствие режима и текста
    # Если текущий режим "Покупки" (SHOP_HEADERS), а в тексте "Книга продаж",
    # или если режим "Продажи" (SALES_HEADERS), а в тексте "Книга покупок".
    # Это говорит о возможной ошибке.
    mode_is_sales = CURRENT_MODE == "SALES_HEADERS"
    caption_has_pokupok = "покупок" in cap.lower()
    caption_has_prodazh = "продаж" in cap.lower()

    if mode_is_sales and caption_has_pokupok:
        # Режим продажи, а документ похоже "Книга покупок"
        await ask_user_mode_confirmation(
            messages[-1],
            images_list,
            cap,
            headers,
            qwen,
            expected="Покупки",
            actual="Продажи",
        )
        return
    elif not mode_is_sales and caption_has_prodazh:
        # Режим покупки, а документ похоже "Книга продаж"
        await ask_user_mode_confirmation(
            messages[-1],
            images_list,
            cap,
            headers,
            qwen,
            expected="Продажи",
            actual="Покупки",
        )
        return
    else:
        # Всё совпадает - продолжаем обработку
        await process_ocr_and_excel(messages, images_list, cap, headers, qwen)


@dp.callback_query(F.data.startswith("continue_as_"))
async def continue_as_mode(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    if user_id not in pending_data:
        await callback_query.answer("Нет данных для обработки.")
        return

    data = pending_data.pop(user_id)
    # callback_data будет вида "continue_as_покупки" или "continue_as_продажи"
    chosen_mode = callback_query.data.replace("continue_as_", "")

    # Определяем нужный режим
    # Если выбрано "покупки" => SHOP_HEADERS, если "продажи" => SALES_HEADERS
    new_mode = "SHOP_HEADERS" if chosen_mode == "покупки" else "SALES_HEADERS"

    global CURRENT_MODE
    if CURRENT_MODE != new_mode:
        CURRENT_MODE = new_mode
        await callback_query.message.edit_text(
            f"Переключаем режим на {chosen_mode.capitalize()}.\nПродолжаем..."
        )
    else:
        await callback_query.message.edit_text("Продолжаем в текущем режиме.")

    await callback_query.answer()
    # Обновляем заголовки под новый/текущий режим
    new_headers = HEADERS[CURRENT_MODE]
    await process_ocr_and_excel(
        [data["message"]], data["images_list"], data["cap"], new_headers, data["qwen"]
    )


@dp.callback_query(F.data.startswith("switch_to_"))
async def switch_mode_after_confirmation(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    if user_id not in pending_data:
        await callback_query.answer("Нет данных для обработки.")
        return

    data = pending_data.pop(user_id)

    # Меняем глобальный режим
    # Если пользователь сказал "switch_to_покупки" - тогда режим должен стать SHOP_HEADERS
    # Если "switch_to_продажи" - тогда SALES_HEADERS
    new_mode = "SHOP_HEADERS" if "покупки" in callback_query.data else "SALES_HEADERS"
    global CURRENT_MODE
    CURRENT_MODE = new_mode

    new_mode_name = "Продажи" if CURRENT_MODE == "SALES_HEADERS" else "Покупки"
    await callback_query.message.edit_text(
        f"Режим переключён на {new_mode_name}, продолжаем."
    )
    await callback_query.answer()

    # Обновляем headers, так как режим изменился
    headers = HEADERS[CURRENT_MODE]
    await process_ocr_and_excel(
        [data["message"]], data["images_list"], data["cap"], headers, data["qwen"]
    )


async def process_ocr_and_excel(
    messages: list[Message], images_list: list, cap: str, headers, qwen
):
    try:
        head_table, head_name = get_head(images_list[0])
        await messages[-1].answer("Шапка извлечена")

        limiter = AsyncLimiter(max_rate=60, time_period=60)
        # Инициализируем список с фиксированным размером, заполненный None
        df_list = [None] * len(images_list)

        async def limited_qwen_ocr(img, index):
            async with limiter:
                try:
                    result = await qwen_ocr(img, headers, qwen)
                    if result is None or getattr(result, "height", 0) == 0:
                        await messages[-1].answer(f"Внимание! Страница {index} пустая!")
                    else:
                        await messages[-1].answer(f"Страница {index} готова")
                        # Сохраняем результат по правильному индексу
                        df_list[index - 1] = result
                except Exception as e:
                    await messages[-1].answer(
                        f"Ошибка при обработке страницы {index}: {e}"
                    )

        # Создаём задачи с ограничителем
        tasks = [
            asyncio.create_task(limited_qwen_ocr(img, i))
            for i, img in enumerate(images_list, start=1)
        ]

        # Обрабатываем задачи по мере их завершения
        for task in asyncio.as_completed(tasks):
            await task

        # После завершения всех задач можно продолжить дальнейшую обработку
        await messages[-1].answer("Обработка всех страниц завершена.")

        # Фильтруем None значения (в случае ошибок или пустых страниц)
        ordered_df_list = [df for df in df_list if df is not None]

        # Объединяем результаты, сохраняя порядок
        if ordered_df_list:
            df = pl.concat(ordered_df_list)
        else:
            df = pl.DataFrame()
        table_df, fixed_table, unfixed_table, not_found, wrong = check_by_inn(df)
        await messages[-1].answer(fixed_table, parse_mode="MarkdownV2")
        await messages[-1].answer(unfixed_table, parse_mode="MarkdownV2")
        await messages[-1].answer(not_found, parse_mode="MarkdownV2")
        await messages[-1].answer(wrong, parse_mode="MarkdownV2")

        work_book = merge_tables_to_excel(
            head_table, table_df, head_name, width=len(headers)
        )
        temp_file = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
        work_book.save(temp_file.name)

        format_excel(temp_file.name, head_table.height + 3)

        excel_table = FSInputFile(path=temp_file.name, filename=f"{cap}.xlsx")
        await messages[-1].answer_document(excel_table)
        temp_file.close()
        os.unlink(temp_file.name)

    except Exception as e:
        print(f"Error: {e}")
        await messages[-1].answer(f"Общая ошибка: {e}")


async def ask_user_mode_confirmation(
    message: types.Message, images_list, cap, headers, qwen, expected: str, actual: str
):
    # Сохраняем данные для последующей обработки
    user_id = message.from_user.id
    pending_data[user_id] = {
        "images_list": images_list,
        "cap": cap,
        "headers": headers,
        "qwen": qwen,
        "message": message,
    }

    # Делаем кнопки для уточнения режима
    continue_button = InlineKeyboardButton(
        text=f"Продолжить как {expected}",
        callback_data=f"continue_as_{expected.lower()}",
    )
    switch_button = InlineKeyboardButton(
        text=f"Нет, это {actual.lower()}", callback_data=f"switch_to_{actual.lower()}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[continue_button, switch_button]])

    await message.answer(
        f"Сейчас режим: {actual.lower()}, а в тексте похоже документ {expected.lower()}.\nНет ли ошибки?",
        reply_markup=kb,
    )


@dp.message(Command("start"), F.from_user.id.not_in(admins))
async def cmd_start(message: Message):
    await message.answer("Нет доступа")


@dp.message(Command("start"), F.from_user.id.in_(admins))
async def admin_start(message: Message):
    await message.answer(
        "Привет, админ! Отправь мне группу изображений с подписью, и я сохраню их в максимальном разрешении.\n"
        "Для настройки режима используй команду /settings."
    )


@dp.message(Command("settings"), F.from_user.id.in_(admins))
async def settings_menu(message: Message):
    current_mode_name = "Продажи" if CURRENT_MODE == "SALES_HEADERS" else "Покупки"
    toggle_button = InlineKeyboardButton(
        text=f"Переключить на {'Покупки' if CURRENT_MODE == 'SALES_HEADERS' else 'Продажи'}",
        callback_data="toggle_mode",
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[toggle_button]])

    # Добавляем жирный шрифт для текущего режима
    # Используем parse_mode="MarkdownV2", поэтому экранируем спецсимволы.
    # Слова "Продажи" и "Покупки" не содержат спецсимволов Markdown, их можно использовать напрямую.
    await message.answer(
        f"Текущий режим: *{current_mode_name}*",
        parse_mode="Markdown",
        reply_markup=kb,
    )


@dp.message(Command("model"), F.from_user.id.in_(admins))
async def qwen_menu(message: Message):
    model_name = "большая" if qwen == QWEN72B else "маленькая"
    toggle_button = InlineKeyboardButton(
        text=f"Переключить на {'большая' if qwen == QWEN7B else 'маленькая'}",
        callback_data="qwen_mode",
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[toggle_button]])

    # Добавляем жирный шрифт для названия модели
    await message.answer(
        f"Текущий режим: *{model_name}*",
        parse_mode="Markdown",
        reply_markup=kb,
    )


@dp.callback_query(F.data == "toggle_mode")
async def toggle_mode(callback_query: types.CallbackQuery):
    global CURRENT_MODE
    CURRENT_MODE = (
        "SHOP_HEADERS" if CURRENT_MODE == "SALES_HEADERS" else "SALES_HEADERS"
    )
    new_mode_name = "Продажи" if CURRENT_MODE == "SALES_HEADERS" else "Покупки"
    toggle_button = InlineKeyboardButton(
        text=f"Переключить на {'Покупки' if CURRENT_MODE == 'SALES_HEADERS' else 'Продажи'}",
        callback_data="toggle_mode",
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[toggle_button]])

    # Оставляем кнопку, меняем текст
    await callback_query.message.edit_text(
        f"Текущий режим: *{new_mode_name}*",
        parse_mode="Markdown",
        reply_markup=kb,
    )
    await callback_query.answer("Режим обновлён!")


@dp.callback_query(F.data == "qwen_mode")
async def qwen_mode(callback_query: types.CallbackQuery):
    global qwen
    qwen = QWEN7B if qwen == QWEN72B else QWEN72B
    model_name = "большая" if qwen == QWEN72B else "маленькая"
    toggle_button = InlineKeyboardButton(
        text=f"Переключить на {'большая' if qwen == QWEN7B else 'маленькая'}",
        callback_data="qwen_mode",
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[toggle_button]])

    # Оставляем кнопку, меняем текст
    await callback_query.message.edit_text(
        f"Текущий режим: *{model_name}*",
        parse_mode="Markdown",
        reply_markup=kb,
    )
    await callback_query.answer("Режим обновлён!")


async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())

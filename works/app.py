import asyncio
import logging
import tempfile
import polars as pl
from typing import List
from aiogram import Bot, Dispatcher
from tqdm import tqdm
from aiogram.types import Message
from aiogram.filters import Command
import os
from datetime import datetime
from dotenv import load_dotenv
from aiogram_media_group import media_group_handler
from aiogram import F, types
from aiogram.types import FSInputFile
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
# from aiogram.utils.keyboard import InlineKeyboardBuilder

# import pandas as pd

# from check import check_df
from inn_check import check_by_inn
from format import format_excel
from merge import merge_tables_to_excel
from async_app import process_image, get_head, process_image_mistral

load_dotenv()

os.environ["POLARS_FMT_MAX_COLS"] = str(100)
os.environ["POLARS_FMT_MAX_ROWS"] = str(1000)

BOT_TOKEN = os.getenv("TOKEN")
admins = [461923889, 1002688109]


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





@dp.message(F.media_group_id, F.content_type.in_({"photo"}), F.from_user.id.in_(admins))
@media_group_handler
async def album_handler(messages: List[types.Message]):
    headers = HEADERS[CURRENT_MODE]
    await messages[0].answer(f"Текущий режим:\n{"Продажи" if CURRENT_MODE == 'SALES_HEADERS' else 'Покупки'}")
    await messages[0].answer(f"Текущие заголовки:\n{', '.join(headers)}")

    # from xlsxwriter import Workbook

    images_list = []
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    folder_path = f"downloaded_images/{current_time}"
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

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
    try:
        df_list = []

        # headers = get_headers(images_list[0])
        head_table, head_name = get_head(images_list[0])

        await messages[-1].answer("Шапка извлечена")
        for i, img in enumerate(tqdm(images_list)):
            df_list.append(process_image_mistral(img, headers))
            await messages[-1].answer(f"Страница {i + 1} готова")
        df: pl.DataFrame = pl.concat(df_list)
        print(df)
        df=df.with_columns(pl.all().str.replace("null", "(пусто)"))
        table_df, fixed_table, unfixed_table, not_found, wrong = check_by_inn(df)
        await messages[-1].answer(fixed_table, parse_mode="MarkdownV2")
        await messages[-1].answer(unfixed_table, parse_mode="MarkdownV2")
        await messages[-1].answer(not_found, parse_mode="MarkdownV2")
        await messages[-1].answer(wrong, parse_mode="MarkdownV2")
        work_book = merge_tables_to_excel(head_table, table_df, head_name, width=len(headers))
        temp_file = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
        work_book.save(temp_file.name)

        # Форматируем Excel файл
        format_excel(temp_file.name, head_table.height + 3)

        excel_table = FSInputFile(path=temp_file.name, filename=f"{cap}.xlsx")

        await messages[-1].answer_document(excel_table)
        temp_file.close()

        # Удаляем временный файл
        os.unlink(temp_file.name)
    except Exception as e:
        print(f"Error: {e}")
        await messages[-1].answer(f"Ошибка, {e}")

    print(text)


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

    await message.answer(
        f"Текущий режим: {current_mode_name}.",
        reply_markup=kb,
    )


@dp.callback_query(F.data == "toggle_mode")
async def toggle_mode(callback_query: types.CallbackQuery):
    global CURRENT_MODE
    CURRENT_MODE = "SHOP_HEADERS" if CURRENT_MODE == "SALES_HEADERS" else "SALES_HEADERS"
    new_mode_name = "Продажи" if CURRENT_MODE == "SALES_HEADERS" else "Покупки"
    await callback_query.message.edit_text(
        f"Режим переключён на {new_mode_name}.",
    )
    await callback_query.answer("Режим обновлён!")

async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())

import asyncio
import base64
import os
from dotenv import load_dotenv
import polars as pl
import pandas as pd
import io
from PIL import Image
import mdpd
import re
from openai import OpenAI, AsyncOpenAI, OpenAIError

load_dotenv()

HYPER_API = os.environ.get("HYPER_API")
HYPER_CLIENT = AsyncOpenAI(api_key=HYPER_API, base_url="https://api.hyperbolic.xyz/v1")


CHATGPT_CLIENT = OpenAI(
    api_key=os.environ.get("OPENAI_API_KEY"),  # This is the default and can be omitted
)

def extract_between_tags(tag: str, string: str, strip: bool = False) -> list[str]:
    ext_list = re.findall(f"<{tag}>(.+?)</{tag}>", string, re.DOTALL)
    if strip:
        ext_list = [e.strip() for e in ext_list]
    return ext_list


def convert_to_str(obj):
    if isinstance(obj, dict):
        return {key: convert_to_str(value) for key, value in obj.items()}
    elif isinstance(obj, list):  # Была ошибка в этой строке
        return [convert_to_str(item) for item in obj]  # Убрали list как тип
    else:
        return str(obj)


def encode_image(image_path):
    """Encode the image to base64."""
    try:
        with Image.open(image_path) as img:
            buffered = io.BytesIO()
            img.save(buffered, format="JPEG")
            return base64.b64encode(buffered.getvalue()).decode("utf-8")
    except Exception as e:
        print(f"Error encoding image: {e}")
        return None


def get_head(img: str, client: OpenAI = CHATGPT_CLIENT) -> tuple[pl.DataFrame, str]:
    print("getting head")
    base64_image = encode_image(img)
    if base64_image is None:
        return None
    messages = [
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": "Привет, на фотографии Excel две таблицы, одна выше другой. Твоя задача - в тэге <name> указать название этой таблицы. А потом в другом тэге, <div>, используя HTML и тег <table> извлечь данные только ВЕРХНЕЙ-таблицы, такой вот таблицы-шапки. Нижнюю не трогать.",
                },
                {
                    "type": "image_url",
                    "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"},
                },
            ],
        },
        {
            "role": "assistant",
            "content": "Конечно, название верхней таблицы: <name>",
        },
        {"role": "user", "content": "продолжай"},
    ]
    response = client.chat.completions.create(model="gpt-4o", messages=messages, timeout=90, )
    # for chunk in response:
    #     print(chunk.choices[0].delta.content or "NoData ", end="")
    # return None, ""
    output = response.choices[0].message.content
    print(output)
    names = extract_between_tags("name", output)
    table_html = extract_between_tags("div", output)[0]
    table_name = names[0] if names else ""
    print(table_html)

    pandas_df = pd.read_html(table_html)[0]
    df = pl.from_pandas(pandas_df)
    print("df:")
    print(df)
    return df, table_name

# async def qwen_ocr(img: str, headers: list[str], qwen: str, client: AsyncOpenAI = HYPER_CLIENT) -> pl.DataFrame:
#     base64_image = encode_image(img)
#     if base64_image is None:
#         return None
#     messages = [
#         {
#             "role": "user",
#             "content": [
#                 {
#                     "type": "text",
#                     "text": f"Сделай распознавание данной таблицы, выдай мне полный результат в MarkDown, все столбцы и строки. Вот столбцы: {headers}",
#                 },
#                 {
#                     "type": "image_url",
#                     "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"},
#                 },
#             ],
#         },
#     ]
#     try:
#         res = await client.chat.completions.create(
#             model=f"Qwen/{qwen}", 
#             messages=messages, 
#             max_tokens=4096, 
#             temperature=0.1,
#             timeout=90
#         )
#         output = res.choices[0].message.content
#         print('output:\n' + output)
#         pandas_df = mdpd.from_md(output, header=headers)
#         df = pl.from_pandas(pandas_df)
#         print(df)
#         return df
#     except OpenAIError as e:
#         print('OpenaiError:' + e)
#         return None

async def qwen_ocr(img: str, headers: list[str], qwen: str, client: AsyncOpenAI = HYPER_CLIENT) -> pl.DataFrame:
    base64_image = encode_image(img)
    if base64_image is None:
        return None

    messages = [
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": f"Сделай распознавание данной таблицы, выдай мне полный результат в MarkDown, все столбцы и строки. Вот столбцы: {headers}",
                },
                {
                    "type": "image_url",
                    "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"},
                },
            ],
        },
    ]

    max_attempts = 1  # Максимальное количество попыток
    for attempt in range(1, max_attempts + 1):
        try:
            # Устанавливаем таймаут на 90 секунд
            res = await asyncio.wait_for(
                client.chat.completions.create(
                    model=f"Qwen/{qwen}", 
                    messages=messages, 
                    max_tokens=4096, 
                    temperature=0.1
                ),
                timeout=120
            )
            output = res.choices[0].message.content
            print('output:\n' + output)
            
            pandas_df = mdpd.from_md(output, header=headers)
            df = pl.from_pandas(pandas_df)
            print(df)
            return df  # Возвращаем результат при успешном выполнении

        except asyncio.TimeoutError:
            print(f"Попытка {attempt} превысила время ожидания (120 секунд).")
        except OpenAIError as e:
            print(f"Попытка {attempt} завершилась ошибкой OpenAI: {e}")
        except Exception as e:
            print(f"Попытка {attempt} завершилась неожиданной ошибкой: {e}")

        if attempt < max_attempts:
            print("Повторная попытка...")
        else:
            print("Достигнуто максимальное количество попыток. Возврат None.")
    
    return None  # Возвращаем None, если все попытки неудачны

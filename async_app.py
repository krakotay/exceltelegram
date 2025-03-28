import base64
import os
from dotenv import load_dotenv
import polars as pl
import io
from PIL import Image
import mdpd
from openai import OpenAI
import re
import pandas as pd
from mistralai import Mistral

load_dotenv()

# ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
MAX_PARALLEL_REQUESTS = 5
# MISTRAL_CLIENT = Anthropic(api_key=ANTHROPIC_API_KEY)

CHATGPT_CLIENT = OpenAI(
    api_key=os.environ.get("OPENAI_API_KEY"),  # This is the default and can be omitted
)
MISTRAL_CLIENT = Mistral(
    api_key=os.getenv("MISTRAL_API_KEY"),
)
MODEL = os.getenv("MISTRAL_NAME")
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

    response = client.chat.completions.create(model="gpt-4o", messages=messages)
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

def process_image_mistral(img: str, headers: list[str], client: Mistral = MISTRAL_CLIENT):
    base64_image = encode_image(img)
    h = f"|{" | ".join(headers)} | \n|{" - |" * len(headers)}"
    print(headers)
    print(h)
    if base64_image is None:
        return None
    messages = [
        {
            "role": "user",
            "content": [
                {"type": "text", "text": "Сделай копию таблицы в MarkDown"},
                {
                    "type": "image_url",
                    "image_url": f"data:image/jpeg;base64,{base64_image}",
                },
            ],
        },
        {
            "role": "assistant",
            "content": [
                {
                    "type": "text",
                    "text": "Конечно, вот таблица в MarkDown:\n ```markdown\n"
                    + h
                    + "\n|",
                }
            ],
        },
        {"role": "user", "content": "Всё ок, продолжай!"},
    ]
    res = MISTRAL_CLIENT.chat.complete(model=MODEL, messages=messages, max_tokens=4096)
    output = res.choices[0].message.content
    print("output: \n" + output)
    pandas_df = mdpd.from_md(output, header=headers)
    df = pl.from_pandas(pandas_df)
    print("df:")
    print(df)
    return df

def process_image(
    img: str, headers: list[str], client: OpenAI = CHATGPT_CLIENT
) -> pl.DataFrame:
    base64_image = encode_image(img)
    h = f"|{" | ".join(headers)} | \n|{" - |" * len(headers)}"
    print(headers)
    print(h)
    if base64_image is None:
        return None
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": f"Преврати эту фотографию таблицы в таблицу MarkDown, в ней ровно {len(headers)} столбцов. В ИИН либо 10 (десять) у ЮРИДИЧЕСКОГО лица, либо 12 (двенадцать) цифр у ФИЗИЧЕСКОГО лица, не больше и не меньше. Твоя задача - максимально точно передать данные каждой конкретной ячейки, кроме . В конкретной ячейке может стоять заглушка, с надписью '(пусто)', меняй её на 'null', но НИКОГДА не ставь ни (пусто), ни null там, где этих надписей на изображении нет.",
                    },
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"},
                    },
                ],
            },
            {
                "role": "assistant",
                "content": [{ "type": "text", "text": "Конечно, вот таблица в MarkDown:\n ```markdown\n" + h + "\n|" }] ,
            },
            {"role": "user", "content" : 'Всё ок, продолжай!'}
        ],
    )

    output = response.choices[0].message.content
    print("output: \n" + output)
    pandas_df = mdpd.from_md(output, header=headers)
    df = pl.from_pandas(pandas_df)
    print("df:")
    print(df)
    return df

import re
from typing import Tuple, Optional

# --- РЕГУЛЯРНЫЕ ВЫРАЖЕНИЯ (остаются без изменений) ---
FIO_PATTERN = re.compile(
    r'\b([А-ЯЁ][а-яё]+(?:-[А-ЯЁ][а-яё]+)?)\s+([А-ЯЁ][а-яё]+)\s+(([А-ЯЁ][а-яё]+))?\b'
)
PHONE_PATTERN = re.compile(
    r'(?:\+7|8)?[ \-.(]*(\d{3})[ \-.)]*(\d{3})[ \-.]*(\d{2})[ \-.]*(\d{2})\b'
)

# --- ИЗМЕНЕНИЕ: Используем токены для замены вместо частичной маскировки ---
FIO_MASK_TOKEN = "[ФИО ЗАМАСКИРОВАНО]"
PHONE_MASK_TOKEN = "[ТЕЛЕФОН ЗАМАСКИРОВАН]"


def extract_and_mask_pii(text: str) -> Tuple[str, Optional[str], Optional[str]]:
    """
    Извлекает ФИО и номер телефона, а затем полностью заменяет их на токены в тексте.
    Возвращает: (замаскированный_текст, извлеченное_фио, извлеченный_телефон)
    """
    if not text:
        return "", None, None

    extracted_fio = None
    extracted_phone = None
    masked_text = text
    
    # --- Извлечение и маскировка телефона ---
    phone_match = PHONE_PATTERN.search(masked_text)
    if phone_match:
        full_phone_digits = "".join(filter(str.isdigit, phone_match.group(0)))
        # Нормализуем номер до 11 цифр, начиная с 7
        if len(full_phone_digits) == 11 and full_phone_digits.startswith('8'):
            extracted_phone = '7' + full_phone_digits[1:]
        elif len(full_phone_digits) == 10:
            extracted_phone = '7' + full_phone_digits
        else:
            extracted_phone = full_phone_digits

        # --- ИЗМЕНЕНИЕ: Заменяем весь найденный номер на токен ---
        masked_text = PHONE_PATTERN.sub(PHONE_MASK_TOKEN, masked_text, count=1)

    # --- Извлечение и маскировка ФИО (ищем в тексте, где уже может быть замаскирован телефон) ---
    fio_match = FIO_PATTERN.search(masked_text)
    if fio_match:
        # Извлекаем полное совпадение
        extracted_fio = fio_match.group(0).strip()
        
        # --- ИЗМЕНЕНИЕ: Заменяем все найденное ФИО на токен ---
        masked_text = FIO_PATTERN.sub(FIO_MASK_TOKEN, masked_text, count=1)

    return masked_text, extracted_fio, extracted_phone

# --- Тесты для проверки ---
if __name__ == '__main__':
    test_case_1 = "Мои данные: Иванов Иван Иванович, мой телефон +7 (999) 123-45-67. Прошу связаться."
    masked, fio, phone = extract_and_mask_pii(test_case_1)
    print(f"Оригинал: {test_case_1}")
    print(f"  Маска: {masked}")
    print(f"  Извлечено ФИО: {fio}")
    print(f"  Извлечен Телефон: {phone}")
    print("-" * 20)
    
    test_case_2 = "Меня зовут Петров-Водкин Кузьма. Звоните 89219876543"
    masked, fio, phone = extract_and_mask_pii(test_case_2)
    print(f"Оригинал: {test_case_2}")
    print(f"  Маска: {masked}")
    print(f"  Извлечено ФИО: {fio}")
    print(f"  Извлечен Телефон: {phone}")
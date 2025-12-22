# hr_bot/utils/formatters.py

def mask_fio(full_name: str | None) -> str:
    """
    Маскирует отчество в полной ФИО, оставляя фамилию и имя открытыми.
    Примеры:
    "Иванов Иван Иванович" -> "Иванов Иван И***"
    "Петров Сергей" -> "Петров Сергей" (если нет отчества)
    "Сидоров" -> "Сидоров" (если только фамилия)
    "Иванов" -> "Иванов"
    """
    if not full_name:
        return "Не указано"

    parts = full_name.strip().split()
    
    if not parts:
        return "Не указано"
    
    # Фамилия всегда первая часть
    surname = parts[0]
    
    # Имя, если есть (вторая часть)
    first_name = parts[1] if len(parts) > 1 else ""

    # Отчество, если есть (третья часть)
    patronymic = parts[2] if len(parts) > 2 else ""

    result_parts = [surname]

    if first_name:
        result_parts.append(first_name)
    
    if patronymic:
        # Маскируем отчество: первая буква + ***
        if len(patronymic) > 1:
            masked_patronymic = patronymic[0] + '***'
        else:
            # Если отчество состоит из одной буквы (редкий случай), маскируем полностью
            masked_patronymic = '*' * len(patronymic)
        result_parts.append(masked_patronymic)

    return " ".join(result_parts)